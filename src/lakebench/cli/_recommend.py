"""``recommend`` and ``config recommend``: sizing guidance from the one
sizing source.

Every figure here comes from ``config.sizing`` for a config at the given
scale: the user's config for ``config recommend``, the default-recipe
config for the workload and mode otherwise.

* Without a cluster (``--scale N``, or the reference table) the plans are
  offline: datagen at its declared or default parallelism, the figure
  ``config show``, ``info`` and the docs tables print.
* With a cluster (detected, or ``--cores`` and ``--memory``) each scale is
  decided by ``check_capacity``, the function ``run``'s capacity preflight
  calls, against that cluster. "Largest scale that fits" is the largest
  scale at which every scale up to it is admitted
  (``largest_fitting_scale``).

In batch that is the preflight's decision for ``run --generate``. In
continuous there are two answers and recommend prints both: a plain
``run``, whose datagen Job is counted beside the streams, and a corpus
generated first (``generate``, then ``run --skip-generate`` within an hour,
while the finished datagen Job still exists; a finished Job is not
counted). Above scale 50 the autosizer sizes the continuous datagen
Job to about 90% of the CPU left after the always-on pods, so a plain run
there is refused or admitted only with its streams capped hard, depending
on the cluster.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table

from lakebench.cli._helpers import esc
from lakebench.exit_codes import ExitCode

if TYPE_CHECKING:
    from lakebench.config.schema import LakebenchConfig
    from lakebench.config.sizing import CapacityVerdict, SizingPlan
    from lakebench.k8s.client import ClusterCapacity

console = Console()

_GIB = 1024**3
_MILESTONES = (1, 10, 50, 100, 300, 500)


def _format_data_size(gb: float) -> str:
    if gb >= 1_000_000:
        return f"{gb / 1_000_000:.1f} PB"
    if gb >= 1_000:
        return f"{gb / 1_000:.1f} TB"
    return f"{gb:.0f} GB"


def _config_at(base: LakebenchConfig, scale: int) -> LakebenchConfig:
    """*base* at *scale*; *base* is not mutated."""
    copy = base.model_copy(deep=True)
    object.__setattr__(copy.architecture.workload.datagen, "scale", float(scale))
    return copy


def _capacity_from(
    cluster_cores: int | None,
    cluster_memory_gb: int | None,
    detect_capacity: Callable[[], ClusterCapacity | None] | None,
) -> tuple[ClusterCapacity | None, bool, str]:
    """``(capacity, node shape known, source label)``."""
    from lakebench.k8s.client import ClusterCapacity

    if cluster_cores is not None and cluster_memory_gb is not None:
        # Node shape unknown: the whole cluster stands in for its largest
        # node, and the largest-pod check is skipped.
        cap = ClusterCapacity(
            total_cpu_millicores=cluster_cores * 1000,
            total_memory_bytes=cluster_memory_gb * _GIB,
            node_count=0,
            largest_node_cpu_millicores=cluster_cores * 1000,
            largest_node_memory_bytes=cluster_memory_gb * _GIB,
        )
        return cap, False, "user-provided"
    if detect_capacity is None:
        return None, False, ""
    from lakebench.k8s.target import ContextConflictError

    try:
        detected = detect_capacity()
    except ContextConflictError:
        # A second context in one process, or a context whose server or CA
        # changed: the CLI's handler exits 3 (context.changed). Falling back
        # to the reference table would hide it behind exit 0.
        raise
    except Exception as e:  # report and fall back to the reference table
        console.print(f"[yellow]Could not detect cluster capacity: {esc(e)}[/yellow]")
        return None, False, ""
    if detected is None:
        return None, False, ""
    cap = ClusterCapacity(
        total_cpu_millicores=(
            cluster_cores * 1000 if cluster_cores is not None else detected.total_cpu_millicores
        ),
        total_memory_bytes=(
            cluster_memory_gb * _GIB
            if cluster_memory_gb is not None
            else detected.total_memory_bytes
        ),
        node_count=detected.node_count,
        largest_node_cpu_millicores=detected.largest_node_cpu_millicores,
        largest_node_memory_bytes=detected.largest_node_memory_bytes,
    )
    return cap, True, f"detected ({detected.node_count} nodes)"


def recommend_impl(
    *,
    cluster_cores: int | None,
    cluster_memory_gb: int | None,
    target_scale: int | None,
    slow_datagen: bool,
    mode: str | None,
    schema_type: str | None,
    base_cfg: LakebenchConfig | None = None,
    detect_capacity: Callable[[], ClusterCapacity | None] | None = None,
) -> int:
    """Print sizing guidance; returns the exit code.

    *base_cfg* is the user's config (``config recommend``); without it the
    default-recipe config for *schema_type* and *mode* is sized.
    *detect_capacity* returns the connected cluster's ``ClusterCapacity``
    or None; it is called only when ``--cores``/``--memory`` are not both
    given.
    """
    from lakebench.config.scale import get_dimensions
    from lakebench.config.schema import is_continuous_mode
    from lakebench.config.sizing import (
        breakdown_text,
        check_capacity,
        default_sizing_config,
        floor_text,
        largest_fitting_scale,
        plan_requirements,
        plan_shortfalls,
    )
    from lakebench.config.support import DATAGEN_SCALE_BANDS, datagen_scale_problem

    if base_cfg is not None:
        workload = base_cfg.architecture.workload.schema_type.value
        mode_label = (
            "continuous" if is_continuous_mode(base_cfg.architecture.pipeline.mode) else "batch"
        )
        base = base_cfg
    else:
        workload = schema_type or "customer360"
        mode_label = "continuous" if is_continuous_mode(mode or "batch") else "batch"
        if workload not in DATAGEN_SCALE_BANDS:
            console.print(
                f"[red]Unknown workload schema {esc(repr(workload))}; use "
                f"{esc(' or '.join(sorted(DATAGEN_SCALE_BANDS)))}.[/red]"
            )
            return int(ExitCode.USAGE)
        base = default_sizing_config(workload, mode_label, 1)
    ceiling = int(DATAGEN_SCALE_BANDS[workload][1])

    def plan_at(scale: int, *, datagen_runs: bool = True) -> SizingPlan:
        return plan_requirements(_config_at(base, scale), datagen_runs=datagen_runs)

    def unverified_note(scale: int) -> str | None:
        problem = datagen_scale_problem(workload, float(scale))
        if problem is None or problem[0] != "unverified":
            return None
        supported_max = int(DATAGEN_SCALE_BANDS[workload][0])
        return f"scales above {supported_max} are unverified for {workload}: {problem[1]}"

    def data_size(scale: int) -> str:
        return _format_data_size(get_dimensions(workload, scale).approx_bronze_gb)

    header = f"workload: {workload}, mode: {mode_label}"
    if base_cfg is None:
        header += ", default recipe hive-iceberg-spark-trino"
    if slow_datagen:
        console.print(
            "[dim]--slow-datagen is ignored: datagen pods that do not fit queue, so "
            "datagen never limits the scale.[/dim]"
        )

    # Case 1: what does one scale need?
    if target_scale is not None:
        plan = plan_at(target_scale)
        dims = get_dimensions(workload, target_scale)
        console.print(
            f"[bold]Scale {target_scale:,}[/bold] (~{esc(_format_data_size(dims.approx_bronze_gb))}, "
            f"~{dims.approx_rows:,} rows; {esc(header)})"
        )
        console.print(f"  Minimum cluster: [bold]{esc(floor_text(plan))}[/bold]")
        console.print(f"  Of which:        {esc(breakdown_text(plan))}")
        if mode_label == "continuous":
            first = plan_at(target_scale, datagen_runs=False)
            console.print(
                f"  Corpus generated first (generate, then run --skip-generate within an "
                f"hour): {esc(first.floor.cpu_cores)} cores / {esc(first.floor.memory_gb)} GB"
            )
        pod = plan.largest_pod
        console.print(
            f"  Largest pod:     {pod.cpu_cores:g} cores ({esc(pod.cpu_from)}), "
            f"{pod.memory_gb:g} GB ({esc(pod.memory_from)}); each must fit on one node"
        )
        if plan.datagen is not None and not plan.datagen.cluster_scaled:
            console.print(
                "  [dim]Datagen is before cluster scaling: run caps it to the cluster, and "
                "above scale 50 raises it to use the cluster.[/dim]"
            )
        note = unverified_note(target_scale)
        if note:
            console.print(f"  [yellow]{esc(note)}[/yellow]")
        if target_scale > ceiling:
            console.print(
                f"  [yellow]Scale {target_scale:,} is above the {esc(workload)} datagen ceiling of "
                f"{esc(ceiling)}; deploy and generate refuse it.[/yellow]"
            )
        return 0

    # Case 2: the cluster, given or detected.
    capacity, check_pod, source = _capacity_from(cluster_cores, cluster_memory_gb, detect_capacity)

    if capacity is None:
        console.print(f"[bold]Cluster sizing reference[/bold] ({esc(header)})")
        console.print(
            "[dim]Connect to a cluster or pass --cores and --memory for the largest "
            "scale that fits; --scale N shows one scale.[/dim]\n"
        )
        table = Table(title="Minimum cluster by scale (datagen before cluster scaling)")
        table.add_column("Scale", justify="right", style="cyan")
        table.add_column("Data", justify="right")
        table.add_column("Cores", justify="right")
        table.add_column("Memory", justify="right")
        table.add_column("Scratch PVC (if enabled)", justify="right")
        table.add_column("Driven by")
        for scale in sorted({*(m for m in _MILESTONES if m <= ceiling), ceiling}):
            plan = plan_at(scale)
            table.add_row(
                f"{scale:,}",
                data_size(scale),
                f"{plan.floor.cpu_cores:,}",
                f"{plan.floor.memory_gb:,} GB",
                f"{plan.scratch_gb:,} Gi",
                plan.floor_driver,
            )
        console.print(table)
        return 0

    cluster = capacity
    cores = cluster.total_cpu_millicores // 1000
    mem_gb = cluster.total_memory_bytes // _GIB
    console.print(f"[bold]Cluster capacity[/bold] ({esc(source)}; {esc(header)})")
    console.print(f"  CPU cores: [bold]{esc(cores)}[/bold]")
    console.print(f"  Memory:    [bold]{esc(mem_gb)} GB[/bold]")
    if not check_pod:
        console.print("  [dim]Node sizes unknown: the largest-pod check is skipped.[/dim]")
    console.print()

    continuous = mode_label == "continuous"
    # Batch: the decision for run --generate (datagen counted). Continuous:
    # "first" is a corpus generated before the streams start, "plain" a run
    # that generates its own corpus beside them.
    verdicts: dict[tuple[int, bool], CapacityVerdict] = {}

    def verdict_at(scale: int, datagen_runs: bool | None = None) -> CapacityVerdict:
        dg = (not continuous) if datagen_runs is None else datagen_runs
        if (scale, dg) not in verdicts:
            verdicts[(scale, dg)] = check_capacity(
                _config_at(base, scale), cluster, check_pod=check_pod, datagen_runs=dg
            )
        return verdicts[(scale, dg)]

    best = largest_fitting_scale(lambda s: verdict_at(s).admitted, upper=ceiling)
    full = largest_fitting_scale(lambda s: verdict_at(s).status == "fits", upper=best)
    plain = (
        largest_fitting_scale(lambda s: verdict_at(s, True).admitted, upper=ceiling)
        if continuous
        else best
    )
    if best == 0:
        console.print("[yellow]The cluster is below the minimum for scale 1.[/yellow]")
        console.print(f"[dim]Scale 1 needs {esc(floor_text(verdict_at(1).plan))}.[/dim]")
        return 0

    if continuous:
        console.print(
            f"[green]Largest scale that fits, plain run:[/green] [bold]{plain:,}[/bold]"
            + (f" (~{esc(data_size(plain))})" if plain else " (scale 1 is refused)")
            + " [dim](datagen counted beside the streams)[/dim]"
        )
        console.print(
            f"[green]Largest scale that fits, corpus generated first:[/green] "
            f"[bold]{best:,}[/bold] (~{esc(data_size(best))}) [dim](generate, then run "
            "--skip-generate within an hour of generation finishing)[/dim]"
        )
    else:
        console.print(
            f"[green]Largest scale that fits:[/green] [bold]{best:,}[/bold] (~{esc(data_size(best))})"
        )
    if full < best:
        who = "a run on a corpus generated first" if continuous else "the run"
        console.print(
            f"[yellow]Above scale {full:,} {esc(who)} caps its streams to fit (degraded).[/yellow]"
        )
    if best == ceiling:
        console.print(
            f"[dim]Bounded by the {esc(workload)} datagen ceiling of {esc(ceiling)}, not by the "
            "cluster.[/dim]"
        )
    note = unverified_note(best)
    if note:
        console.print(f"[yellow]{esc(note)}[/yellow]")

    title = "Scale options (what run requests on this cluster"
    title += ", corpus generated first)" if continuous else ")"
    table = Table(title=title)
    table.add_column("Scale", justify="right", style="cyan")
    table.add_column("Data", justify="right")
    table.add_column("Cores", justify="right")
    table.add_column("Memory", justify="right")
    table.add_column("Status")
    if continuous:
        # Per scale: plain-run admission is not monotonic in scale, so a
        # scale above the plain answer can show OK here.
        table.add_column("Plain run (this scale)")
    points = sorted({*(m for m in _MILESTONES if m <= ceiling), best})
    nxt = [m for m in points if m > best]

    def status_of(v: CapacityVerdict, largest: bool) -> str:
        if v.status == "refused":
            short = plan_shortfalls(v.plan, cluster, check_pod=check_pod)
            return f"[red]needs {', '.join(short) or 'more capacity'}[/red]"
        if largest:
            text = "[green bold]<- largest[/green bold]"
        elif v.status == "degraded":
            text = "[yellow]OK, streams capped[/yellow]"
        else:
            text = "[green]OK[/green]"
        if v.warnings and v.status == "fits":
            text += " [dim](datagen pods queue)[/dim]"
        return text

    for scale in [m for m in points if m <= best] + nxt[:1]:
        v = verdict_at(scale)
        row = [
            f"{scale:,}",
            data_size(scale),
            f"{v.plan.floor.cpu_cores:,}",
            f"{v.plan.floor.memory_gb:,} GB",
            status_of(v, scale == best),
        ]
        if continuous:
            row.append(status_of(verdict_at(scale, True), False))
        table.add_row(*row)
    console.print(table)
    console.print(f"\n[dim]Next: lakebench init --scale {esc(best)}[/dim]")
    return 0
