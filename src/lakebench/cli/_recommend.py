"""``recommend`` and ``config recommend``: sizing guidance from the one
sizing source (CC-22).

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

In batch that is exactly the preflight's decision for ``run``. In
continuous it is the decision for a corpus generated before the streams
start (``generate``, then ``run --skip-generate`` within an hour, while the
finished datagen Job still exists; a finished Job is not counted, LB-158). A plain continuous ``run`` is also checked with its
datagen Job counted beside the streams, and above scale 50 the autosizer
sizes that Job to about 90% of the cluster's CPU, so the preflight refuses
it on any cluster; recommend says so rather than report a scale that
shrinks as the cluster grows.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table

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
    try:
        detected = detect_capacity()
    except Exception as e:  # report and fall back to the reference table
        console.print(f"[yellow]Could not detect cluster capacity: {e}[/yellow]")
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
    from lakebench.config.support import DATAGEN_SCALE_BANDS

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
                f"[red]Unknown workload schema {workload!r}; use "
                f"{' or '.join(sorted(DATAGEN_SCALE_BANDS))}.[/red]"
            )
            return 1
        base = default_sizing_config(workload, mode_label, 1)
    ceiling = int(DATAGEN_SCALE_BANDS[workload][1])

    def plan_at(scale: int) -> SizingPlan:
        return plan_requirements(_config_at(base, scale))

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
            f"[bold]Scale {target_scale:,}[/bold] (~{_format_data_size(dims.approx_bronze_gb)}, "
            f"~{dims.approx_rows:,} rows; {header})"
        )
        console.print(f"  Minimum cluster: [bold]{floor_text(plan)}[/bold]")
        console.print(f"  Of which:        {breakdown_text(plan)}")
        if plan.full != plan.floor:
            console.print(
                f"  All datagen pods at once: {plan.full.cpu_cores} cores / "
                f"{plan.full.memory_gb} GB"
            )
        pod = plan.largest_pod
        console.print(
            f"  Largest pod:     {pod.cpu_cores:g} cores ({pod.cpu_from}), "
            f"{pod.memory_gb:g} GB ({pod.memory_from}); each must fit on one node"
        )
        if plan.datagen is not None and not plan.datagen.cluster_scaled:
            console.print(
                "  [dim]Datagen is before cluster scaling: run caps it to the cluster, and "
                "above scale 50 raises it to use the cluster.[/dim]"
            )
        if target_scale > ceiling:
            console.print(
                f"  [yellow]Scale {target_scale:,} is above the {workload} datagen ceiling of "
                f"{ceiling}; deploy and generate refuse it.[/yellow]"
            )
        return 0

    # Case 2: the cluster, given or detected.
    capacity, check_pod, source = _capacity_from(cluster_cores, cluster_memory_gb, detect_capacity)

    if capacity is None:
        console.print(f"[bold]Cluster sizing reference[/bold] ({header})")
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
    console.print(f"[bold]Cluster capacity[/bold] ({source}; {header})")
    console.print(f"  CPU cores: [bold]{cores}[/bold]")
    console.print(f"  Memory:    [bold]{mem_gb} GB[/bold]")
    if not check_pod:
        console.print("  [dim]Node sizes unknown: the largest-pod check is skipped.[/dim]")
    console.print()

    continuous = mode_label == "continuous"
    if continuous:
        console.print(
            "  [dim]Continuous: sized for a corpus generated before the streams start "
            "(generate, then run --skip-generate within an hour).[/dim]"
        )
    verdicts: dict[int, CapacityVerdict] = {}

    def verdict_at(scale: int) -> CapacityVerdict:
        if scale not in verdicts:
            verdicts[scale] = check_capacity(
                _config_at(base, scale),
                cluster,
                check_pod=check_pod,
                datagen_runs=not continuous,
            )
        return verdicts[scale]

    best = largest_fitting_scale(lambda s: verdict_at(s).admitted, upper=ceiling)
    full = largest_fitting_scale(lambda s: verdict_at(s).status == "fits", upper=best)
    if best == 0:
        console.print("[yellow]The cluster is below the minimum for scale 1.[/yellow]")
        console.print(f"[dim]Scale 1 needs {floor_text(verdict_at(1).plan)}.[/dim]")
        return 0

    console.print(
        f"[green]Largest scale that fits:[/green] [bold]{best:,}[/bold] (~{data_size(best)})"
    )
    if full < best:
        console.print(
            f"[yellow]Above scale {full:,} the run caps its streams to fit (degraded).[/yellow]"
        )
    if best == ceiling:
        console.print(
            f"[dim]Bounded by the {workload} datagen ceiling of {ceiling}, not by the "
            "cluster.[/dim]"
        )
    if continuous and best > 50:
        console.print(
            "[yellow]Above scale 50 a continuous run that generates its own corpus is "
            "refused by the preflight (datagen is sized to the cluster and counted beside "
            "the streams); generate first, then run --skip-generate within an hour of "
            "generation finishing.[/yellow]"
        )

    table = Table(title="Scale options (what run requests on this cluster)")
    table.add_column("Scale", justify="right", style="cyan")
    table.add_column("Data", justify="right")
    table.add_column("Cores", justify="right")
    table.add_column("Memory", justify="right")
    table.add_column("Status")
    points = sorted({*(m for m in _MILESTONES if m <= ceiling), best})
    nxt = [m for m in points if m > best]
    for scale in [m for m in points if m <= best] + nxt[:1]:
        v = verdict_at(scale)
        if v.status == "refused":
            short = plan_shortfalls(v.plan, cluster, check_pod=check_pod)
            status = f"[red]needs {', '.join(short) or 'more capacity'}[/red]"
        elif scale == best:
            status = "[green bold]<- largest[/green bold]"
        elif v.status == "degraded":
            status = "[yellow]OK, streams capped[/yellow]"
        else:
            status = "[green]OK[/green]"
        if v.warnings and v.status == "fits":
            status += " [dim](datagen pods queue)[/dim]"
        table.add_row(
            f"{scale:,}",
            data_size(scale),
            f"{v.plan.floor.cpu_cores:,}",
            f"{v.plan.floor.memory_gb:,} GB",
            status,
        )
    console.print(table)
    console.print(f"\n[dim]Next: lakebench init --scale {best}[/dim]")
    return 0
