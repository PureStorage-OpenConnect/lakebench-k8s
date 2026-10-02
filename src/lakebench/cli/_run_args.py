"""Every ``lakebench run`` argument checked before the first cluster call.

``run`` used to reach the cluster (capacity read, auto-deploy, the operator
watch list, the scripts ConfigMaps) before it looked at its own flags, so an
unknown ``--stage`` or a flag the chosen mode ignores was found only after
something had changed on the cluster, or never. ``validate_run_args`` is
pure: it reads the options and the loaded config, and refuses a bad
argument or combination with ``UsageError`` (exit 2) before anything else
runs.

The rules are one table, ``RUN_RULES``: a predicate over ``RunArgs`` and the
resolved mode, the refusal, what to do instead, and the line the CLI
reference lists (``tests/test_run_args.py`` checks the reference has exactly
these lines).

``--local`` with a continuous run is an unsupported combination, refused
just after these rules by the support check (``config.support``), as is any
workload, recipe and mode ``run`` does not support; neither makes a cluster
call.

Benchmark settings ``run`` does not honour (another mode, a cold cache,
several streams) are refused when the config loads for ``run``
(``config.schema.BenchmarkConfig``), so they are not repeated here.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from lakebench.exit_codes import UsageError

#: The batch pipeline's stages, in order.
BATCH_STAGES: tuple[str, ...] = ("bronze-verify", "silver-build", "gold-finalize")

#: The shortest continuous window ``--duration`` accepts, in seconds.
MIN_DURATION_S = 60


@dataclass(frozen=True)
class RunArgs:
    """The options ``lakebench run`` was given."""

    stage: str | None = None
    timeout: int | None = None
    skip_benchmark: bool = False
    continuous: bool = False
    sustained: bool = False
    duration: int | None = None
    include_datagen: bool = False
    skip_deploy: bool = False
    skip_generate: bool = False
    regenerate: bool = False
    skip_maintenance: bool = False
    force_rebuild: bool = False
    force_reset: bool = False
    deploy_only: bool = False
    generate_only: bool = False
    yes: bool = False
    local: bool = False


@dataclass(frozen=True)
class RunPlan:
    """What the arguments resolve to: the mode the run uses."""

    mode: str  # "batch" or "continuous"
    local: bool


@dataclass(frozen=True)
class RunRule:
    """One refused argument or combination."""

    #: True when the arguments break the rule.
    broken: Callable[[RunArgs, str], bool]
    #: What is refused, in one sentence (or built from the arguments).
    message: str | Callable[[RunArgs], str]
    #: What to do instead.
    next: str
    #: The rule as the CLI reference lists it (docs/cli-reference.md).
    doc: str

    def text(self, args: RunArgs) -> str:
        return self.message(args) if callable(self.message) else self.message


def _local_stages() -> tuple[str, ...]:
    from lakebench.cli._local import LOCAL_JOB_ORDER

    return LOCAL_JOB_ORDER


def _bad_stage(a: RunArgs, mode: str) -> bool:
    if not a.stage or mode != "batch":
        return False
    return a.stage not in (_local_stages() if a.local else BATCH_STAGES)


RUN_RULES: tuple[RunRule, ...] = (
    RunRule(
        _bad_stage,
        lambda a: f"Unknown stage: {a.stage}",
        f"use one of {', '.join(BATCH_STAGES)}",
        "`--stage` that is not `bronze-verify`, `silver-build` or `gold-finalize`",
    ),
    RunRule(
        lambda a, mode: bool(a.stage) and mode == "continuous",
        "--stage does not apply to a continuous run (its three streams run together)",
        "drop --stage, or run in batch mode",
        "`--stage` with a continuous run (flag or config)",
    ),
    RunRule(
        lambda a, mode: a.deploy_only and a.generate_only,
        "--deploy-only and --generate-only cannot be combined",
        "pick one: --generate-only also deploys",
        "`--deploy-only` with `--generate-only`",
    ),
    RunRule(
        lambda a, mode: a.deploy_only and (bool(a.stage) or a.include_datagen or a.skip_generate),
        "--deploy-only only deploys: --stage, --generate and --skip-generate do not apply",
        "drop them, or drop --deploy-only",
        "`--deploy-only` with `--stage`, `--generate` or `--skip-generate`",
    ),
    RunRule(
        lambda a, mode: a.generate_only and a.skip_generate,
        "--generate-only and --skip-generate cannot be combined",
        "pick one",
        "`--generate-only` with `--skip-generate`",
    ),
    RunRule(
        lambda a, mode: (
            a.local and (a.deploy_only or a.generate_only or a.force_rebuild or a.skip_maintenance)
        ),
        "--local does not deploy, generate on its own, rebuild or run maintenance: "
        "--deploy-only, --generate-only, --force-rebuild and --skip-maintenance do not apply",
        "drop them with --local",
        "`--local` with `--deploy-only`, `--generate-only`, `--force-rebuild` or "
        "`--skip-maintenance`",
    ),
    RunRule(
        lambda a, mode: a.regenerate and not (a.include_datagen or a.generate_only),
        "--regenerate only applies with --generate or --generate-only",
        "add --generate, or drop --regenerate",
        "`--regenerate` without `--generate` or `--generate-only`",
    ),
    RunRule(
        lambda a, mode: (
            a.regenerate and (a.local or (mode == "continuous" and not a.generate_only))
        ),
        "--regenerate does not apply to a local or continuous run (a continuous run "
        "clears and regenerates its own data)",
        "drop --regenerate",
        "`--regenerate` with `--local`, or with a continuous run other than `--generate-only`",
    ),
    RunRule(
        lambda a, mode: a.skip_generate and a.include_datagen,
        "--skip-generate and --generate cannot be combined",
        "pick one",
        "`--skip-generate` with `--generate`",
    ),
    RunRule(
        lambda a, mode: a.force_reset and mode == "batch",
        "--force-reset only applies to a continuous run",
        "drop --force-reset, or add --continuous",
        "`--force-reset` on a batch run",
    ),
    RunRule(
        lambda a, mode: a.force_rebuild and mode == "continuous",
        "--force-rebuild only applies to a batch run",
        "drop --force-rebuild, or run in batch mode",
        "`--force-rebuild` on a continuous run",
    ),
    RunRule(
        lambda a, mode: a.duration is not None and mode == "batch",
        "--duration only applies to a continuous run",
        "drop --duration, or add --continuous",
        "`--duration` on a batch run",
    ),
    RunRule(
        lambda a, mode: a.duration is not None and a.duration < MIN_DURATION_S,
        f"--duration is below {MIN_DURATION_S} s",
        f"use --duration {MIN_DURATION_S} or more",
        f"`--duration` below {MIN_DURATION_S}",
    ),
    RunRule(
        lambda a, mode: a.timeout is not None and a.timeout < 1,
        "--timeout must be at least 1 s",
        "use a positive --timeout, or leave it out for the scaled default",
        "`--timeout` below 1",
    ),
)


def run_mode(args: RunArgs, cfg: Any) -> str:
    """The mode this run uses: ``--continuous`` (or ``--sustained``) wins
    over the config, which is not written back."""
    from lakebench.config.schema import is_continuous_mode

    if args.continuous or args.sustained:
        return "continuous"
    return "continuous" if is_continuous_mode(cfg.architecture.pipeline.mode) else "batch"


def run_args_problems(args: RunArgs, cfg: Any) -> list[RunRule]:
    """Every rule *args* break (empty when the run may start)."""
    mode = run_mode(args, cfg)
    return [rule for rule in RUN_RULES if rule.broken(args, mode)]


def validate_run_args(args: RunArgs, cfg: Any) -> RunPlan:
    """The resolved plan, or ``UsageError`` (exit 2) naming the first broken
    rule (and how many more there are). Makes no cluster call."""
    problems = run_args_problems(args, cfg)
    if problems:
        first = problems[0]
        more = f" ({len(problems) - 1} more refused argument(s))" if len(problems) > 1 else ""
        raise UsageError(f"{first.text(args)}{more}", next=first.next, path="run.args")
    from lakebench.config.c360_run import (
        GOLD_STRATEGY_KEY,
        GOLD_STRATEGY_VALUES,
        gold_override_problem,
    )

    gold = gold_override_problem(cfg)
    if gold:
        raise UsageError(
            gold,
            next=f"remove {GOLD_STRATEGY_KEY} from spark.conf, or set it to "
            + ", ".join(GOLD_STRATEGY_VALUES),
            path="spark.conf",
        )
    return RunPlan(mode=run_mode(args, cfg), local=args.local)
