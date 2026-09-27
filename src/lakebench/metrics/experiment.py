"""The experiment block: what produced a run, and whether two runs compare.

Every metrics.json carries an ``experiment`` block (mission outcome 3,
invariant 5): the workload and its version, the corpus that was generated
(generator image and digest, seed, corpus role, scale), the architecture
composition and component versions, the mode, the table-maintenance policy,
the stages and detection rules that ran or were skipped, the limits
Lakebench imposed on the run, and a result fingerprint per benchmark query
(benchmark.fingerprint).

``lakebench compare`` and the perf gate read it through ``refusals``: two
runs whose workload, corpus, seed, scale or mode differ, or whose benchmark
queries returned different results, are not compared on performance
(invariant 2). A record without the block (written before it existed) is
"not comparable: no provenance".

The block is assembled in two halves. ``experiment_inputs(cfg)`` runs when
the run starts and is stored in ``config_snapshot["experiment_inputs"]``
(outside the perf-gate fingerprint keys, so adding it moved no
fingerprint). ``build_experiment(metrics)`` runs when metrics.json is
written and adds what only the run knows.

WORKLOAD_VERSIONS history (bump a workload's version whenever what it
computes changes: the pipeline scripts' output semantics, the detection
rules, or the benchmark queries; a run under another version compares with
nothing measured under this one):

- ``c360-1``, ``aml-1``: first stamped versions (2026-09-26).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

EXPERIMENT_SCHEMA = "exp1"

WORKLOAD_VERSIONS: dict[str, str] = {
    "customer360": "c360-1",
    "financial": "aml-1",
    "custom": "custom-1",
}

#: The AML generator's MODEL_VERSION in this tree (datagen_rs/src/model.rs,
#: kept equal by tests/test_experiment.py). The Customer 360 generator has
#: no model version of its own; its identity is the image reference, whose
#: tag is the datagen_rs commit it was built from.
DATAGEN_MODEL_VERSIONS: dict[str, str | None] = {
    "financial": "datagen-v2-rs-0.3",
    "customer360": None,
    "custom": None,
}

NO_PROVENANCE = "not comparable: no provenance"

# Batch and continuous Spark job types -> snapshot executor-override key.
_OVERRIDE_KEY = {
    "bronze-verify": "bronze",
    "silver-build": "silver",
    "gold-finalize": "gold",
    "bronze-ingest": "bronze_ingest",
    "silver-stream": "silver_stream",
    "gold-refresh": "gold_refresh",
}


def _short_hash(obj: Any) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


#: Recipe names spell the query engine slot as in config/recipes.py.
_RECIPE_SLOT_NAMES = {"spark-thrift": "thrift"}


def _recipe(snapshot_like: Mapping[str, Any]) -> str:
    """The recipe name of a composition: the RECIPES key when a recipe has
    these components, else the same naming scheme (--local's hadoop-catalog
    composition has no recipe)."""
    from lakebench.config.support import recipe_for

    parts = [
        str(snapshot_like.get(k) or "?")
        for k in ("catalog", "table_format", "pipeline_engine", "query_engine")
    ]
    name = recipe_for(*parts)
    if name:
        return name
    parts[3] = _RECIPE_SLOT_NAMES.get(parts[3], parts[3])
    return "-".join(parts)


# ---------------------------------------------------------------------------
# Config half
# ---------------------------------------------------------------------------


def _batch_retention(cfg: Any) -> str | None:
    try:
        from lakebench.cli._sustained import resolve_maintenance_retention

        return str(resolve_maintenance_retention(cfg))
    except Exception:  # noqa: BLE001
        return None


def effective_trickle(cfg: Any) -> int | None:
    """max_files_per_trigger as a continuous run of *cfg* uses it: the config
    value, or the auto value for its run_duration (cli/_sustained
    resolve_trickle). A continuous run resolves it onto the config before
    anything is recorded; this covers records built from a config directly."""
    value = cfg.architecture.pipeline.sustained.max_files_per_trigger
    if value is not None:
        return value
    try:
        from lakebench.cli._sustained import resolve_trickle

        return resolve_trickle(cfg, cfg.architecture.pipeline.sustained.run_duration)["value"]
    except Exception:  # noqa: BLE001
        return None


def _stackable_hive(cfg: Any) -> str:
    """The Hive the Stackable operator runs: images.hive is only the
    HiveCluster productVersion, and the image is resolved by the operator
    from it and the SDP release (oci.stackable.tech/sdp/hive:<v>-stackable<sdp>).
    Derived from the configured versions, not read from the pod."""
    from lakebench.deploy.engine import image_tag

    version = image_tag(cfg.images.hive)
    sdp = cfg.architecture.catalog.hive.operator.version
    return f"oci.stackable.tech/sdp/hive:{version}-stackable{sdp} (derived, not read from the pod)"


def _c360_resolved(cfg: Any) -> dict[str, Any]:
    """The Customer 360 parameters the generator was given.

    ``unique_customers`` is the customer id space passed to datagen
    (``datagen_customer_id_max``), the same value the c360 correctness check
    uses. ``date_range_days`` is not recorded: datagen never reads it; the
    event window is ``timestamp_start``/``timestamp_end`` (defaults as the
    correctness check resolves them). A value that cannot be resolved is
    left out, never guessed.
    """
    c360 = cfg.architecture.workload.customer360
    out: dict[str, Any] = {}
    try:
        out["unique_customers"] = int(cfg.get_scale_dimensions().customers)
        out["unique_customers_source"] = (
            "customer360.unique_customers" if c360.unique_customers is not None else "scale"
        )
    except Exception:  # noqa: BLE001 -- recorded as absent, never guessed
        pass
    try:
        from lakebench.metrics.c360_correctness import expected_context

        ctx = expected_context(cfg)
        out["event_window"] = {"start": ctx["window_start"], "end": ctx["window_end"]}
    except Exception:  # noqa: BLE001
        pass
    return out


def _canonical_mode(mode: Any) -> str:
    from lakebench.config.support import canonical_mode

    return canonical_mode(mode)


def _frozen_support(cfg: Any, run_mode: str | None, system: str) -> dict[str, Any]:
    from lakebench.config.support import support_state_for_config
    from lakebench.metrics.provenance import run_provenance

    try:
        return support_state_for_config(cfg, run_mode, system=system, provenance=run_provenance())
    except Exception as e:  # noqa: BLE001 -- recorded, never raised from a snapshot
        return {"state": "unverified", "basis": f"support state not computed: {e}"}


def experiment_inputs(
    cfg: Any, *, run_mode: str | None = None, system: str = "cluster"
) -> dict[str, Any]:
    """The config-derived half of the experiment block.

    *run_mode* is the mode the run will use when the caller knows it
    (``run --continuous`` does not write the mode back to the config). The
    support state is computed here, at run start, and frozen: re-rendering a
    record later with a newer validation record must not change what the run
    was stamped with.
    """
    arch = cfg.architecture
    workload = arch.workload
    dg = workload.datagen
    schema = workload.schema_type.value
    images = cfg.images

    seed: int | None
    seed_error = None
    try:
        from lakebench.config.datagen_seed import config_seed

        seed = config_seed(cfg)
    except Exception as e:  # noqa: BLE001 -- recorded, never raised from a snapshot
        seed, seed_error = None, str(e)

    if schema == "financial":
        params: dict[str, Any] = {
            "tm_operations": workload.tm_operations.model_dump(mode="json"),
            "w1_max_vertices": workload.w1_max_vertices,
            "retention_workload": workload.retention_workload,
            "retention_months": workload.retention_months,
        }
        params_id = _short_hash(params)
    else:
        # The id hashes the declared overrides, as before, so runs recorded
        # before the resolved values were stamped stay comparable; what they
        # resolve to is a function of scale and the timestamp window, both
        # already corpus identity.
        declared = {"customer360": workload.customer360.model_dump(mode="json")}
        params_id = _short_hash(declared)
        params = {"customer360": _c360_resolved(cfg)}

    corpus = {
        "schema": schema,
        "generator_image": images.datagen,
        "seed": seed,
        "corpus_role": dg.corpus_role,
        "robustness_perturbation": dg.robustness_perturbation,
        "scale": dg.get_effective_scale(),
        "timestamp_start": dg.timestamp_start,
        "timestamp_end": dg.timestamp_end,
        "dirty_data_ratio": dg.dirty_data_ratio,
        "unique_customers": workload.customer360.unique_customers,
        "date_range_days": workload.customer360.date_range_days,
    }
    corpus["id"] = _short_hash(corpus)
    # The id keeps hashing the declared overrides (null when unset), so ids
    # recorded before this stay valid. The record shows what ran instead:
    # the resolved customer id space for c360, and no date_range_days,
    # which datagen never reads (the window is timestamp_start/_end).
    declared_customers = corpus.pop("unique_customers", None)
    corpus.pop("date_range_days", None)
    if schema != "financial":
        resolved = params["customer360"].get("unique_customers")
        if resolved is not None:
            corpus["unique_customers"] = resolved
        elif declared_customers is not None:
            corpus["unique_customers"] = declared_customers
    if seed_error:
        corpus["seed_error"] = seed_error

    table_format = arch.table_format.type.value
    format_version: str | None
    try:
        from lakebench.modules.pipeline_engines.spark.job import resolve_format_version

        requested = (
            arch.table_format.iceberg.version
            if table_format == "iceberg"
            else arch.table_format.delta.version
        )
        format_version = resolve_format_version(images.spark, table_format, requested)
    except Exception:  # noqa: BLE001
        format_version = None

    catalog = arch.catalog.type.value
    catalog_version = {
        "hive": _stackable_hive(cfg),
        "polaris": images.polaris,
        "unity": images.unity,
    }.get(catalog)
    query_engine = arch.query_engine.type.value
    engine_version = {
        "trino": images.trino,
        "spark-thrift": images.spark,
        "duckdb": f"duckdb {arch.query_engine.duckdb.version} on {images.duckdb}",
        "none": None,
    }.get(query_engine)

    sustained = arch.pipeline.sustained
    return {
        "workload": {
            "name": schema,
            "version": WORKLOAD_VERSIONS.get(schema, f"{schema}-unversioned"),
            "generator_model_version": DATAGEN_MODEL_VERSIONS.get(schema),
            "parameters_id": params_id,
            "parameters": params,
        },
        "corpus": corpus,
        "architecture": {
            "recipe": _recipe(
                {
                    "catalog": catalog,
                    "table_format": table_format,
                    "pipeline_engine": arch.pipeline_engine.value,
                    "query_engine": query_engine,
                }
            ),
            "catalog": {"type": catalog, "version": catalog_version},
            "table_format": {"type": table_format, "version": format_version},
            "pipeline_engine": {"type": arch.pipeline_engine.value, "image": images.spark},
            "query_engine": {"type": query_engine, "version": engine_version},
            # How the query engine reaches the tables: through the catalog
            # (Trino, Spark Thrift) or straight from object storage (DuckDB
            # reads the table's metadata files, not the catalog's pointer).
            "query_access_path": {
                "trino": "catalog",
                "spark-thrift": "catalog",
                "duckdb": "direct_storage",
            }.get(query_engine),
        },
        "maintenance_config": {
            "pre_benchmark_maintenance": arch.pipeline.pre_benchmark_maintenance,
            "compaction_enabled": sustained.compaction_enabled,
            # How aggressive the maintenance is, when it runs: an execution
            # condition beside what ran.
            "settings": (
                {
                    "retention_threshold": sustained.retention_threshold,
                    "retention_interval": sustained.retention_interval,
                    "compaction_interval": sustained.compaction_interval,
                }
                if arch.pipeline.mode.value in ("sustained", "continuous")
                else {
                    "retention_threshold": _batch_retention(cfg),
                    # Batch pre-benchmark expiry does not use
                    # sustained.retention_threshold: resolve_maintenance_retention
                    # gives 0s (every older snapshot) unless the workload
                    # retains history (retention_workload).
                    "retention_source": "resolve_maintenance_retention (batch pre-benchmark)",
                }
            ),
        },
        "mode": arch.pipeline.mode.value,
        **({"run_mode": _canonical_mode(run_mode)} if run_mode else {}),
        "support": _frozen_support(cfg, run_mode, system),
        "config_limits": {
            "max_files_per_trigger": effective_trickle(cfg),
            # Continuous in-stream rounds take one sample per query whatever
            # the benchmark block says (collector.CONTINUOUS_ROUND_BENCHMARK).
            "benchmark_iterations": (
                1
                if arch.pipeline.mode.value in ("sustained", "continuous")
                else arch.benchmark.iterations
            ),
            "w1_max_vertices": workload.w1_max_vertices if schema == "financial" else None,
            "tm_max_alerts_per_customer": (
                workload.tm_operations.max_alerts_per_customer if schema == "financial" else None
            ),
        },
    }


# ---------------------------------------------------------------------------
# Run half
# ---------------------------------------------------------------------------


def _executor_caps(metrics: Any, snapshot: Mapping[str, Any], schema: str) -> list[dict]:
    from lakebench.modules.pipeline_engines.spark.job import get_job_profile

    scale = float(snapshot.get("scale") or 0)
    overrides = ((snapshot.get("spark") or {}).get("executor_overrides")) or {}
    out = []
    job_types = [j.job_type for j in metrics.jobs] + [s.job_type for s in metrics.streaming]
    for job_type in dict.fromkeys(job_types):
        profile = get_job_profile(job_type, schema)
        if not profile or "base_executors" not in profile:
            continue
        base = int(profile["base_executors"])
        uncapped = (
            base
            if scale <= 10
            else base + int((scale - 10) * profile["executors_per_100_scale"] // 100)
        )
        override = overrides.get(_OVERRIDE_KEY.get(job_type, ""))
        cap = int(profile["max_executors"])
        observed = next(
            (j.executor_count for j in metrics.jobs if j.job_type == job_type and j.executor_count),
            None,
        )
        streaming = [s for s in metrics.streaming if s.job_type == job_type]
        if observed is None and streaming:
            # What the stream actually requested, after the concurrent budget.
            observed = next(
                (s.requested_executors for s in streaming if s.requested_executors), None
            )
        wanted = override if override is not None else min(uncapped, cap)
        entry = {
            "job_type": job_type,
            "scale_derived": uncapped,
            "cap": cap,
            "override": override,
            "cap_hit": override is None and uncapped > cap,
            "observed": observed,
        }
        if streaming and override is None and observed is not None and observed < wanted:
            # Continuous streams share a concurrent executor budget
            # (job.py): the cluster granted fewer than the profile asks.
            entry["budget_cap"] = {"requested": wanted, "granted": observed}
        out.append(entry)
    return out


def _stages(metrics: Any, pb: Any) -> tuple[list[str], list[str]]:
    executed: list[str] = []
    skipped: list[str] = []
    stages = list(getattr(pb, "stages", None) or [])
    if stages:
        for s in stages:
            (executed if s.success else skipped).append(
                s.stage_name if s.success else f"{s.stage_name} (failed)"
            )
    else:
        for j in metrics.jobs:
            (executed if j.success else skipped).append(
                j.job_type if j.success else f"{j.job_type} (failed)"
            )
        for s in metrics.streaming:
            (executed if s.success else skipped).append(
                s.job_type if s.success else f"{s.job_type} (failed)"
            )
    has_bench = metrics.benchmark is not None or bool(metrics.benchmark_rounds)
    if not has_bench and "query" not in executed:
        skipped.append("benchmark (not run)")
    if str(metrics.maintenance_policy_id).endswith("+skipped"):
        skipped.append("table maintenance (--skip-maintenance)")
    return executed, skipped


def _rules(metrics: Any) -> dict[str, Any]:
    ran: dict[str, int] = {}
    skipped: dict[str, str] = {}
    errors: dict[str, str] = {}
    for j in metrics.jobs:
        ran.update(j.alerts_by_rule or {})
        skipped.update(j.rules_skipped or {})
        errors.update(j.rule_errors or {})
    for s in metrics.streaming:
        for rule in s.ttd_by_rule or {}:
            ran.setdefault(rule, 0)
    if not (ran or skipped or errors):
        return {}
    return {
        "executed": sorted(r for r in ran if r not in errors),
        "skipped": dict(sorted(skipped.items())),
        "errored": dict(sorted(errors.items())),
    }


def _caps_bound(limits: Mapping[str, Any], rules: Mapping[str, Any]) -> list[str]:
    """The Lakebench limits that bound this run (invariant 4), one line each.
    A figure measured under one of these is not what the system can do."""
    out = [
        f"{x['job_type']}: executor cap {x['cap']} (scale asks for {x['scale_derived']})"
        for x in limits.get("executors") or []
        if x.get("cap_hit")
    ]
    out += [
        f"{x['job_type']}: concurrent executor budget granted {x['budget_cap']['granted']} "
        f"of {x['budget_cap']['requested']}"
        for x in limits.get("executors") or []
        if x.get("budget_cap")
    ]
    if limits.get("tm_alerts_over_capacity"):
        out.append(
            f"TM max_alerts_per_customer ({limits.get('tm_max_alerts_per_customer')}): "
            f"{limits['tm_alerts_over_capacity']} alerts over capacity"
        )
    out += [f"auto-sizing: {c}" for c in limits.get("autosize_cuts") or []]
    if limits.get("maintenance_stopped"):
        out.append("pre-benchmark maintenance stopped on its time budget")
    for rule, why in (rules.get("skipped") or {}).items():
        if "cap" in str(why):
            out.append(f"rule {rule} skipped: {why}")
    return out


def _bound_kinds(limits: Mapping[str, Any], rules: Mapping[str, Any]) -> list[str]:
    """Which limits bound, without the counts: the like-for-like condition.
    The counts (a concurrent budget granted from live cluster capacity)
    vary between runs of one config and stay in ``bound`` as evidence."""
    out = {
        f"{x['job_type']}: executor cap" for x in limits.get("executors") or [] if x.get("cap_hit")
    }
    out |= {
        f"{x['job_type']}: concurrent executor budget"
        for x in limits.get("executors") or []
        if x.get("budget_cap")
    }
    if limits.get("tm_alerts_over_capacity"):
        out.add("TM max_alerts_per_customer")
    if limits.get("autosize_cuts"):
        out.add("auto-sizing cuts")
    if limits.get("maintenance_stopped"):
        out.add("pre-benchmark maintenance budget")
    out |= {f"rule {r} cap" for r, why in (rules.get("skipped") or {}).items() if "cap" in str(why)}
    return sorted(out)


def _benchmark_queries(metrics: Any) -> list[dict[str, Any]]:
    bench = metrics.benchmark
    if bench is None and metrics.pipeline_benchmark is not None:
        bench = metrics.pipeline_benchmark.query_benchmark
    return list(getattr(bench, "queries", None) or [])


def _continuous_results(metrics: Any) -> dict[str, Any]:
    """A continuous run's results: the fingerprints of the result check the
    CLI runs once the whole corpus has passed through the pipeline and the
    streams have stopped (cli/_sustained.py), when every table is a function
    of the corpus alone. The in-stream rounds read tables still being
    written and are never fingerprinted."""
    check = (getattr(metrics, "continuous", None) or {}).get("result_check") or {}
    fps = dict(check.get("fingerprints") or {})
    if fps and not check.get("not_checked"):
        return {
            "query_set_id": check.get("query_set_id"),
            "fingerprints": fps,
            "basis": "continuous result check after the corpus settled",
        }
    return {
        "query_set_id": check.get("query_set_id"),
        "fingerprints": {},
        "not_checked": "continuous: "
        + str(check.get("not_checked") or "no end-of-run result check was recorded"),
    }


def _results(metrics: Any, mode: str) -> dict[str, Any]:
    if mode in ("sustained", "continuous"):
        return _continuous_results(metrics)
    bench = metrics.benchmark
    if bench is None and metrics.pipeline_benchmark is not None:
        bench = metrics.pipeline_benchmark.query_benchmark
    if bench is None:
        return {"query_set_id": None, "fingerprints": {}, "not_checked": "no benchmark ran"}
    fps: dict[str, Any] = {}
    for q in _benchmark_queries(metrics):
        name = q.get("name") or q.get("query_name")
        if name:
            fps[str(name)] = q.get("result_fingerprint")
    return {
        "query_set_id": getattr(bench, "query_set_id", None),
        "fingerprints": fps,
    }


def _datagen(metrics: Any, inputs: Mapping[str, Any]) -> dict[str, Any]:
    fleet = metrics.datagen_fleet or {}
    image = fleet.get("image")
    ids = [i for i in (fleet.get("image_ids") or []) if i]
    digest = None
    reason = None
    if len(ids) == 1 and "@" in ids[0]:
        digest = ids[0].split("@", 1)[1]
    elif len(ids) > 1:
        reason = "datagen pods ran different images: " + ", ".join(sorted(ids))
    elif not fleet:
        reason = "no datagen fleet record for this run (data generated elsewhere or earlier)"
    else:
        reason = "the datagen fleet record carries no pod image id"
    out: dict[str, Any] = {
        "configured_image": (inputs.get("corpus") or {}).get("generator_image"),
        "pod_image": image,
        "digest": digest,
        **({"digest_reason": reason} if digest is None else {}),
        # Corpus parameters the datagen pods ran with (their container args),
        # when the fleet record carries them.
        "observed": bool(fleet) and any(fleet.get(k) is not None for k in ("seed", "scale")),
        "seed": fleet.get("seed"),
        "scale": fleet.get("scale"),
        "data_quality": fleet.get("data_quality"),
        "mixed_params": list(fleet.get("mixed_params") or []),
    }
    return out


def _observed_corpus(corpus: Mapping[str, Any], dg: Mapping[str, Any]) -> tuple[dict, list[str]]:
    """The corpus as generated: the config's values replaced by what the
    datagen pods ran with where the fleet record says, and the problems that
    make it not one known corpus (config and pods disagree, pods disagree)."""
    out = dict(corpus)
    problems: list[str] = []
    if dg.get("data_quality") == "mixed":
        problems.append(
            "datagen pods did not write one corpus (mixed: "
            + ", ".join(dg.get("mixed_params") or ["?"])
            + ")"
        )
    if dg.get("pod_image") and "," in str(dg["pod_image"]):
        problems.append(f"datagen pods ran different images ({dg['pod_image']})")
    for key in ("seed", "scale"):
        seen = dg.get(key)
        if seen is None:
            continue
        declared = corpus.get(key)
        # The pods get --scale as "%.6f" (deploy/datagen.py).
        if declared is not None and abs(float(declared) - float(seen)) > 1e-6:
            problems.append(f"config {key} {declared!r} but the datagen pods ran {seen!r}")
        out[key] = int(seen) if key == "seed" else float(seen)
    if dg.get("pod_image") and "," not in str(dg["pod_image"]):
        if corpus.get("generator_image") and dg["pod_image"] != corpus["generator_image"]:
            problems.append(
                f"config generator image {corpus['generator_image']!r} but the pods ran "
                f"{dg['pod_image']!r}"
            )
        out["generator_image"] = dg["pod_image"]
    out["observed"] = bool(dg.get("observed"))
    if not out["observed"]:
        out["observed_note"] = "declared by the config, not observed from the datagen pods"
    return out, problems


def support_state(
    workload: str | None, arch: Mapping[str, Any], mode: str | None, *, system: str = "cluster"
) -> dict[str, Any]:
    """The DESIGN 6.5 support state of a run's workload x architecture x mode,
    computed by lakebench.config.support from layers 1-3 and the release
    validation record (config/validated_combinations.yaml)."""
    from lakebench.config.support import support_state as _state

    def t(key: str) -> str | None:
        v = arch.get(key)
        return (v or {}).get("type") if isinstance(v, Mapping) else v

    return _state(
        workload,
        t("catalog"),
        t("table_format"),
        t("pipeline_engine"),
        t("query_engine"),
        mode,
        system=system,
    )


def _recorded_support(
    inputs: Mapping[str, Any], schema: str, arch: Mapping[str, Any], mode: str, local: bool
) -> dict[str, Any]:
    """The support state frozen at run start. A record from before it was
    frozen is recomputed, but never as supported: the validation record in
    force when it ran is unknown."""
    frozen = inputs.get("support")
    if isinstance(frozen, Mapping) and frozen.get("state"):
        return dict(frozen)
    out = support_state(schema, arch, mode, system="local" if local else "cluster")
    if out.get("state") == "supported":
        out.update(
            state="unverified",
            basis="support state not recorded at run start; not re-stamped as supported",
        )
    return out


def _repetitions(metrics: Any) -> dict[str, Any]:
    """How many measurements stand behind the run's figures (invariant 5).
    One run is n=1 for any claim of run-to-run repeatability."""
    from lakebench.benchmark.spread import samples_per_query

    bench = metrics.benchmark
    if bench is None and metrics.pipeline_benchmark is not None:
        bench = metrics.pipeline_benchmark.query_benchmark
    queries = list(getattr(bench, "queries", None) or [])
    return {
        "runs": 1,
        "benchmark_samples_per_query": samples_per_query(queries) if queries else None,
        "benchmark_rounds": len(getattr(metrics, "benchmark_rounds", None) or []) or None,
    }


def build_experiment(metrics: Any) -> dict[str, Any] | None:
    """The experiment block for *metrics* (a PipelineMetrics), or None when
    its snapshot has no experiment inputs (a record from before them)."""
    snapshot = metrics.config_snapshot or {}
    inputs = snapshot.get("experiment_inputs")
    if not isinstance(inputs, Mapping):
        return None
    pb = metrics.pipeline_benchmark
    mode = (
        inputs.get("run_mode")
        or getattr(pb, "pipeline_mode", None)
        or inputs.get("mode")
        or "batch"
    )
    mode = "sustained" if mode == "continuous" else mode
    schema = (inputs.get("workload") or {}).get("name") or "customer360"
    executed, skipped = _stages(metrics, pb)
    limits = dict(inputs.get("config_limits") or {})
    limits["executors"] = _executor_caps(metrics, snapshot, schema)
    limits["autosize_cuts"] = list(getattr(metrics, "autosize_cuts", None) or [])
    if mode == "sustained":
        limits["intake_limit"] = getattr(pb, "intake_limit", None)
    else:
        limits.pop("max_files_per_trigger", None)
    if pb is not None:
        limits["maintenance_stopped"] = getattr(pb, "maintenance_stopped", None)
    sampling = (metrics.financial_scoring or {}).get("sampling")
    if sampling:
        limits["scoring_sampling"] = sampling
    tm_over = 0
    for j in metrics.jobs:
        ops = getattr(j, "tm_ops", None) or {}
        tm_over = max(tm_over, int(ops.get("alerts_over_capacity") or 0))
    if tm_over:
        limits["tm_alerts_over_capacity"] = tm_over
    rules = _rules(metrics)
    limits["bound"] = _caps_bound(limits, rules)
    limits["bound_kinds"] = _bound_kinds(limits, rules)
    bench = metrics.benchmark
    if bench is None and metrics.pipeline_benchmark is not None:
        bench = metrics.pipeline_benchmark.query_benchmark
    if bench is not None:
        # What the recorded benchmark ran with (lakebench benchmark can
        # rerun it with other --iterations or --mode than the config's).
        limits["benchmark_iterations"] = getattr(bench, "iterations", None) or limits.get(
            "benchmark_iterations"
        )
        limits["benchmark_mode"] = getattr(bench, "mode", None)
    from lakebench.metrics.maintenance_policy import effective_maintenance

    arch = dict(inputs.get("architecture") or {})
    local = bool(snapshot.get("local"))
    if local:
        # --local runs DuckDB against the tables in local object storage,
        # whatever query engine and catalog the config names.
        arch.update(
            {
                "configured_recipe": arch.get("recipe"),
                "catalog": {"type": "none", "version": "local: tables read from storage"},
                "query_engine": {"type": "duckdb", "version": "local container"},
                "query_access_path": "direct_storage",
            }
        )
        arch["recipe"] = _recipe(
            {
                "catalog": "none",
                "table_format": (arch.get("table_format") or {}).get("type"),
                "pipeline_engine": (arch.get("pipeline_engine") or {}).get("type"),
                "query_engine": "duckdb",
            }
        )
    mcfg = inputs.get("maintenance_config") or {}
    effective = effective_maintenance(
        metrics.maintenance_policy_id,
        table_format=(arch.get("table_format") or {}).get("type"),
        query_engine=(arch.get("query_engine") or {}).get("type"),
        mode=mode,
        pre_benchmark_maintenance=mcfg.get("pre_benchmark_maintenance"),
        compaction_enabled=mcfg.get("compaction_enabled"),
        stopped=getattr(pb, "maintenance_stopped", False) if mode == "batch" else False,
        outcomes=[] if local else getattr(metrics, "maintenance_outcomes", None),
    )
    dg = _datagen(metrics, inputs)
    corpus, corpus_problems = _observed_corpus(dict(inputs.get("corpus") or {}), dg)
    corpus["datagen"] = dg
    if corpus_problems:
        corpus["problems"] = corpus_problems
    return {
        "schema": EXPERIMENT_SCHEMA,
        "system": "local" if local else "cluster",
        "support": _recorded_support(inputs, schema, arch, mode, local),
        "repetitions": _repetitions(metrics),
        "workload": dict(inputs.get("workload") or {}),
        "corpus": corpus,
        "architecture": arch,
        "maintenance_settings": dict(mcfg.get("settings") or {}),
        "mode": mode,
        "maintenance_policy_id": metrics.maintenance_policy_id,
        "effective_maintenance": effective,
        "stages": {"executed": executed, "skipped": skipped},
        "rules": rules,
        "limits": limits,
        "results": _results(metrics, mode),
        "lakebench": dict(metrics.provenance or {}),
    }


def planned_experiment(cfg: Any) -> dict[str, Any]:
    """What ``identity`` can know about a run of *cfg* before it runs (no
    generator digest, the policy the config asks for)."""
    from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID, effective_maintenance

    inputs = experiment_inputs(cfg)
    mode = "sustained" if inputs.get("mode") in ("sustained", "continuous") else "batch"
    arch = inputs["architecture"]
    mcfg = inputs["maintenance_config"]
    return {
        "workload": inputs["workload"],
        "corpus": inputs["corpus"],
        "architecture": {"query_access_path": arch.get("query_access_path")},
        "system": "cluster",
        "limits": {"benchmark_iterations": inputs["config_limits"].get("benchmark_iterations")},
        "maintenance_settings": dict(mcfg.get("settings") or {}),
        "mode": mode,
        "effective_maintenance": effective_maintenance(
            MAINTENANCE_POLICY_ID,
            table_format=arch["table_format"]["type"],
            query_engine=arch["query_engine"]["type"],
            mode=mode,
            pre_benchmark_maintenance=mcfg["pre_benchmark_maintenance"],
            compaction_enabled=mcfg["compaction_enabled"],
        ),
    }


# ---------------------------------------------------------------------------
# Comparability
# ---------------------------------------------------------------------------


def identity(exp: Mapping[str, Any]) -> dict[str, Any]:
    """The fields that must match for two runs to be compared at all."""
    w = exp.get("workload") or {}
    c = exp.get("corpus") or {}
    dg = c.get("datagen") or {}
    return {
        "workload": w.get("name"),
        "workload version": w.get("version"),
        "generator model version": w.get("generator_model_version"),
        "workload parameters": w.get("parameters_id"),
        "corpus id": c.get("id"),
        "generator image": c.get("generator_image"),
        "seed": c.get("seed"),
        "corpus role": c.get("corpus_role"),
        "scale": c.get("scale"),
        "mode": exp.get("mode"),
        "generator digest": dg.get("digest"),
        # Execution conditions (CONDITION_KEYS): a difference makes a pair
        # comparable but not like-for-like (DESIGN 2.4, 6.5). The perf gate
        # and reproduce, which need the same experiment, refuse on them too.
        "effective maintenance": (exp.get("effective_maintenance") or {}).get("id"),
        "maintenance settings": exp.get("maintenance_settings"),
        "query access path": (exp.get("architecture") or {}).get("query_access_path"),
        "system": exp.get("system"),
        "benchmark iterations": (exp.get("limits") or {}).get("benchmark_iterations"),
        "benchmark mode": (exp.get("limits") or {}).get("benchmark_mode"),
        "Lakebench limits that bound": list((exp.get("limits") or {}).get("bound_kinds") or []),
    }


#: identity() keys that are execution conditions, not experiment identity.
CONDITION_KEYS = frozenset(
    {
        "effective maintenance",
        "maintenance settings",
        "query access path",
        "system",
        "benchmark iterations",
        "benchmark mode",
        "Lakebench limits that bound",
    }
)


def corpus_problems(exp: Mapping[str, Any] | None) -> list[str]:
    """Why a run's corpus is not one known corpus (see _observed_corpus)."""
    return list(((exp or {}).get("corpus") or {}).get("problems") or [])


def results_established(exp: Mapping[str, Any] | None) -> bool | str:
    """True when the run recorded benchmark results that can be checked for
    equivalence, else the reason they cannot (DESIGN 6.5: comparable means
    the results are equivalent, which needs results)."""
    res = (exp or {}).get("results") or {}
    if res.get("not_checked"):
        return str(res["not_checked"])
    if not res.get("fingerprints"):
        return "no benchmark query results were recorded"
    return True


def identity_hash(exp: Mapping[str, Any]) -> str:
    ident = identity(exp)
    # A digest is evidence when both sides have one; an unresolved digest on
    # one side is not a difference (the image reference still is).
    ident.pop("generator digest", None)
    return _short_hash(ident)


def experiment_of(record: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    exp = (record or {}).get("experiment")
    return exp if isinstance(exp, Mapping) and exp.get("schema") else None


def diff_identities(ia: Mapping[str, Any], ib: Mapping[str, Any], keys: Any = None) -> list[str]:
    """Differences between two ``identity`` dicts, one line per field
    (only *keys*, when given)."""
    out = []
    for key in dict.fromkeys([*ia, *ib]):
        if keys is not None and key not in keys:
            continue
        va, vb = ia.get(key), ib.get(key)
        if key == "generator digest" and (va is None or vb is None):
            continue
        if va != vb:
            out.append(f"{key} differs ({va!r} vs {vb!r})")
    return out


def identity_differences(a: Mapping[str, Any], b: Mapping[str, Any]) -> list[str]:
    """Experiment-identity differences (not execution conditions)."""
    ia, ib = identity(a), identity(b)
    return diff_identities(ia, ib, [k for k in {**ia, **ib} if k not in CONDITION_KEYS])


def condition_differences(a: Mapping[str, Any], b: Mapping[str, Any]) -> list[str]:
    """Execution-condition differences: comparable, not like-for-like."""
    return diff_identities(identity(a), identity(b), CONDITION_KEYS)


def result_fingerprints(exp: Mapping[str, Any]) -> dict[str, Any]:
    return dict((exp.get("results") or {}).get("fingerprints") or {})


def diff_fingerprints(
    ra: Mapping[str, Any],
    rb: Mapping[str, Any],
    label_a: str = "A",
    label_b: str = "B",
) -> list[str]:
    """One line per benchmark query whose results are not shown equal,
    naming both fingerprints. Empty when every query matches."""
    from lakebench.benchmark.fingerprint import describe, mismatch

    out = []
    for name in sorted(set(ra) | set(rb)):
        fa, fb = ra.get(name), rb.get(name)
        if name not in ra or name not in rb:
            out.append(
                f"{name} ran in only one run ({label_a}: {describe(fa) if name in ra else 'absent'}; "
                f"{label_b}: {describe(fb) if name in rb else 'absent'})"
            )
            continue
        why = mismatch(fa, fb)
        if why:
            out.append(
                f"{name} results not shown equal, {why} "
                f"({label_a}: {describe(fa)}; {label_b}: {describe(fb)})"
            )
    return out


def fingerprint_differences(
    a: Mapping[str, Any], b: Mapping[str, Any], label_a: str = "A", label_b: str = "B"
) -> list[str]:
    return diff_fingerprints(result_fingerprints(a), result_fingerprints(b), label_a, label_b)


def refusals(
    record_a: Mapping[str, Any] | None,
    record_b: Mapping[str, Any] | None,
    label_a: str = "A",
    label_b: str = "B",
) -> tuple[list[str], list[str], list[str]]:
    """Why metrics.json dicts *record_a* and *record_b* cannot be compared on
    performance: (provenance refusals, result refusals, notes).

    Provenance refusals mean the runs are different experiments. Result
    refusals mean the same experiment returned different query results.
    Notes are caveats that do not refuse (results that were not checked).
    Execution conditions are not refusals: see ``like_for_like``.
    """
    ea, eb = experiment_of(record_a), experiment_of(record_b)
    prov: list[str] = []
    for label, rec, exp in ((label_a, record_a, ea), (label_b, record_b, eb)):
        if exp is None:
            rid = (rec or {}).get("run_id") or "unknown run"
            prov.append(f"{NO_PROVENANCE} ({label}: run {rid} has no experiment block)")
    if prov:
        return prov, [], []
    assert ea is not None and eb is not None
    prov = identity_differences(ea, eb)
    for label, exp in ((label_a, ea), (label_b, eb)):
        prov.extend(f"{label}: {p}" for p in corpus_problems(exp))
    notes: list[str] = []
    res_a, res_b = ea.get("results") or {}, eb.get("results") or {}
    unchecked = res_a.get("not_checked") or res_b.get("not_checked")
    if unchecked:
        notes.append(f"result equivalence not checked: {unchecked}")
        return prov, [], notes
    qa, qb = res_a.get("query_set_id"), res_b.get("query_set_id")
    results: list[str] = []
    if qa != qb:
        results.append(f"benchmark query sets differ ({qa} vs {qb})")
    results.extend(fingerprint_differences(ea, eb, label_a, label_b))
    return prov, results, notes


def like_for_like(
    record_a: Mapping[str, Any] | None, record_b: Mapping[str, Any] | None
) -> list[str]:
    """Execution conditions that differ between two metrics.json dicts
    (effective maintenance, query access path). A comparable pair with any
    is comparable but not like-for-like (DESIGN 6.5). Empty when either has
    no experiment block (that pair is refused before this matters)."""
    ea, eb = experiment_of(record_a), experiment_of(record_b)
    if ea is None or eb is None:
        return []
    return condition_differences(ea, eb)


def support_of(record: Mapping[str, Any] | None) -> str:
    """The recorded support state of a metrics.json dict, or "unknown"."""
    exp = experiment_of(record)
    return str(((exp or {}).get("support") or {}).get("state") or "unknown")


def stored_identity_refusals(
    expected_identity: Mapping[str, Any] | None,
    expected_fingerprints: Mapping[str, Any] | None,
    actual: Mapping[str, Any] | None,
    what: str,
    failed: Any = (),
) -> list[str]:
    """Refusals for a run (*actual*: its experiment block) checked against a
    stored reference: a perf-gate baseline or a reproduction package, which
    keep only the identity and the result fingerprints. *what* names the
    reference in messages ("baseline", "package"). Every identity field
    counts here, execution conditions included: a reference is only matched
    like-for-like.

    *failed* names queries that failed in the run. The caller already fails
    the run for them (a regression, not a different experiment), so they are
    left out of the result check rather than reported twice. A query that
    succeeded but could not be fingerprinted still refuses."""
    if actual is None:
        return [f"{NO_PROVENANCE} (the run has no experiment block)"]
    if not expected_identity:
        return [f"{NO_PROVENANCE} (the {what} was recorded without an experiment identity)"]
    missing = [k for k in identity(actual) if k not in expected_identity]
    if missing:
        return [
            f"not comparable: the {what} was recorded with an older experiment identity "
            f"(no {', '.join(missing)}); record it again from a current run"
        ]
    reasons = [f"{r} from the {what}" for r in diff_identities(expected_identity, identity(actual))]
    reasons.extend(f"run: {p}" for p in corpus_problems(actual))
    established = results_established(actual)
    if established is not True:
        # Nothing shows the run returned the reference's results.
        reasons.append(f"comparability not established (run: {established})")
        return reasons
    if not expected_fingerprints:
        reasons.append(f"comparability not established (the {what} has no result fingerprints)")
        return reasons
    # Queries that failed in the run, and queries the reference recorded as
    # failed (no fingerprint), have no pair of results to compare.
    skip = set(failed or ()) | {n for n, f in (expected_fingerprints or {}).items() if f is None}
    got = {n: f for n, f in result_fingerprints(actual).items() if n not in skip}
    want = {n: f for n, f in (expected_fingerprints or {}).items() if n not in skip}
    reasons.extend(diff_fingerprints(want, got, what, "run"))
    return reasons


def failed_queries(record: Mapping[str, Any] | None) -> set[str]:
    """Names of the benchmark queries a metrics.json dict records as failed."""
    rec = record or {}
    bench = rec.get("benchmark") or (rec.get("pipeline_benchmark") or {}).get("query_benchmark")
    out = set()
    for q in (bench or {}).get("queries") or []:
        if isinstance(q, Mapping) and not q.get("success", True):
            name = q.get("name") or q.get("query_name")
            if name:
                out.add(str(name))
    return out
