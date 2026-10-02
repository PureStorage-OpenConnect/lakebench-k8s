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

Identity versions. A block is stamped ``exp2``
(``identity_version`` 2) only when every ``V2_REQUIRED_INPUTS`` entry is
present: corpus id v2 (from the generator's markers), the run-start
``identity_version`` and a system identity with at least one observed
part. Otherwise it is ``exp1`` and names what was missing in
``v2_unavailable``. ``identity()``
reads exp1 blocks with the frozen ``_identity_v1`` and exp2 blocks with
``_identity_v2``; a stored block is never rebuilt
(``PipelineMetrics.experiment_block``), so no stored id or digest moves.
Which keys say what (workload, corpus, architecture, system, conditions) is
``metrics/comparability.py``.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping
from typing import Any

from lakebench.metrics import comparability as _cmp

EXPERIMENT_SCHEMA_V1 = "exp1"
EXPERIMENT_SCHEMA_V2 = "exp2"
#: The newest schema; written only when every V2_REQUIRED_INPUTS entry exists.
EXPERIMENT_SCHEMA = EXPERIMENT_SCHEMA_V2
IDENTITY_VERSION = 2

#: What an exp2 block needs, by the name ``v2_unavailable`` lists when absent.
V2_REQUIRED_INPUTS = ("corpus id v2", "identity version", "system identity")

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
    """The Hive the Stackable operator runs: the HiveCluster productVersion
    the template renders (STACKABLE_HIVE_VERSION, never images.hive),
    resolved by the operator with the SDP release to
    oci.stackable.tech/sdp/hive:<v>-stackable<sdp>. Derived from the rendered
    and configured versions, not read from the pod."""
    from lakebench.config.schema import STACKABLE_HIVE_VERSION

    version = STACKABLE_HIVE_VERSION
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
        # date_range_days is no longer a config key (nothing read it); the
        # null it always hashed to stays, so ids recorded before stay put.
        declared = {
            "customer360": {
                **workload.customer360.model_dump(mode="json"),
                "date_range_days": None,
            }
        }
        params_id = _short_hash(declared)
        params = {"customer360": _c360_resolved(cfg)}

    from lakebench.metrics.fingerprint_inputs import is_credential_key, is_location_key
    from lakebench.modules.pipeline_engines.spark.conf_keys import user_spark_overrides

    # Local runs build no Spark job manifest, so the user conf never ran.
    # Values of keys that can hold a credential or name where a deployment
    # lives are not written into the record.
    user_conf = (
        {
            k: (
                "<redacted>"
                if is_credential_key(k)
                or is_location_key(k)
                or "endpoint" in k.lower()
                or ".bucket." in k
                else v
            )
            for k, v in user_spark_overrides(cfg.spark.conf or {}).items()
        }
        if system != "local"
        else {}
    )

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
        # Constant: no longer a config key; see the parameters id above.
        "date_range_days": None,
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
        # Written at run start by 1.7 and later: the run asks for identity
        # v2, which build_experiment stamps only when its inputs exist.
        "identity_version": IDENTITY_VERSION,
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
            # The user's Spark conf over the job defaults, only when it
            # changes something, so a default config's block is unchanged.
            **({"spark_conf_user": user_conf} if user_conf else {}),
        },
        "maintenance_config": {
            "pre_benchmark_maintenance": arch.pipeline.pre_benchmark_maintenance,
            "compaction_enabled": sustained.compaction_enabled,
            # How aggressive the maintenance is, when it runs: an execution
            # condition beside what ran.
            "settings": (
                {
                    "retention_threshold": sustained.retention_threshold,
                    "retention_interval": sustained.effective_retention_interval(),
                    "compaction_interval": sustained.effective_compaction_interval(),
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


def _stages(metrics: Any, pb: Any, query_engine: str | None = None) -> tuple[list[str], list[str]]:
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
        skipped.append(
            "benchmark (skipped: no query engine)"
            if (query_engine or "").lower() == "none"
            else "benchmark (not run)"
        )
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


def _corpus_v2(
    corpus: dict[str, Any],
    problems: list[str],
    inputs: Mapping[str, Any],
    dg: Mapping[str, Any],
    fleet: Any = None,
) -> tuple[dict[str, Any], list[str]]:
    """Add corpus id v2 from the persisted run-end observation and,
    for a series repetition, the inherited block (metrics/corpus_identity).
    ``corpus.id`` (v1) is untouched. A record with neither input (every
    v1.6 record) is returned unchanged."""
    from lakebench.metrics.corpus_identity import corpus_v2_fields

    obs = inputs.get("corpus_observation")
    inherited = inputs.get("inherited_corpus")
    v2 = corpus_v2_fields(
        dict(inputs.get("corpus") or {}),
        obs=obs if isinstance(obs, Mapping) else None,
        inherited=inherited if isinstance(inherited, Mapping) else None,
        model_version=(inputs.get("workload") or {}).get("generator_model_version"),
        fleet_digest=dg.get("digest"),
        fleet=fleet if isinstance(fleet, Mapping) else None,
    )
    if v2 is None:
        return corpus, problems
    if v2.replace is not None:
        # Series case (b): repetition 1's block, verbatim (its problems
        # included); this repetition's observation found no markers.
        block = v2.replace
        return block, list(block.pop("problems", None) or [])
    corpus.update(v2.fields)
    return corpus, [*problems, *v2.problems]


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
    executed, skipped = _stages(
        metrics, pb, ((inputs.get("architecture") or {}).get("query_engine") or {}).get("type")
    )
    limits = dict(inputs.get("config_limits") or {})
    limits["executors"] = _executor_caps(metrics, snapshot, schema)
    limits["autosize_cuts"] = list(getattr(metrics, "autosize_cuts", None) or [])
    if mode == "sustained":
        limits["intake_limit"] = getattr(pb, "intake_limit", None)
        # Rounds behind the continuous QpH median: benchmark iterations are
        # an execution condition (DESIGN 2.4), and a median over 4 rounds
        # does not stand like-for-like against one over 5.
        limits["benchmark_rounds"] = sum(
            1 for r in getattr(pb, "benchmark_rounds", None) or [] if (r.qph or 0) > 0
        )
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
    corpus, corpus_problems = _corpus_v2(
        corpus, corpus_problems, inputs, dg, getattr(metrics, "datagen_fleet", None)
    )
    if corpus_problems:
        corpus["problems"] = corpus_problems
    v17 = inputs.get("identity_version") == IDENTITY_VERSION
    sysid = inputs.get("system_identity")
    sysid = dict(sysid) if isinstance(sysid, Mapping) and sysid.get("parts") else None
    missing: list[str] = []
    if v17:
        if corpus.get("id_v2") is None and not corpus.get("id_v2_unavailable"):
            from lakebench.metrics.corpus_identity import NOT_OBSERVED

            corpus["id_v2"] = None
            corpus["id_v2_unavailable"] = NOT_OBSERVED
        if corpus.get("id_v2") is None:
            missing.append("corpus id v2")
        from lakebench.metrics.system_identity import CLUSTER_PARTS, observed_parts

        if sysid is None or (
            not local and not observed_parts(sysid.get("parts") or {}) & set(CLUSTER_PARTS)
        ):
            # A cluster run's identity counts only with a part read from the
            # cluster itself; a stub from a failed sample, or one built from
            # the config alone, is kept as evidence but is not one. A local
            # run's identity observes no part by design and is complete.
            missing.append("system identity")
        from lakebench.metrics.comparability import access_paths

        arch["access_paths"] = {"pipeline": "catalog", **access_paths({"architecture": arch})}
    else:
        missing.append("identity version")
    exp2 = not missing
    block: dict[str, Any] = {
        "schema": EXPERIMENT_SCHEMA_V2 if exp2 else EXPERIMENT_SCHEMA_V1,
        **({"identity_version": IDENTITY_VERSION} if exp2 else {}),
        **({"v2_unavailable": missing} if v17 and missing else {}),
        **({"system_identity": sysid} if sysid is not None else {}),
        # Allocatable and co-tenant load at run start and end:
        # Observational, never compared.
        **(
            {"observed": copy.deepcopy(dict(inputs["observed"]))}
            if isinstance(inputs.get("observed"), Mapping)
            else {}
        ),
    }
    return {
        **block,
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


def refresh_benchmark(metrics: Any) -> None:
    """Bring a stored block's benchmark half up to date after ``lakebench
    benchmark`` replaced the record's benchmark (``cli/_query.py``).

    A stored block is never rebuilt, so without this its result
    fingerprints, benchmark iterations and mode would still describe the
    benchmark that was replaced. Only those keys, the sample count, the
    "benchmark (not run)" entry of the skipped stages and a
    ``benchmark_source`` note change; schema, corpus, architecture and every
    other key stay as stored. The identity digest moves with the benchmark
    iterations and mode (and on exp2 the query set id), as the record now
    describes a different benchmark. A record with no stored block is left alone
    (its block is built from the record when it is saved). For a continuous
    record the results stay its end-of-run result check, which a later
    benchmark does not change."""
    exp = getattr(metrics, "experiment", None)
    if not isinstance(exp, dict) or not exp.get("schema"):
        return
    bench = metrics.benchmark
    if bench is None:
        return
    mode = exp.get("mode") or "batch"
    if mode != "sustained":
        old = exp.get("results") or {}
        new = _results(metrics, mode)
        # Keys another writer put in results stay; the benchmark's own
        # three are replaced.
        exp["results"] = {
            **{
                k: v
                for k, v in old.items()
                if k not in ("query_set_id", "fingerprints", "not_checked")
            },
            **new,
        }
        stages = exp.get("stages") or {}
        if isinstance(stages.get("skipped"), list):
            stages["skipped"] = [
                x for x in stages["skipped"] if not str(x).startswith("benchmark (")
            ]
    limits = exp.setdefault("limits", {})
    limits["benchmark_iterations"] = getattr(bench, "iterations", None) or limits.get(
        "benchmark_iterations"
    )
    limits["benchmark_mode"] = getattr(bench, "mode", None)
    reps = exp.setdefault("repetitions", {})
    reps["benchmark_samples_per_query"] = _repetitions(metrics).get("benchmark_samples_per_query")
    exp["benchmark_source"] = "lakebench benchmark, after the run (replaced the run's benchmark)"


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
        # The whole composition, so the classifier sees the components and
        # the compaction operation a run of this config will use.
        "architecture": dict(arch),
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
    """The experiment identity of a block as stored: ``_identity_v1`` for an
    exp1 block (and any block without ``identity_version`` 2), else
    ``_identity_v2``. Its digest is ``identity_hash``."""
    from lakebench.metrics.comparability import EXP2, block_generation

    if block_generation(exp) == EXP2:
        return _identity_v2(exp)
    return _identity_v1(exp)


def _identity_v1(exp: Mapping[str, Any]) -> dict[str, Any]:
    """The v1.6 identity, frozen: its output, and so every stored exp1
    digest, never changes (tests/test_stored_records.py pins the 24 stored
    records). New keys go in ``_identity_v2`` or in the read-time
    classification of ``metrics/comparability.py``."""
    w = exp.get("workload") or {}
    c = exp.get("corpus") or {}
    dg = c.get("datagen") or {}
    out = {
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
        "effective maintenance": (exp.get("effective_maintenance") or {}).get("id"),
        "maintenance settings": exp.get("maintenance_settings"),
        "query access path": (exp.get("architecture") or {}).get("query_access_path"),
        "system": exp.get("system"),
        "benchmark iterations": (exp.get("limits") or {}).get("benchmark_iterations"),
        "benchmark mode": (exp.get("limits") or {}).get("benchmark_mode"),
        "Lakebench limits that bound": list((exp.get("limits") or {}).get("bound_kinds") or []),
    }
    if exp.get("mode") == "sustained":
        # Continuous only, so batch identities (and their baselines) keep
        # their keys.
        out["benchmark rounds"] = (exp.get("limits") or {}).get("benchmark_rounds")
    return out


def _identity_v2(exp: Mapping[str, Any]) -> dict[str, Any]:
    """The v2 identity: the v1 keys without the generator image tag
    and the ``cluster``/``local`` string, with corpus id v2, the query set
    id, the system fingerprint, the access paths and the dependency pinset,
    plus each ``OPTIONAL_IDENTITY_KEYS`` key whose value is not its
    default. ``identity version`` names the version, so a stored identity
    dict says which rules wrote it."""
    from lakebench.metrics import comparability as cmp

    w = exp.get("workload") or {}
    c = exp.get("corpus") or {}
    dg = c.get("datagen") or {}
    limits = exp.get("limits") or {}
    _, pinset, _ = cmp.dependency_pinset(exp)
    out: dict[str, Any] = {
        "identity version": IDENTITY_VERSION,
        "workload": w.get("name"),
        "workload version": w.get("version"),
        "generator model version": w.get("generator_model_version"),
        "workload parameters": w.get("parameters_id"),
        "query set id": (exp.get("results") or {}).get("query_set_id"),
        "corpus id v2": c.get("id_v2"),
        "seed": c.get("seed"),
        "corpus role": c.get("corpus_role"),
        "scale": c.get("scale"),
        "mode": exp.get("mode"),
        "generator digest": dg.get("digest"),
        "access paths": cmp.access_paths(exp),
        "dependency pinset": pinset or cmp.PINSET_NOT_RECORDED,
        "system fingerprint": (exp.get("system_identity") or {}).get("fingerprint"),
        "effective maintenance": (exp.get("effective_maintenance") or {}).get("id"),
        "maintenance settings": exp.get("maintenance_settings"),
        "benchmark iterations": limits.get("benchmark_iterations"),
        "benchmark mode": limits.get("benchmark_mode"),
        "Lakebench limits that bound": list(limits.get("bound_kinds") or []),
    }
    if exp.get("mode") == "sustained":
        out["benchmark rounds"] = limits.get("benchmark_rounds")
    out.update(cmp.optional_keys(exp))
    return out


#: Identity keys that are execution conditions (the Conditions group of
#: metrics/comparability.py, its one definition): a difference makes a pair
#: comparable but not like-for-like. ``system`` and ``query access path``
#: moved out in 1.7 (System and Architecture groups); ``compaction
#: operation`` is a read-time key, not an identity() key.
CONDITION_KEYS = frozenset(_cmp.CONDITION_KEYS)

#: Conditions that are also outcomes of the run: the in-stream round count
#: depends on how long each round took, so a slower build fits fewer rounds,
#: and the investigator sessions that ran depend on the cases open.
#: compare reports a difference (not like-for-like); the perf gate and
#: reproduce do not refuse on it, or a regression that costs a round would
#: read as "not comparable" instead of a regression.
OUTCOME_CONDITION_KEYS = _cmp.OUTCOME_CONDITION_KEYS


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
    """The identity digest of a block as stored (``identity`` minus the
    generator digest). For exp1 blocks it is the v1.6 digest unchanged."""
    ident = identity(exp)
    # A digest is evidence when both sides have one; an unresolved digest on
    # one side is not a difference (the image reference still is).
    ident.pop("generator digest", None)
    return _short_hash(ident)


def identity_digest(record_or_block: Mapping[str, Any]) -> str | None:
    """``identity_hash`` of a metrics.json dict's stored block (or of a
    block passed directly); None for a record without one."""
    exp = (
        experiment_of(record_or_block)
        if "experiment" in record_or_block or "run_id" in record_or_block
        else record_or_block
    )
    return identity_hash(exp) if exp else None


def experiment_of(record: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    """The record's experiment block when it is one this Lakebench reads
    (exp1, or exp2 with identity version 2), else None: a block of another
    schema reads as no provenance, as the comparison ladder reads it."""
    exp = (record or {}).get("experiment")
    if not isinstance(exp, Mapping) or _cmp.generation(record) == _cmp.LEGACY:
        return None
    return exp


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


def identity_differences(
    a: Mapping[str, Any],
    b: Mapping[str, Any],
    record_a: Mapping[str, Any] | None = None,
    record_b: Mapping[str, Any] | None = None,
) -> list[str]:
    """Experiment-identity differences: the Workload and Corpus groups
    (metrics/comparability.py). Architecture, System and Conditions
    differences are not identity differences. Blocks of different identity
    versions differ as a whole. *record_a* and *record_b* are the blocks'
    metrics.json dicts, when known, for the read-time derivations."""
    from lakebench.metrics import comparability as cmp

    ca, cb = cmp.classify(a, record_a), cmp.classify(b, record_b)
    if ca.generation != cb.generation:
        return [f"identity version differs ({ca.generation} vs {cb.generation})"]
    return [str(d) for g in (cmp.WORKLOAD, cmp.CORPUS) for d in cmp.diff_group(ca, cb, g)]


def condition_differences(
    a: Mapping[str, Any],
    b: Mapping[str, Any],
    record_a: Mapping[str, Any] | None = None,
    record_b: Mapping[str, Any] | None = None,
) -> list[str]:
    """Execution-condition differences (the Conditions group, with the
    compaction operation): comparable, not like-for-like."""
    from lakebench.metrics import comparability as cmp

    ca, cb = cmp.classify(a, record_a), cmp.classify(b, record_b)
    return [str(d) for d in cmp.diff_group(ca, cb, cmp.CONDITIONS)]


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
    from lakebench.metrics import comparability as cmp

    # Ladder step 0 (identity versions, required keys, a withheld seed).
    step0 = cmp.pair_verdict([record_a or {}], [record_b or {}], label_a, label_b)
    prov = list(step0.reasons) if step0.step == "0" else []
    if _cmp.block_generation(ea) == _cmp.block_generation(eb):
        # (A version mismatch is already the step 0 line.)
        prov += identity_differences(ea, eb, record_a, record_b)
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
    """Why two metrics.json dicts are not like-for-like: the execution
    conditions that differ (effective maintenance, the compaction
    operation and the rest of the Conditions group), preceded by the
    ladder's confounded line (architecture and system both differ) or its
    step 7a line (only the dependency set differs). Empty when either has
    no experiment block (that pair is refused before this matters)."""
    ea, eb = experiment_of(record_a), experiment_of(record_b)
    if ea is None or eb is None:
        return []
    from lakebench.metrics import comparability as cmp

    out = condition_differences(ea, eb, record_a, record_b)
    verdict = cmp.pair_verdict([record_a or {}], [record_b or {}])
    if verdict.verdict == cmp.CONFOUNDED or verdict.step == "7a":
        # Not conditions, but just as fatal to like-for-like (ladder steps
        # 6 and 7a): shown with the conditions until compare reads the
        # ladder itself.
        out = list(verdict.reasons) + out
    return out


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
    like-for-like. The exception is OUTCOME_CONDITION_KEYS (the in-stream
    round count), which the run's own speed decides.

    *failed* names queries that failed in the run. The caller already fails
    the run for them (a regression, not a different experiment), so they are
    left out of the result check rather than reported twice. A query that
    succeeded but could not be fingerprinted still refuses."""
    if actual is None:
        return [f"{NO_PROVENANCE} (the run has no experiment block)"]
    if not expected_identity:
        return [f"{NO_PROVENANCE} (the {what} was recorded without an experiment identity)"]
    full_actual = identity(actual)
    version_refusal = _identity_version_refusal(expected_identity, full_actual, actual, what)
    if version_refusal:
        return [version_refusal]
    unobserved = (
        _unobserved_system(expected_identity.get("system fingerprint"), actual)
        if full_actual.get("identity version") == IDENTITY_VERSION
        else None
    )
    if unobserved:
        return [f"not comparable: {unobserved}; the {what} cannot be matched to a system"]
    actual_identity = {k: v for k, v in full_actual.items() if k not in OUTCOME_CONDITION_KEYS}
    # An optional key (set only when non-default) absent from the reference
    # is a difference in that key, not an older identity.
    missing = [
        k
        for k in actual_identity
        if k not in expected_identity and k not in _cmp.OPTIONAL_IDENTITY_KEYS
    ]
    if missing:
        return [
            f"not comparable: the {what} was recorded with an older experiment identity "
            f"(no {', '.join(missing)}); record it again from a current run"
        ]
    expected = {k: v for k, v in expected_identity.items() if k not in OUTCOME_CONDITION_KEYS}
    reasons = [f"{r} from the {what}" for r in diff_identities(expected, actual_identity)]
    # The count itself may differ, but not the estimator: with no in-stream
    # round composite_qph is the post-stream benchmark (streams stopped),
    # which must not stand against an in-stream median, or a regression that
    # empties every round would read as a pass.
    r_ref, r_run = (
        expected_identity.get("benchmark rounds"),
        identity(actual).get("benchmark rounds"),
    )
    if r_ref is not None and r_run is not None and (r_ref > 0) != (r_run > 0):
        reasons.append(
            f"continuous QpH estimator differs: the {what}'s is a median of {r_ref} in-stream "
            f"round(s), the run's of {r_run} (0 means the post-stream benchmark)"
        )
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


def _unobserved_system(expected_fingerprint: Any, actual: Mapping[str, Any]) -> str | None:
    """Why a v2 reference or run names no observed system, or None. The
    fingerprint of an identity with no observed part is one constant per
    system type, so two such runs on different clusters would match."""
    from lakebench.metrics.system_identity import PARTS, fingerprint_of, observed_parts

    sysid = actual.get("system_identity")
    if isinstance(sysid, Mapping) and not observed_parts(sysid.get("parts") or {}):
        return "the run observed no part of its system"
    blank = {
        fingerprint_of({p: {"not_observed": ""} for p in PARTS}, system_type=t)
        for t in ("cluster", "local")
    }
    if expected_fingerprint in blank:
        return "the reference observed no part of its system"
    return None


def _identity_version_refusal(
    expected: Mapping[str, Any], actual_identity: Mapping[str, Any], actual: Any, what: str
) -> str | None:
    """One refusal naming both identity versions when a stored reference
    and a run were recorded under different ones (L8), else None. A v1
    reference against a v2 run is refused, and so is the reverse: the
    workload version bumps of 1.7 make them different experiments anyway."""
    want = expected.get("identity version", 1)
    got = actual_identity.get("identity version", 1)
    if want == got:
        return None
    if want < got:
        return (
            f"not comparable: the {what} was recorded with experiment identity v{want} and "
            f"this run is v{got}; record the {what} again from a current run"
        )
    missing = list((actual or {}).get("v2_unavailable") or [])
    why = (
        "its corpus has no generator marker; re-run it on the current datagen image"
        if "corpus id v2" in missing
        else (
            f"it recorded no {', '.join(missing)}; re-run it with the current Lakebench"
            if missing
            else "re-run it with the current Lakebench"
        )
    )
    return (
        f"not comparable: the {what} was recorded with experiment identity v{want} and "
        f"this run is v{got} ({why})"
    )


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
