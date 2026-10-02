"""CFG-9 (CC-17): settings nothing read are removed; ids and prefixes stay put.

Each removed key, at a value other than its old default, is refused with fix
text by the commands that change data and dropped with a note by teardown
and read commands. At its old default (v1.6 wrote every field) it changed
nothing and is dropped with a note everywhere. Corpus and workload ids, the
datagen prefix and the prefix the frozen financial scripts read do not move.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from lakebench.config import LoadPurpose, load_config
from lakebench.config.loader import ConfigValidationError, load_notes
from tests.conftest import make_config

FIXTURES = Path(__file__).resolve().parent / "fixtures"

_S3 = {
    "endpoint": "http://minio:9000",
    "access_key": "${LAKEBENCH_S3_ACCESS_KEY:-a}",
    "secret_key": "${LAKEBENCH_S3_SECRET_KEY:-b}",
}


def _merge(dst: dict, src: dict) -> dict:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _merge(dst[k], v)
        else:
            dst[k] = v
    return dst


def _write(tmp_path: Path, extra: dict) -> Path:
    data = _merge({"name": "cfg9", "platform": {"storage": {"s3": dict(_S3)}}}, extra)
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


# (extra config, the key named in the refusal, a phrase of its fix text)
REFUSED = [
    ({"images": {"hive": "apache/hive:4.0.1"}}, "'hive' was removed", "Hive 3.1.3"),
    ({"images": {"prometheus": "prom/prometheus:v3"}}, "'prometheus' was removed", "chart"),
    ({"images": {"grafana": "grafana/grafana:11"}}, "'grafana' was removed", "chart"),
    (
        {"platform": {"storage": {"s3": {"secret_ref": "my-secret"}}}},
        "'secret_ref' was removed",
        "access_key and secret_key",
    ),
    (
        {"architecture": {"catalog": {"hive": {"thrift": {"max_threads": 80}}}}},
        "'thrift' was removed",
        "HiveCluster template",
    ),
    (
        {"architecture": {"catalog": {"polaris": {"version": "1.5.0"}}}},
        "'version' was removed",
        "images.polaris",
    ),
    (
        {"architecture": {"catalog": {"unity": {"version": "0.3.0"}}}},
        "'version' was removed",
        "images.unity",
    ),
    (
        {"architecture": {"table_format": {"iceberg": {"file_format": "orc"}}}},
        "'file_format' was removed",
        "Parquet",
    ),
    (
        {"architecture": {"table_format": {"iceberg": {"properties": {"a": "b"}}}}},
        "'properties' was removed",
        "no table property",
    ),
    (
        {"architecture": {"table_format": {"delta": {"properties": {"a": "b"}}}}},
        "'properties' was removed",
        "no table property",
    ),
    (
        {"architecture": {"pipeline": {"medallion": {"bronze": {"path_template": "raw/x"}}}}},
        "'medallion' was removed",
        "a custom bronze layout is not supported",
    ),
    (
        {"architecture": {"pipeline": {"medallion": {"silver": {"partition_by": ["x"]}}}}},
        "'medallion' was removed",
        "fixed bronze layout",
    ),
    (
        {"workload": {"customer360": {"date_range_days": 90}}},
        "'date_range_days' was removed",
        "timestamp_start",
    ),
    ({"observability": {"reports": {"format": "json"}}}, "'reports' was removed", "report.html"),
    ({"observability": {"storage_class": "fast"}}, "'storage_class' was removed", "default"),
    (
        {"observability": {"prometheus_stack_enabled": False}},
        "'prometheus_stack_enabled' was removed",
        "full stack",
    ),
    (
        {"observability": {"s3_metrics_enabled": False}},
        "'s3_metrics_enabled' was removed",
        "not gated on it",
    ),
    (
        {"observability": {"spark_metrics_enabled": False}},
        "'spark_metrics_enabled' was removed",
        "not gated on it",
    ),
    ({"version": 2}, "'version' was removed", "one config schema"),
    ({"secret_ref": "my-secret"}, "'secret_ref' was removed", "access_key and secret_key"),
    # v1.6 mapped financial to pacs008 only on the exact C360 default; with a
    # trailing slash its data went under customer/interactions/.
    (
        {
            "workload": {"schema": "financial", "datagen": {"seed": 43}},
            "architecture": {
                "pipeline": {"medallion": {"bronze": {"path_template": "customer/interactions/"}}}
            },
        },
        "'medallion' was removed",
        "fixed bronze layout",
    ),
    (
        {
            "architecture": {
                "pipeline": {"medallion": {"bronze": {"path_template": "/customer/interactions"}}}
            }
        },
        "'medallion' was removed",
        "fixed bronze layout",
    ),
]


@pytest.mark.parametrize(
    ("extra", "key", "fix"), REFUSED, ids=[r[1] + str(i) for i, r in enumerate(REFUSED)]
)
@pytest.mark.parametrize("purpose", [LoadPurpose.RUN, LoadPurpose.MUTATE])
def test_removed_key_refused_with_fix_text(tmp_path, extra, key, fix, purpose):
    path = _write(tmp_path, extra)
    with pytest.raises(ConfigValidationError) as e:
        load_config(path, purpose=purpose, print_notes=False)
    assert key in str(e.value) and fix in str(e.value)


@pytest.mark.parametrize(
    ("extra", "key", "fix"), REFUSED, ids=[r[1] + str(i) for i, r in enumerate(REFUSED)]
)
@pytest.mark.parametrize("purpose", [LoadPurpose.TEARDOWN, LoadPurpose.READ])
def test_removed_key_loads_for_teardown(tmp_path, extra, key, fix, purpose):
    cfg = load_config(_write(tmp_path, extra), purpose=purpose, print_notes=False)
    name = key.split("'")[1]
    assert any(f"'{name}' (" in t and "no longer used" in t for t in load_notes(cfg).texts())


# (extra config at the old default, the key named in the note)
AT_DEFAULT = [
    ({"images": {"hive": "apache/hive:3.1.3"}}, "hive"),
    ({"images": {"prometheus": "prom/prometheus:v2.48.0"}}, "prometheus"),
    ({"platform": {"storage": {"s3": {"secret_ref": ""}}}}, "secret_ref"),
    (
        {
            "architecture": {
                "catalog": {
                    "hive": {
                        "thrift": {"min_threads": 10, "max_threads": 50, "client_timeout": "300s"}
                    }
                }
            }
        },
        "thrift",
    ),
    ({"architecture": {"catalog": {"polaris": {"version": "1.6.0"}}}}, "version"),
    ({"architecture": {"table_format": {"iceberg": {"file_format": "parquet"}}}}, "file_format"),
    ({"architecture": {"table_format": {"delta": {"properties": {}}}}}, "properties"),
    (
        {"architecture": {"pipeline": {"medallion": {"bronze": {"format": "parquet"}}}}},
        "medallion",
    ),
    (
        {
            "architecture": {
                "pipeline": {"medallion": {"bronze": {"path_template": "customer/interactions/"}}}
            }
        },
        "medallion",
    ),
    ({"workload": {"customer360": {"date_range_days": None}}}, "date_range_days"),
    ({"observability": {"s3_metrics_enabled": True}}, "s3_metrics_enabled"),
    ({"observability": {"reports": {"enabled": True}}}, "reports"),
    ({"version": 1}, "version"),
    ({"description": "anything at all"}, "description"),
    ({"secret_ref": ""}, "secret_ref"),
    ({"architecture": {"catalog": {"hive": {"thrift": None}}}}, "thrift"),
    ({"observability": {"reports": None}}, "reports"),
    ({"platform": {"compute": {"spark": {"executor": {"instances": 8}}}}}, "executor"),
    ({"platform": {"storage": {"scratch": {"size": "100Gi"}}}}, "size"),
]


@pytest.mark.parametrize(
    ("extra", "key"), AT_DEFAULT, ids=[r[1] + str(i) for i, r in enumerate(AT_DEFAULT)]
)
def test_removed_key_at_its_old_default_loads_with_a_note(tmp_path, extra, key):
    cfg = load_config(_write(tmp_path, extra), purpose=LoadPurpose.RUN, print_notes=False)
    notes = load_notes(cfg).texts()
    assert any(f"'{key}' (" in t and "old default" in t for t in notes), notes


def test_financial_medallion_naming_pacs008_loads():
    # v1.6 told AML users to write path_template: pacs008 (the live ledger
    # configs and the examples did); it named the layout that is now fixed.
    data = {
        "recipe": "polaris-iceberg-spark-trino",
        "workload": {"schema": "financial", "datagen": {"seed": 43}},
        "architecture": {
            "catalog": {"polaris": {"client_secret": "x"}},
            "pipeline": {
                "medallion": {"bronze": {"format": "parquet", "path_template": "pacs008"}}
            },
        },
    }
    from lakebench.config.schema import LakebenchConfig

    cfg = LakebenchConfig.model_validate(
        _merge({"name": "aml", "platform": {"storage": {"s3": dict(_S3)}}}, data),
        context={"purpose": LoadPurpose.RUN},
    )
    assert cfg.workload.schema_type.value == "financial"


def test_c360_medallion_naming_pacs008_is_refused(tmp_path):
    # For Customer 360, pacs008 was a real (if unread by Spark) layout change.
    path = _write(
        tmp_path,
        {"architecture": {"pipeline": {"medallion": {"bronze": {"path_template": "pacs008"}}}}},
    )
    with pytest.raises(ConfigValidationError, match="'medallion' was removed"):
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)


def test_v16_saved_config_deploys_and_names_its_streams(tmp_path, monkeypatch):
    # A v1.6 save_config output carries every removed key at its default: it
    # loads for deploy with notes. run refuses only the explicit streams: 4,
    # which run never honoured.
    monkeypatch.setenv("LAKEBENCH_S3_ACCESS_KEY", "a")
    monkeypatch.setenv("LAKEBENCH_S3_SECRET_KEY", "b")
    path = tmp_path / "v16.yaml"
    path.write_text((FIXTURES / "v16-saved-c360.yaml").read_text())
    cfg = load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False)
    removed = {t.split(")")[0] + ")" for t in load_notes(cfg).texts() if "no longer used" in t}
    assert removed == {
        "'description' (LakebenchConfig)",
        "'version' (LakebenchConfig)",
        "'hive' (ImagesConfig)",
        "'prometheus' (ImagesConfig)",
        "'grafana' (ImagesConfig)",
        "'secret_ref' (S3Config)",
        "'size' (ScratchStorageConfig)",
        "'driver' (SparkComputeConfig)",
        "'executor' (SparkComputeConfig)",
        "'thrift' (HiveConfig)",
        "'version' (PolarisConfig)",
        "'version' (UnityConfig)",
        "'file_format' (IcebergConfig)",
        "'properties' (IcebergConfig)",
        "'properties' (DeltaConfig)",
        "'medallion' (ProcessingConfig)",
        "'date_range_days' (Customer360Config)",
        "'reports' (ObservabilityConfig)",
        "'storage_class' (ObservabilityConfig)",
        "'prometheus_stack_enabled' (ObservabilityConfig)",
        "'s3_metrics_enabled' (ObservabilityConfig)",
        "'spark_metrics_enabled' (ObservabilityConfig)",
    }, removed
    with pytest.raises(ConfigValidationError) as e:
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
    assert "benchmark.streams 4" in str(e.value)
    assert "was removed" not in str(e.value)


# -- ids do not move -----------------------------------------------------------


def _short_hash(obj: object) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


_IMAGE = {"datagen": "docker.io/sillidata/lb-datagen:1.6.0"}
# Golden ids computed by integrate 9afa879 (before the removals) for these
# configs; the parameters ids are also derived by hand from the v1.6 dump.
ID_CASES = {
    "c360-default": (
        {"images": _IMAGE, "architecture": {"workload": {"datagen": {"seed": 43}}}},
        {"corpus_id": "da755873809f8fe3", "parameters_id": "12bee0ff88813f11"},
    ),
    "c360-customers-window": (
        {
            "images": _IMAGE,
            "architecture": {
                "workload": {
                    "datagen": {
                        "seed": 43,
                        "timestamp_start": "2024-03-01",
                        "timestamp_end": "2024-09-01",
                    },
                    "customer360": {"unique_customers": 5000},
                }
            },
        },
        {"corpus_id": "aba967e9535e8b9e", "parameters_id": "f820e455d0d687cb"},
    ),
    "aml-seed43": (
        {
            "images": _IMAGE,
            "recipe": "polaris-iceberg-spark-trino",
            "architecture": {"workload": {"schema": "financial", "datagen": {"seed": 43}}},
        },
        {"corpus_id": "e15c7cecee0df916", "parameters_id": "ace33c3eb57403c9"},
    ),
    "c360-scale5-dirty": (
        {
            "images": _IMAGE,
            "architecture": {
                "workload": {"datagen": {"seed": 43, "scale": 5, "dirty_data_ratio": 0.2}}
            },
        },
        {"corpus_id": "7003cd6633e0217e", "parameters_id": "12bee0ff88813f11"},
    ),
}


def test_v16_saved_config_ids_pinned(monkeypatch):
    # The v1.6 saved config carries every removed key; its ids are the ones
    # 9afa879 computed for it.
    from lakebench.metrics.experiment import experiment_inputs

    monkeypatch.setenv("LAKEBENCH_S3_ACCESS_KEY", "a")
    monkeypatch.setenv("LAKEBENCH_S3_SECRET_KEY", "b")
    cfg = load_config(FIXTURES / "v16-saved-c360.yaml", purpose=LoadPurpose.READ, print_notes=False)
    ei = experiment_inputs(cfg, run_mode="batch")
    assert (ei["corpus"]["id"], ei["workload"]["parameters_id"]) == (
        "12c4521ea33cba73",
        "12bee0ff88813f11",
    )


@pytest.mark.parametrize("case", sorted(ID_CASES))
def test_corpus_id_pinned(case):
    import warnings

    from lakebench.metrics.experiment import experiment_inputs

    overrides, want = ID_CASES[case]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        ei = experiment_inputs(make_config(**overrides), run_mode="batch")
    got = {"corpus_id": ei["corpus"]["id"], "parameters_id": ei["workload"]["parameters_id"]}
    assert got == want


def test_c360_parameters_id_keeps_the_v16_shape():
    # v1.6 hashed customer360.model_dump(), which carried date_range_days.
    assert (
        _short_hash({"customer360": {"unique_customers": None, "date_range_days": None}})
        == (ID_CASES["c360-default"][1]["parameters_id"])
    )
    assert (
        _short_hash({"customer360": {"unique_customers": 5000, "date_range_days": None}})
        == (ID_CASES["c360-customers-window"][1]["parameters_id"])
    )


# -- the bronze prefix does not move -------------------------------------------


def _prefixes(schema: str, mode: str) -> tuple[list[str], dict[str, str | None]]:
    """(datagen Job container args, {job type: LB_FINANCIAL_BRONZE_PREFIX})."""
    from lakebench.deploy.datagen import DatagenDeployer
    from lakebench.deploy.engine import DeploymentEngine
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    cfg = make_config(
        name="pfx-probe",
        images=_IMAGE,
        recipe=(
            "polaris-iceberg-spark-trino" if schema == "financial" else "hive-iceberg-spark-trino"
        ),
        architecture={
            "workload": {"schema": schema, "datagen": {"seed": 43}},
            "pipeline": {"mode": mode},
        },
    )
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    k8s.namespace_exists.return_value = True
    with patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False):
        engine = DeploymentEngine(config=cfg, k8s_client=k8s, dry_run=True)
    ctx = DatagenDeployer(engine)._build_datagen_context()
    docs = [d for d in yaml.safe_load_all(engine.renderer.render("datagen/job.yaml.j2", ctx)) if d]
    job = next(d for d in docs if d.get("kind") == "Job")
    args = job["spec"]["template"]["spec"]["containers"][0]["args"]
    manager = SparkJobManager(cfg, k8s)
    env: dict[str, str | None] = {}
    with (
        patch(
            "lakebench.modules.pipeline_engines.spark.job._read_silver_state_epoch",
            return_value=0,
        ),
        patch.object(SparkJobManager, "_read_bronze_data_clock", return_value=None),
    ):
        for jt in JobType:
            values = [
                e["value"]
                for e in manager._build_env_vars(jt)
                if e.get("name") == "LB_FINANCIAL_BRONZE_PREFIX"
            ]
            env[jt.value] = values[0] if values else None
    return args, env


# The datagen Job container args 9afa879 rendered for these configs
# (seed 43, scale 10, image 1.6.0 pinned in _prefixes); batch and continuous
# render the same args.
_ARGS_9AFA879 = {
    "customer360": [
        "--schema", "customer360", "--target-tb", "0.097656", "--customer-id-max", "1000000",
        "--mode", "all", "--delivery-mode", "continuous", "--file-size-mb", "64",
        "--bucket", "pfx-probe-bronze", "--prefix", "customer/interactions", "--seed", "43",
        "--dirty-ratio", "0.08", "--total-nodes", "4", "--workers", "0",
    ],
    "financial": [
        "--schema", "financial", "--scale", "10.000000", "--target-tb", "0.082031",
        "--mode", "all", "--delivery-mode", "continuous", "--file-size-mb", "64",
        "--bucket", "pfx-probe-bronze", "--prefix", "pacs008", "--seed", "43",
        "--dirty-ratio", "0.08", "--total-nodes", "4", "--workers", "0",
    ],
}  # fmt: skip


@pytest.mark.parametrize("mode", ["batch", "continuous"])
def test_bronze_prefix_args_unchanged(mode):
    """Pinned against integrate 9afa879: the datagen Job's --prefix and the
    LB_FINANCIAL_BRONZE_PREFIX every financial Spark job gets (read by the
    frozen bronze_verify_financial.py, bronze_ingest_financial.py,
    score_financial_reference.py and score_financial.py)."""
    args, env = _prefixes("customer360", mode)
    assert args == _ARGS_9AFA879["customer360"]
    assert set(env.values()) == {None}
    args, env = _prefixes("financial", mode)
    assert args == _ARGS_9AFA879["financial"]
    assert args[args.index("--prefix") + 1] == "pacs008"
    assert env and all(v == "pacs008/" for v in env.values()), env
