"""Perf-gate fingerprint version 2 and baseline store schema 2 (CC-11).

A changed config, job profile, Spark conf or dependency set must never
compare like for like, and records from before version 2 are refused by name.
Each test fails with its fix reverted; the d3 design names most of them.
"""

from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from lakebench.config.schema import ImagesConfig
from lakebench.metrics import perf_gate as pg
from lakebench.metrics.collector import build_config_snapshot
from lakebench.metrics.fingerprint_inputs import (
    FINGERPRINT_VERSION,
    fingerprint_inputs,
    is_credential_key,
    owned_conf,
)
from lakebench.modules.pipeline_engines.spark import job as job_mod
from tests import test_perf_gate as _tpg
from tests.conftest import make_config
from tests.test_perf_gate import (
    PERF,
    PINSET,
    _batch_run,
    _record,
    _snapshot,
)

# The perf-gate tests' store-and-runs fixture.
env = _tpg.env

ROOT = Path(__file__).resolve().parents[1]
STORED_V16_BATCH = ROOT / "tests/fixtures/records/run-20260929-212900-5105a0/metrics.json"


def _v2_run(env, run_id: str = "20260925-100000-bbbbbb", **over) -> pg.RunRecord:
    return pg.load_run(env.write_run(_batch_run(env.snaps["c360-batch-s10"], run_id, **over)))


def _baseline(env) -> None:
    _record(
        env, "c360-batch-s10", _batch_run(env.snaps["c360-batch-s10"], "20260924-100000-aaaaaa")
    )


# -- the run side is read, never rebuilt ---------------------------------------


def test_v16_run_snapshot_not_recomputed_as_v2(env):
    _baseline(env)
    raw = json.loads(STORED_V16_BATCH.read_text())
    assert "fingerprint_version" not in raw["config_snapshot"]
    run = pg.load_run(env.write_run(raw))
    c = pg.compare_run(env.store(), "c360-batch-s10", run)
    assert c.verdict == pg.REFUSED
    assert any("run predates fingerprint v2" in r for r in c.reasons), c.reasons
    # Named, not diffed field by field as if it were a version 2 snapshot.
    assert not any("config fingerprint differs" in r for r in c.reasons), c.reasons
    # And a v1.6 record can never become a baseline.
    with pytest.raises(pg.PerfGateError, match="run predates fingerprint v2"):
        pg.record_baseline(env.store(), "c360-batch-s10", run, "abc", replace=True)


def test_v1_snapshot_projection_is_unchanged():
    # A version 1 snapshot keeps its version 1 projection: the driver and
    # executor blocks and scratch.size it recorded, no version 2 keys.
    snap = json.loads(STORED_V16_BATCH.read_text())["config_snapshot"]
    fp = pg.snapshot_fingerprint(snap, "batch")
    assert "fingerprint_inputs" not in fp and "fingerprint_version" not in fp
    assert fp["scratch"]["size"] == snap["scratch"]["size"]
    assert fp["spark"]["executor"] == snap["spark"]["executor"]


# -- baselines from before version 2 ------------------------------------------


def test_v16_baseline_refused_fingerprint_v1(env, tmp_path):
    # The checked-in store is schema 1 with an accepted v1.6 baseline.
    store_dir = tmp_path / "checked-in"
    shutil.copytree(PERF, store_dir)
    store = pg.load_store(store_dir / "baselines.yaml")
    base = store.baselines["c360-batch-s10"]
    assert base.accepted and base.fingerprint_version == 1
    for raw in (
        _batch_run(_snapshot(store_dir / "c360-batch-s10.yaml"), "20260930-100000-cccccc"),
        json.loads(STORED_V16_BATCH.read_text()),
    ):
        run = pg.load_run(env.write_run(raw))
        c = pg.compare_run(store, "c360-batch-s10", run)
        assert c.verdict == pg.REFUSED
        named = [r for r in c.reasons if "baseline predates fingerprint v2" in r]
        assert named and "re-record" in named[0] and "record --replace" in named[0], c.reasons


def test_compare_run_refuses_v1_store_entry(env):
    # A schema 1 entry whose fingerprint_hash happens to equal the pinned
    # version 2 hash (copied by hand, say) is still refused by its version:
    # the version, not a hash mismatch, carries the refusal.
    _baseline(env)
    store = env.store()
    pinned_hash = store.pinned("c360-batch-s10").fingerprint_hash
    data = yaml.safe_load(env.store_path.read_text())
    data["schema_version"] = 1
    entry = data["baselines"]["c360-batch-s10"]
    entry.pop("fingerprint_version")
    entry.pop("pinset_sha256")
    entry["fingerprint_hash"] = pinned_hash
    env.store_path.write_text(yaml.safe_dump(data, sort_keys=False))

    store = env.store()
    assert store.baselines["c360-batch-s10"].fingerprint_version == 1
    c = pg.compare_run(store, "c360-batch-s10", _v2_run(env))
    assert c.verdict == pg.REFUSED
    assert any("baseline predates fingerprint v2" in r for r in c.reasons), c.reasons


# -- store schema 2 -----------------------------------------------------------


def test_store_schema_2_roundtrip(env):
    _baseline(env)
    text = env.store_path.read_text()
    assert yaml.safe_load(text)["schema_version"] == 2
    base = env.store().baselines["c360-batch-s10"]
    assert base.fingerprint_version == FINGERPRINT_VERSION
    assert base.pinset_sha256 == PINSET


def test_v1_entry_survives_a_schema_2_save(tmp_path):
    # A schema 1 store with an accepted entry is saved by the next record of
    # another config; the v1 entry stays version 1, the store still loads,
    # and compare refuses that entry by its version.
    store_dir = tmp_path / "checked-in"
    shutil.copytree(PERF, store_dir)
    store = pg.load_store(store_dir / "baselines.yaml")
    store.save()
    data = yaml.safe_load((store_dir / "baselines.yaml").read_text())
    assert data["schema_version"] == 2
    assert data["baselines"]["c360-batch-s10"]["fingerprint_version"] == 1
    assert "pinset_sha256" not in data["baselines"]["c360-batch-s10"]
    again = pg.load_store(store_dir / "baselines.yaml")
    assert again.baselines["c360-batch-s10"].fingerprint_version == 1


def test_schema_2_accepted_v2_entry_needs_a_pinset(env):
    _baseline(env)
    data = yaml.safe_load(env.store_path.read_text())
    del data["baselines"]["c360-batch-s10"]["pinset_sha256"]
    env.store_path.write_text(yaml.safe_dump(data, sort_keys=False))
    with pytest.raises(pg.PerfGateError, match="lacks pinset_sha256"):
        env.store()


@pytest.mark.parametrize("schema", [0, 3, True, None])
def test_unknown_store_schema_refused(tmp_path, schema):
    p = tmp_path / "baselines.yaml"
    p.write_text(yaml.safe_dump({"schema_version": schema, "baselines": {"x": {"config": "x"}}}))
    with pytest.raises(pg.PerfGateError, match="schema_version"):
        pg.load_store(p)


# -- dependency set ------------------------------------------------------------


def test_pinset_mismatch_refused(env):
    _baseline(env)
    other = "b2" * 32
    c = pg.compare_run(env.store(), "c360-batch-s10", _v2_run(env, pinset=other))
    assert c.verdict == pg.REFUSED
    assert f"dependency set differs from the baseline ({other[:12]} vs {PINSET[:12]})" in c.reasons
    # Everything else is equal: the pinset alone refuses it.
    assert len(c.reasons) == 1, c.reasons
    same = pg.compare_run(env.store(), "c360-batch-s10", _v2_run(env, "20260925-110000-dddddd"))
    assert same.verdict == pg.PASS, same.reasons


def test_run_without_pinset_refused_and_not_recordable(env):
    _baseline(env)
    run = _v2_run(env, pinset=None)
    c = pg.compare_run(env.store(), "c360-batch-s10", run)
    assert any("none recorded vs" in r for r in c.reasons), c.reasons
    with pytest.raises(pg.PerfGateError, match="records no dependency set"):
        pg.record_baseline(env.store(), "c360-batch-s10", run, "abc", replace=True)


# -- what version 2 hashes -----------------------------------------------------


def _pinned_hash(env) -> str:
    return pg.load_pinned(env.store_dir / "c360-batch-s10.yaml").fingerprint_hash


def test_profile_change_moves_fingerprint(env, monkeypatch):
    before = _pinned_hash(env)
    patched = copy.deepcopy(job_mod._JOB_PROFILES)
    patched["silver-build"]["executor_memory"] = "40g"
    monkeypatch.setattr(job_mod, "_JOB_PROFILES", patched)
    assert _pinned_hash(env) != before


def test_owned_conf_change_moves_fingerprint(env, monkeypatch):
    before = _pinned_hash(env)
    real = job_mod.SparkJobManager._build_manifest

    def patched(self, *a, **k):
        m = real(self, *a, **k)
        m["spec"]["sparkConf"]["spark.hadoop.fs.s3a.connection.maximum"] = "100"
        return m

    monkeypatch.setattr(job_mod.SparkJobManager, "_build_manifest", patched)
    assert _pinned_hash(env) != before


def test_location_and_credentials_do_not_move_fingerprint():
    a = make_config(name="fp-a")
    b = make_config(
        name="fp-b-other-namespace",
        platform={
            "storage": {
                "s3": {
                    "endpoint": "https://elsewhere:443",
                    "access_key": "other-key",
                    "secret_key": "other-secret",
                    "buckets": {"bronze": "x-b", "silver": "x-s", "gold": "x-g"},
                }
            }
        },
    )
    fa = pg.snapshot_fingerprint(build_config_snapshot(a), "batch")
    fb = pg.snapshot_fingerprint(build_config_snapshot(b), "batch")
    assert fa == fb
    assert pg.fingerprint_hash(fa) == pg.fingerprint_hash(fb)


@pytest.mark.parametrize(
    "recipe",
    [
        "hive-iceberg-spark-trino",
        "hive-iceberg-spark-thrift",
        "hive-iceberg-spark-duckdb",
        "hive-iceberg-spark-none",
        "polaris-iceberg-spark-trino",
        "polaris-iceberg-spark-thrift",
        "polaris-iceberg-spark-duckdb",
        "polaris-iceberg-spark-none",
        "hive-delta-spark-trino",
        "hive-delta-spark-thrift",
        "hive-delta-spark-none",
    ],
)
@pytest.mark.parametrize("schema", ["customer360", "financial"])
@pytest.mark.parametrize("continuous", [False, True])
def test_inputs_carry_no_location_or_secret(recipe, schema, continuous):
    """For every recipe and mode: two deployments that differ only in where
    they live and in their keys have equal inputs, and no key, secret,
    namespace, endpoint or bucket appears in them."""
    if schema == "financial" and "delta" in recipe:
        pytest.skip("the financial workload is Iceberg only")

    def cfg(name: str, endpoint: str, key: str, bucket: str):
        c = make_config(
            name=name,
            recipe=recipe,
            platform={
                "storage": {
                    "s3": {
                        "endpoint": endpoint,
                        "access_key": key,
                        "secret_key": key + "-secret",
                        "buckets": {
                            "bronze": bucket + "-b",
                            "silver": bucket + "-s",
                            "gold": bucket + "-g",
                        },
                    }
                }
            },
            architecture={"workload": {"schema": schema}},
        )
        object.__setattr__(c.architecture.catalog.polaris, "client_secret", key + "-polaris")
        return c

    one = fingerprint_inputs(cfg("nsone", "http://host-one:9000", "KEYONE", "bkone"), continuous)
    two = fingerprint_inputs(cfg("nstwo", "http://host-two:9000", "KEYTWO", "bktwo"), continuous)
    assert "error" not in one, one
    assert one == two
    text = json.dumps(one)
    for needle in ("nsone", "host-one", "KEYONE", "bkone", "polaris-"):
        assert needle not in text, needle


def test_credential_keys_dropped():
    conf = {
        "spark.hadoop.fs.s3a.encryption.key": "k1",
        "spark.hadoop.fs.s3a.server-side-encryption.key": "k2",
        "spark.ssl.keyStorePassword": "k3",
        "spark.sql.catalog.x.oauthToken": "k4",
        "spark.hadoop.fs.s3a.access.key": "k5",
        "spark.hadoop.fs.s3a.secret.key": "k6",
        "spark.authenticate.secret": "k7",
        "spark.sql.shuffle.partitions": "64",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "p",
        "spark.sql.catalog.lakehouse.token-refresh-enabled": "true",
    }
    kept = owned_conf(conf)
    assert not [v for v in kept.values() if v.startswith("k")], kept
    assert kept["spark.sql.shuffle.partitions"] == "64"
    assert not is_credential_key("spark.hadoop.fs.s3a.aws.credentials.provider")


def test_user_conf_is_hashed_never_written():
    # A user conf entry can hold a secret under any key name: it enters the
    # fingerprint as a hash, so a change moves it, and its value is never
    # written to the record.
    secret = "azure-account-key-value-xyz"
    user = {
        "spark.hadoop.fs.azure.account.key.acct.dfs.core.windows.net": secret,
        "spark.speculation": "true",
    }
    one = fingerprint_inputs(make_config(spark={"conf": user}), False)
    assert secret not in json.dumps(one)
    assert "spark.speculation" not in one["owned_conf"]["silver-build"]
    digest = one["user_conf_sha256"]["silver-build"]
    assert digest and len(digest) == 64
    changed = fingerprint_inputs(
        make_config(spark={"conf": {**user, "spark.speculation": "false"}}), False
    )
    assert changed["user_conf_sha256"]["silver-build"] != digest
    # A credential-named key does not enter the digest at all.
    other_key = {**user, "spark.hadoop.fs.azure.account.key.acct.dfs.core.windows.net": "x"}
    same = fingerprint_inputs(make_config(spark={"conf": other_key}), False)
    assert same["user_conf_sha256"]["silver-build"] == digest
    # The default conf is Lakebench's, written in plain text, and no user
    # digest is recorded for it.
    default = fingerprint_inputs(make_config(), False)
    assert default["user_conf_sha256"] == {
        "bronze-verify": None,
        "silver-build": None,
        "gold-finalize": None,
    }


def test_query_engine_and_settle_settings_move_fingerprint():
    base = pg.fingerprint_hash(
        pg.snapshot_fingerprint(build_config_snapshot(make_config()), "batch")
    )
    for over in (
        {"architecture": {"query_engine": {"trino": {"worker": {"spill_enabled": False}}}}},
        {"architecture": {"benchmark": {"maintenance_settle": {"enabled": False}}}},
        {"architecture": {"catalog": {"hive": {"resources": {"memory": "8Gi"}}}}},
    ):
        snap = build_config_snapshot(make_config(**over))
        assert pg.fingerprint_hash(pg.snapshot_fingerprint(snap, "batch")) != base, over
    thrift = make_config(recipe="hive-iceberg-spark-thrift")
    thrift2 = make_config(
        recipe="hive-iceberg-spark-thrift",
        architecture={"query_engine": {"spark_thrift": {"memory": "24g"}}},
    )
    h1 = pg.fingerprint_hash(pg.snapshot_fingerprint(build_config_snapshot(thrift), "batch"))
    h2 = pg.fingerprint_hash(pg.snapshot_fingerprint(build_config_snapshot(thrift2), "batch"))
    assert h1 != h2


def test_offline_conf_equals_the_submitted_conf():
    """The offline build is the conf submit_job sends, for every pipeline job."""
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    for recipe in (
        "hive-iceberg-spark-trino",
        "polaris-iceberg-spark-trino",
        "hive-delta-spark-trino",
    ):
        cfg = make_config(recipe=recipe)
        for continuous in (False, True):
            inputs = fingerprint_inputs(cfg, continuous)
            k8s = MagicMock()
            k8s.get_cluster_capacity.return_value = None
            mgr = SparkJobManager(cfg, k8s)
            sent: dict[str, dict] = {}

            def create(sent=sent, **kw):
                sent[kw["body"]["metadata"]["name"]] = kw["body"]

            api = MagicMock()
            api.create_namespaced_custom_object.side_effect = create
            with (
                patch.object(mgr, "_delete_job"),
                patch.object(mgr, "scripts_changed_since_apply", return_value=None),
                patch.object(mgr, "_build_env_vars", return_value=[]),
                patch("kubernetes.client.CustomObjectsApi", return_value=api),
            ):
                for jt in inputs["owned_conf"]:
                    mgr.submit_job(JobType(jt))
            for jt, conf in inputs["owned_conf"].items():
                body = sent[f"lakebench-{jt}"]
                assert owned_conf(body["spec"]["sparkConf"]) == conf, (recipe, jt)
                ex = body["spec"]["executor"]
                prof = inputs["job_profiles"][jt]
                assert (ex["instances"], ex["memory"], ex["memoryOverhead"], ex["cores"]) == (
                    prof["executor_instances"],
                    prof["executor_memory"],
                    prof["executor_memory_overhead"],
                    prof["executor_cores"],
                ), (recipe, jt)


def test_run_of_another_config_file_refused(env):
    _baseline(env)
    snap = env.snaps["c360-batch-s10"]
    assert snap["config_sha256"] == env.store().pinned("c360-batch-s10").file_sha256
    for value, needle in (("f" * 64, "different config file"), (None, "no config_sha256")):
        raw = _batch_run({**snap, "config_sha256": value}, "20260925-120000-ffffff")
        c = pg.compare_run(env.store(), "c360-batch-s10", pg.load_run(env.write_run(raw)))
        assert c.verdict == pg.REFUSED
        assert any(needle in r for r in c.reasons), c.reasons
        (env.runs / "run-20260925-120000-ffffff" / "metrics.json").unlink()
        (env.runs / "run-20260925-120000-ffffff").rmdir()


def test_run_snapshot_records_its_config_file(tmp_path):
    import hashlib

    from lakebench.config.loader import save_config

    path = tmp_path / "c.yaml"
    save_config(make_config(), path)
    snap = build_config_snapshot(make_config(), config_path=path)
    assert snap["config_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_continuous_pinned_config_must_pin_executor_counts(env):
    path = env.store_dir / "c360-continuous-s10.yaml"
    data = yaml.safe_load(path.read_text())
    del data["platform"]["compute"]["spark"]["silver_stream_executors"]
    path.write_text(yaml.safe_dump(data))
    with pytest.raises(pg.PerfGateError, match="silver_stream_executors"):
        pg.load_pinned(path)


def test_local_run_records_no_manifests():
    snap = build_config_snapshot(make_config(), run_mode="batch", system="local")
    assert snap["fingerprint_inputs"] == {"error": "local run: no Spark job manifests"}


def test_build_failure_is_recorded_and_refused(env, monkeypatch):
    def boom(self, *a, **k):
        raise RuntimeError("no manifest")

    monkeypatch.setattr(job_mod.SparkJobManager, "_build_manifest", boom)
    snap = build_config_snapshot(make_config())
    assert snap["fingerprint_inputs"] == {"error": "RuntimeError: no manifest"}
    with pytest.raises(pg.PerfGateError, match="fingerprint inputs could not be built"):
        pg.load_pinned(env.store_dir / "c360-batch-s10.yaml")
    monkeypatch.undo()
    raw = _batch_run(env.snaps["c360-batch-s10"], "20260925-100000-eeeeee")
    raw["config_snapshot"] = {
        **raw["config_snapshot"],
        "fingerprint_inputs": snap["fingerprint_inputs"],
    }
    reasons = pg.run_refusals(
        pg.load_run(env.write_run(raw)), pg.load_pinned(env.store_dir / "c360-batch-s10.yaml")
    )
    assert any("fingerprint inputs could not be built" in r for r in reasons), reasons


# -- golden --------------------------------------------------------------------

# The version 2 fingerprint of the default Customer 360 batch config (scale
# 10, hive-iceberg-spark-trino, Spark 4.1.1), written by hand from the
# schema defaults, _JOB_PROFILES and the literals in _build_manifest, not
# generated by the code under test (checked by a second agent). The datagen
# and Trino images come from ImagesConfig, so their re-pins need no edit; a
# Spark image change does, because the conf depends on the Spark version.
_GOLDEN_PROFILES = {
    "bronze-verify": {
        "driver_cores": 2,
        "driver_memory": "4g",
        "executor_cores": 2,
        "executor_memory": "4g",
        "executor_memory_overhead": "2g",
        "executor_instances": 4,
        "scratch_size": "50Gi",
    },
    "silver-build": {
        "driver_cores": 4,
        "driver_memory": "32g",
        "executor_cores": 4,
        "executor_memory": "48g",
        "executor_memory_overhead": "12g",
        "executor_instances": 8,
        "scratch_size": "300Gi",
    },
    "gold-finalize": {
        "driver_cores": 4,
        "driver_memory": "32g",
        "executor_cores": 4,
        "executor_memory": "32g",
        "executor_memory_overhead": "8g",
        "executor_instances": 4,
        "scratch_size": "300Gi",
    },
}
_GOLDEN_CONF = {
    # 100 GB of bronze / 2000 tasks is under the 256 MiB floor.
    "spark.sql.files.maxPartitionBytes": "268435456",
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",
    "spark.sql.adaptive.enabled": "true",
    "spark.dynamicAllocation.enabled": "false",
    "spark.hadoop.fs.s3a.connection.maximum": "200",
    "spark.hadoop.fs.s3a.threads.max": "100",
    "spark.hadoop.fs.s3a.fast.upload.buffer": "bytebuffer",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
}
# The whole silver-build owned conf, key by key from _build_manifest for
# hive + iceberg on Spark 4.0 at scale 10 (8 executors x 4 cores, base
# partitions 64), scratch and observability off. The schema's default
# spark.conf seeds it; Lakebench's later literals overwrite the S3A
# connection and thread counts and the partitions. Location keys (S3
# endpoint, catalog uri, warehouse, catalog s3.endpoint, the jar URLs) are
# left out; the jars themselves are the dependency pinset's.
_GOLDEN_SILVER_CONF = {
    "spark.default.parallelism": "64",
    "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:MaxGCPauseMillis=200",
    "spark.driver.maxResultSize": "8g",
    "spark.dynamicAllocation.enabled": "false",
    "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:MaxGCPauseMillis=200",
    "spark.executor.heartbeatInterval": "30s",
    "spark.files.useFetchCache": "false",
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.attempts.maximum": "20",
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    ),
    "spark.hadoop.fs.s3a.block.size": "268435456",
    "spark.hadoop.fs.s3a.connection.maximum": "200",
    "spark.hadoop.fs.s3a.connection.timeout": "60000",
    "spark.hadoop.fs.s3a.endpoint.region": "us-east-1",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.fast.upload.active.blocks": "16",
    "spark.hadoop.fs.s3a.fast.upload.buffer": "bytebuffer",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.max.total.tasks": "200",
    "spark.hadoop.fs.s3a.multipart.size": "268435456",
    "spark.hadoop.fs.s3a.multipart.threshold": "268435456",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.retry.interval": "500ms",
    "spark.hadoop.fs.s3a.retry.limit": "10",
    "spark.hadoop.fs.s3a.threads.max": "100",
    "spark.hadoop.hive.metastore.client.socket.timeout": "300s",
    "spark.kubernetes.driver.service.deleteOnTermination": "true",
    "spark.kubernetes.executor.deleteOnTermination": "true",
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",
    "spark.network.timeout": "600s",
    # SAF-8: the jobs hide credential keys in the Spark UI and event log.
    "spark.redaction.regex": "(?i)secret|password|token|access[.]?key|credential",
    "spark.rpc.askTimeout": "300s",
    "spark.scheduler.listenerbus.eventqueue.appStatus.capacity": "2000",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "268435456",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.lakehouse.hive.metastore-timeout": "5m",
    "spark.sql.catalog.lakehouse.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.lakehouse.io.manifest-encoder-threads": "16",
    "spark.sql.catalog.lakehouse.io.threads": "32",
    "spark.sql.catalog.lakehouse.s3.multipart.size": "268435456",
    "spark.sql.catalog.lakehouse.s3.path-style-access": "true",
    "spark.sql.catalog.lakehouse.type": "hive",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.files.maxPartitionBytes": "268435456",
    "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.parquet.filterPushdown": "true",
    "spark.sql.shuffle.partitions": "64",
    "spark.task.maxFailures": "4",
    "spark.ui.enabled": "false",
    "spark.ui.liveUpdate.minFlushPeriod": "30s",
    "spark.ui.liveUpdate.period": "5s",
    "spark.ui.retainedDeadExecutors": "10",
    "spark.ui.retainedJobs": "100",
    "spark.ui.retainedStages": "100",
    "spark.ui.retainedTasks": "1000",
}
_GOLDEN_PARTITIONS = {"bronze-verify": "32", "silver-build": "64", "gold-finalize": "32"}
_GOLDEN_RESULT_SIZE = {"bronze-verify": "8g", "silver-build": "8g", "gold-finalize": "8g"}


def test_fingerprint_v2_default_golden():
    images = ImagesConfig()
    fp = pg.snapshot_fingerprint(build_config_snapshot(make_config()), "batch")
    inputs = fp.pop("fingerprint_inputs")
    expected = {
        "pipeline_mode": "batch",
        "fingerprint_version": 2,
        "scale": 10,
        "processing_pattern": "medallion",
        "catalog": "hive",
        "table_format": "iceberg",
        "pipeline_engine": "spark",
        "query_engine": "trino",
        "workload_schema": "customer360",
        "spark": {
            "executor_overrides": {
                "bronze": None,
                "silver": None,
                "gold": None,
                "bronze_ingest": None,
                "silver_stream": None,
                "gold_refresh": None,
            }
        },
        "datagen": {"scale": 10, "mode": "auto", "parallelism": 4, "file_size": "64mb"},
        "images": {"datagen": images.datagen, "spark": images.spark, "trino": images.trino},
        "benchmark": {
            "mode": "power",
            "streams": 1,
            "cache": "hot",
            "iterations": 3,
            "maintenance_settle": {
                "enabled": True,
                "max_seconds": 2700,
                "interval_seconds": 60,
                "tolerance_pct": 10,
                "probe_query": None,
                "probe_samples": 1,
            },
        },
        "maintenance": {
            "pre_benchmark_maintenance": True,
            "retention_interval": None,
            "retention_threshold": "30m",
            "compaction_enabled": True,
            "compaction_interval": None,
        },
        "scratch": {
            "enabled": False,
            "storage_class": "px-csi-scratch",
            "size_per_job": {
                "bronze-verify": "50Gi",
                "silver-build": "300Gi",
                "gold-finalize": "300Gi",
            },
        },
        "trino": {
            "coordinator": {"cpu": "2", "memory": "8Gi"},
            "worker": {"replicas": 2, "cpu": "4", "memory": "16Gi"},
        },
    }
    assert images.spark == "apache/spark:4.1.1-python3"  # the conf below assumes 4.1
    assert fp == expected
    assert inputs["job_profiles"] == _GOLDEN_PROFILES
    # Heap = 80% of the pod limit in whole MiB (jvm_heap_for_limit): 8Gi ->
    # 6553m, 16Gi -> 13107m. Per-node query memory 35% and headroom 30% of
    # the heap (trino_memory_properties); cluster max = 2 workers x 4587MB.
    assert inputs["query_engine"].pop("derived") == {
        "coordinator_heap": "6553m",
        "worker_heap": "13107m",
        "memory_properties": {
            "coordinator_max_memory_per_node": "2293MB",
            "coordinator_heap_headroom": "1965MB",
            "worker_max_memory_per_node": "4587MB",
            "worker_heap_headroom": "3932MB",
            "max_memory": "9174MB",
        },
    }
    assert inputs["query_engine"] == {
        "type": "trino",
        "trino": {
            "coordinator": {"cpu": "2", "memory": "8Gi"},
            "worker": {
                "replicas": 2,
                "cpu": "4",
                "memory": "16Gi",
                "spill_enabled": True,
                "spill_max_per_node": "40Gi",
                "storage": "50Gi",
                "storage_class": "",
            },
            "catalog_name": "lakehouse",
        },
    }
    assert inputs["catalog"] == {
        "type": "hive",
        "resources": {"cpu_min": "500m", "cpu_max": "2", "memory": "4Gi"},
    }
    assert inputs["owned_conf"]["silver-build"] == _GOLDEN_SILVER_CONF
    assert inputs["user_conf_sha256"] == dict.fromkeys(_GOLDEN_PROFILES)
    for jt, conf in inputs["owned_conf"].items():
        # The other jobs differ from silver only in their partitions.
        assert set(conf) == set(_GOLDEN_SILVER_CONF), jt
        for key, value in _GOLDEN_CONF.items():
            assert conf[key] == value, (jt, key)
        assert conf["spark.sql.shuffle.partitions"] == _GOLDEN_PARTITIONS[jt]
        assert conf["spark.default.parallelism"] == _GOLDEN_PARTITIONS[jt]
        assert conf["spark.driver.maxResultSize"] == _GOLDEN_RESULT_SIZE[jt]
        for gone in (
            "spark.hadoop.fs.s3a.endpoint",
            "spark.sql.catalog.lakehouse.uri",
            "spark.sql.catalog.lakehouse.warehouse",
            "spark.sql.catalog.lakehouse.s3.endpoint",
        ):
            assert gone not in conf, (jt, gone)
