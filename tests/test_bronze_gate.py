"""SAF-9: stale bronze on buckets the deployment did not create (SD-17, DESIGN ch01 section 5).

One gate, ``bronze_prefix_gate``, decides before any datagen Job whether the
datagen prefix may be written: an owned bucket's prefix is cleared only with
``--regenerate``; an unowned one is never cleared, and is written over only
with ``--allow-stale-bronze``, which the run records. ``DatagenDeployer``'s
cycle-0 path refuses the same way for a caller that skipped the gate.

Also LB-231: destroy clears the kept silver-state's ``bronze_data_clock``
when it empties bronze in a surviving namespace.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lakebench.deploy.ownership import OWNER_MARKER_KEY, TAG_CLUSTER, TAG_DEPLOYMENT_NAME
from lakebench.exit_codes import ExitCode
from tests.fixtures.recording_k8s import K8sRecorder, recording

NS = "u01"
FP = "fp-here"
BRONZE = "u01-bronze"
PREFIX = "customer/interactions"


@pytest.fixture(autouse=True)
def _fingerprint():
    with patch("lakebench.deploy.ownership.api_server_fingerprint", return_value=FP):
        yield


def _cfg():
    from tests.conftest import make_config

    return make_config(name=NS)


def _seed(rec: K8sRecorder, *, owned: bool, objects=(), created_record: bool | None = None):
    cfg = _cfg()
    rec.for_config(cfg)
    anns = {"lakebench.deployment/name": NS}
    if created_record if created_record is not None else owned:
        anns["lakebench.deployment/created-buckets"] = BRONZE
    rec.add_namespace(NS, annotations=anns)
    tags = {TAG_DEPLOYMENT_NAME: NS, TAG_CLUSTER: FP} if owned else None
    rec.add_bucket(BRONZE, list(objects), tags=tags)
    return cfg


def _gate(cfg, **kw):
    from lakebench.deploy.datagen import bronze_prefix_gate

    kw.setdefault("regenerate", False)
    kw.setdefault("allow_stale_bronze", False)
    return bronze_prefix_gate(cfg, **kw)


class TestGate:
    def test_owned_empty_proceeds(self):
        with recording() as rec:
            cfg = _seed(rec, owned=True)
            got = _gate(cfg)
            assert got.proceed and got.owned and not got.stale_allowed

    def test_owned_nonempty_refuses_without_regenerate(self):
        with recording() as rec:
            cfg = _seed(rec, owned=True, objects=[f"{PREFIX}/part-0"])
            got = _gate(cfg)
            assert not got.proceed and "--regenerate" in got.message

    def test_regenerate_on_owned_clears_only_the_datagen_prefix(self):
        """C2-8: the datagen prefix, its multipart uploads, never the bucket."""
        with recording() as rec:
            cfg = _seed(
                rec,
                owned=True,
                objects=[f"{PREFIX}/part-0", f"{PREFIX}/part-1", "checkpoints/x", OWNER_MARKER_KEY],
            )
            got = _gate(cfg, regenerate=True)
            assert got.proceed and got.cleared == 2
            assert sorted(rec.buckets_store[BRONZE]) == sorted(["checkpoints/x", OWNER_MARKER_KEY])
            rec.assert_recorded(api="s3", method="list_multipart_uploads")

    def test_adopted_nonempty_refuses(self):
        """An unowned bucket with objects: refused, named, nothing deleted."""
        with recording() as rec:
            cfg = _seed(rec, owned=False, objects=[f"{PREFIX}/part-0"])
            got = _gate(cfg)
            assert not got.proceed
            assert "did not create" in got.message and "--allow-stale-bronze" in got.message
            assert list(rec.buckets_store[BRONZE]) == [f"{PREFIX}/part-0"]

    def test_regenerate_on_unowned_deletes_nothing(self):
        """Fails reverted: --regenerate emptied the whole bucket, whoever owned it."""
        with recording() as rec:
            cfg = _seed(rec, owned=False, objects=[f"{PREFIX}/part-0", "theirs/data"])
            got = _gate(cfg, regenerate=True)
            assert not got.proceed
            assert "does not empty a bucket this deployment did not create" in got.message
            assert sorted(rec.buckets_store[BRONZE]) == [f"{PREFIX}/part-0", "theirs/data"]
            assert not [c for c in rec.mutations() if c.api == "s3"]

    def test_allow_flag_recorded(self):
        with recording() as rec:
            cfg = _seed(rec, owned=False, objects=[f"{PREFIX}/part-0", f"{PREFIX}/part-1"])
            got = _gate(cfg, allow_stale_bronze=True)
            assert got.proceed and got.stale_allowed
            assert got.record() == {
                "allowed": True,
                "objects_before": 2,
                "bucket": BRONZE,
                "prefix": PREFIX,
            }
            assert list(rec.buckets_store[BRONZE]) == [f"{PREFIX}/part-0", f"{PREFIX}/part-1"]

    def test_unowned_empty_proceeds(self):
        with recording() as rec:
            cfg = _seed(rec, owned=False, objects=["other/x"])
            assert _gate(cfg).proceed

    def test_marker_alone_is_empty(self):
        with recording() as rec:
            cfg = _seed(rec, owned=False, objects=[OWNER_MARKER_KEY])
            assert _gate(cfg).proceed

    def test_regenerate_empty_prefix_refused(self):
        with recording() as rec:
            cfg = _seed(rec, owned=True, objects=["x/part-0"])
            cfg.architecture.pipeline.medallion.bronze.path_template = "/"
            got = _gate(cfg, regenerate=True)
            assert not got.proceed and "prefix is empty" in got.message
            assert list(rec.buckets_store[BRONZE]) == ["x/part-0"]

    def test_listing_failure_refuses(self):
        with recording() as rec:
            cfg = _seed(rec, owned=True)
            rec.fail_s3("list_objects_v2", "InternalError")
            got = _gate(cfg)
            assert not got.proceed and "could not list" in got.message

    def test_ownership_that_cannot_be_checked_is_a_prerequisite_not_a_refusal(self):
        """A transient API error proving ownership exits 4, not 3."""

        def raising(cfg, bucket, s3=None, *, strict=False):
            if strict:
                raise RuntimeError("apiserver timed out")
            return False

        with recording() as rec:
            cfg = _seed(rec, owned=True, objects=[f"{PREFIX}/part-0"])
            with patch("lakebench.deploy.datagen.deployment_may_empty", raising):
                got = _gate(cfg, regenerate=True)
            assert not got.proceed and "could not check who owns" in got.message
            assert got.exit_code == ExitCode.PREREQUISITE

    def test_cli_exits_refused_on_refusal(self):
        import typer

        from lakebench.cli._helpers import enforce_bronze_gate

        with recording() as rec:
            cfg = _seed(rec, owned=False, objects=[f"{PREFIX}/part-0"])
            with pytest.raises(typer.Exit) as e:
                enforce_bronze_gate(cfg, regenerate=True)
            assert e.value.exit_code == ExitCode.REFUSED  # run.bronze_nonempty


class TestDeployerCycleZero:
    def _deployer(self, cfg, allow=False):
        from lakebench.deploy.datagen import DatagenDeployer

        engine = MagicMock()
        engine.config = cfg
        return DatagenDeployer(engine, allow_stale_bronze=allow)

    def test_multicycle_cycle0_unowned_refuses(self):
        """Fails reverted: the clear skipped an unowned bucket with an INFO line."""
        from lakebench.deploy.datagen import StaleBronzeRefused

        with recording() as rec:
            cfg = _seed(rec, owned=False, objects=[f"{PREFIX}/part-0"])
            with pytest.raises(StaleBronzeRefused):
                self._deployer(cfg)._clear_bronze_prefix_if_fresh(0, PREFIX)
            assert list(rec.buckets_store[BRONZE]) == [f"{PREFIX}/part-0"]

    def test_allowed_stale_proceeds_without_clearing(self):
        with recording() as rec:
            cfg = _seed(rec, owned=False, objects=[f"{PREFIX}/part-0"])
            self._deployer(cfg, allow=True)._clear_bronze_prefix_if_fresh(0, PREFIX)
            assert list(rec.buckets_store[BRONZE]) == [f"{PREFIX}/part-0"]

    def test_owned_is_cleared_and_append_cycles_keep(self):
        with recording() as rec:
            cfg = _seed(rec, owned=True, objects=[f"{PREFIX}/part-0", OWNER_MARKER_KEY])
            d = self._deployer(cfg)
            d._clear_bronze_prefix_if_fresh(1, PREFIX)
            assert f"{PREFIX}/part-0" in rec.buckets_store[BRONZE]
            d._clear_bronze_prefix_if_fresh(0, PREFIX)
            assert list(rec.buckets_store[BRONZE]) == [OWNER_MARKER_KEY]

    def test_a_16_adopted_empty_record_does_not_count_as_owned(self):
        """SAF-10: only the created record (or an owner marker) proves this
        cluster's claim; 1.6's adopted-empty record does not."""
        from lakebench.deploy.datagen import StaleBronzeRefused

        with recording() as rec:
            cfg = _seed(rec, owned=False, objects=[f"{PREFIX}/part-0"], created_record=False)
            rec.s3_tagging = False
            ns = rec.store[("namespaces", None, NS)]
            ns.metadata.annotations["lakebench.deployment/adopted-empty-buckets"] = BRONZE
            with pytest.raises(StaleBronzeRefused):
                self._deployer(cfg)._clear_bronze_prefix_if_fresh(0, PREFIX)
            assert list(rec.buckets_store[BRONZE]) == [f"{PREFIX}/part-0"]

    def test_marker_claim_counts_as_owned(self):
        import json

        with recording() as rec:
            cfg = _seed(rec, owned=False, created_record=False)
            rec.s3_tagging = False
            rec.buckets_store[BRONZE] = {
                f"{PREFIX}/part-0": b"x",
                OWNER_MARKER_KEY: json.dumps({"deployment": NS, "cluster": FP}).encode(),
            }
            self._deployer(cfg)._clear_bronze_prefix_if_fresh(0, PREFIX)
            assert list(rec.buckets_store[BRONZE]) == [OWNER_MARKER_KEY]

    def test_unproven_legacy_bucket_is_not_owned(self):
        """Row 4: our name tag, no cluster stamp, not in the record."""
        from lakebench.deploy.datagen import StaleBronzeRefused

        with recording() as rec:
            cfg = _seed(rec, owned=False, objects=[f"{PREFIX}/part-0"], created_record=False)
            rec.tags_store[BRONZE] = {TAG_DEPLOYMENT_NAME: NS}
            with pytest.raises(StaleBronzeRefused):
                self._deployer(cfg)._clear_bronze_prefix_if_fresh(0, PREFIX)
            got = _gate(cfg, regenerate=True)
            assert not got.proceed and "reclaim-bucket" in got.message


def test_continuous_unowned_refuses():
    """The continuous start refuses an unowned bucket before any datagen
    (``_require_reset_ownership``, the same SAF-10 verdicts)."""
    from kubernetes import client

    from lakebench.cli._sustained import _bucket_ownership_problem

    with recording() as rec:
        cfg = _seed(rec, owned=False, objects=[f"{PREFIX}/part-0"])
        for b in ("u01-silver", "u01-gold"):
            rec.add_bucket(b, tags={TAG_DEPLOYMENT_NAME: NS, TAG_CLUSTER: FP})
        problem = _bucket_ownership_problem(cfg, client.CoreV1Api())
        assert problem and BRONZE in problem


def test_stale_bronze_in_metrics_and_badge():
    from datetime import datetime, timezone

    from lakebench.metrics.collector import PipelineMetrics
    from lakebench.metrics.verdict import compute_badge_status

    m = PipelineMetrics(run_id="r", deployment_name=NS, start_time=datetime.now(timezone.utc))
    m.datagen_stale_bronze = {
        "allowed": True,
        "objects_before": 7,
        "bucket": BRONZE,
        "prefix": PREFIX,
    }
    assert m.to_dict()["datagen"]["stale_bronze"]["objects_before"] == 7
    _ok, _reasons, warnings = compute_badge_status(m)
    assert any("bronze held 7 objects before generate" in w for w in warnings)


# ---------------------------------------------------------------------------
# LB-231
# ---------------------------------------------------------------------------


def _destroy(rec, *, clean_buckets: bool):
    from lakebench.deploy.destroy import destroy_all
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict
    from lakebench.k8s.client import K8sClient

    cfg = _cfg()
    cfg.platform.kubernetes.create_namespace = False
    rec.for_config(cfg)
    rec.add_namespace(
        NS,
        annotations={
            "lakebench.deployment/name": NS,
            "lakebench.deployment/created-buckets": BRONZE,
        },
    )
    rec.add_bucket(
        BRONZE,
        [f"{PREFIX}/part-0"],
        tags={TAG_DEPLOYMENT_NAME: NS, TAG_CLUSTER: FP, "lakebench.created": "true"},
    )
    rec.add(
        "configmaps",
        {
            "metadata": {"name": "lakebench-silver-state"},
            "data": {"bronze_data_clock": "2026-09-30", "rebuild_epoch_c360_iceberg": "3"},
        },
        namespace=NS,
    )
    rec.add_spark_operator(watched=[NS])
    rec.add_stackable()
    engine = MagicMock()
    engine.config = cfg
    engine.k8s = K8sClient(namespace=NS)
    match = IdentityReport(
        verdict=IdentityVerdict.MATCH, resource_name=NS, expected_deployment=NS, found_deployment=NS
    )
    with (
        patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=match),
        patch("lakebench.deploy.destroy._sleep", lambda s: None),
    ):
        destroy_all(engine, clean_buckets=clean_buckets)
    return rec.store[("configmaps", NS, "lakebench-silver-state")].data


@pytest.mark.parametrize("clean_buckets", [True, False])
def test_destroy_clears_the_bronze_clock_only_with_bronze(clean_buckets):
    with recording() as rec:
        data = _destroy(rec, clean_buckets=clean_buckets)
        assert data["rebuild_epoch_c360_iceberg"] == "3"  # counters never go back
        assert data["bronze_data_clock"] == ("" if clean_buckets else "2026-09-30")


class TestMultiCycleGate:
    """The multi-cycle loop runs the gate with clear_owned: an owned prefix is
    cleared as 1.6 did before cycle 0, and --allow-stale-bronze still applies
    to an unowned one."""

    def test_owned_prefix_is_cleared_without_regenerate(self):
        with recording() as rec:
            cfg = _seed(rec, owned=True, objects=[f"{PREFIX}/part-0", OWNER_MARKER_KEY])
            got = _gate(cfg, clear_owned=True)
            assert got.proceed and got.cleared == 1
            assert list(rec.buckets_store[BRONZE]) == [OWNER_MARKER_KEY]

    def test_unowned_with_allow_stale_proceeds(self):
        with recording() as rec:
            cfg = _seed(rec, owned=False, objects=[f"{PREFIX}/part-0"])
            got = _gate(cfg, clear_owned=True, allow_stale_bronze=True)
            assert got.proceed and got.stale_allowed
            assert list(rec.buckets_store[BRONZE]) == [f"{PREFIX}/part-0"]

    def test_unowned_without_the_flag_refuses(self):
        with recording() as rec:
            cfg = _seed(rec, owned=False, objects=[f"{PREFIX}/part-0"])
            assert not _gate(cfg, clear_owned=True).proceed

    def test_run_wires_the_multicycle_gate_with_clear_owned(self):
        """[static] the loop's call passes clear_owned=True and the user's flags."""
        import ast
        from pathlib import Path

        src = Path(__file__).resolve().parent.parent / "src/lakebench/cli/_run.py"
        calls = [
            n
            for n in ast.walk(ast.parse(src.read_text()))
            if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "enforce_bronze_gate"
        ]
        assert len(calls) == 2
        multi = [c for c in calls if any(k.arg == "clear_owned" for k in c.keywords)]
        assert len(multi) == 1
        assert [ast.unparse(a) for a in multi[0].args] == [
            "cfg",
            "regenerate",
            "allow_stale_bronze",
        ]
