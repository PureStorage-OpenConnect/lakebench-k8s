"""SAF-10 on tagless backends: the owner marker and the keys it adds (SD-18b).

- ``write_owner_marker``: a conditional PUT where the backend enforces it,
  proved on a throwaway probe key (never the marker key, d3 N2); a
  read-back fallback otherwise.
- Every "is this empty" check, count and size skips ``.lakebench/`` keys,
  and every emptying but destroy's release keeps them.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from lakebench.deploy import ownership
from lakebench.deploy.ownership import OWNER_MARKER_KEY, write_owner_marker
from lakebench.s3.client import LAKEBENCH_KEY_PREFIX, has_user_objects
from tests.fixtures.recording_k8s import recording

NS = "u01"
B = "u01-bronze"
SRC = Path(__file__).resolve().parent.parent / "src" / "lakebench"
ME = {"deployment": NS, "cluster": "fp-here"}
THEM = {"deployment": NS, "cluster": "fp-there"}


@pytest.fixture(autouse=True)
def _fresh_mode_cache(monkeypatch):
    monkeypatch.setattr(ownership, "_MARKER_WRITE_MODE", {})
    monkeypatch.setattr(ownership, "_marker_sleep", lambda s: None)


def _boto():
    import boto3

    return boto3.client("s3")


def _marker(rec, bucket=B):
    raw = rec.buckets_store[bucket].get(OWNER_MARKER_KEY)
    return json.loads(raw) if raw is not None else None


def test_marker_prefixes_agree():
    assert ownership.MARKER_PREFIX == LAKEBENCH_KEY_PREFIX
    assert OWNER_MARKER_KEY.startswith(LAKEBENCH_KEY_PREFIX)


class TestWriteOwnerMarker:
    def test_conditional_claim_on_an_empty_bucket(self):
        with recording(NS) as rec:
            rec.add_bucket(B)
            got = write_owner_marker(_boto(), B, ME)
            assert got.ours and got.mode == "conditional"
            assert _marker(rec) == ME
            # The probe key is gone; only the marker stays.
            assert list(rec.buckets_store[B]) == [OWNER_MARKER_KEY]

    def test_conditional_marker_race(self):
        """Two clusters, one empty bucket: the second writer gets 412, reads the
        winner's marker and loses."""
        with recording(NS) as rec:
            rec.add_bucket(B)
            assert write_owner_marker(_boto(), B, THEM).ours
            got = write_owner_marker(_boto(), B, ME)
            assert not got.ours and got.marker == THEM
            assert _marker(rec) == THEM

    def test_probe_never_writes_marker_key(self):
        """d3 N2: on a backend that ignores IfNoneMatch, a second cluster's marker
        landing between our calls survives the probe (it is a throwaway key)."""
        with recording(NS) as rec:
            rec.s3_conditional = "ignored"
            rec.add_bucket(B)
            puts: list[str] = []

            def other_cluster_lands(bucket, key):
                puts.append(key)
                if key.startswith(".lakebench/probe-") and len(puts) == 1:
                    rec.buckets_store[bucket][OWNER_MARKER_KEY] = json.dumps(THEM).encode()

            rec.after_put = other_cluster_lands
            got = write_owner_marker(_boto(), B, ME)
            assert got.mode == "unconditional"
            assert not got.ours
            assert _marker(rec) == THEM
            assert all(k.startswith(".lakebench/probe-") for k in puts)

    def test_unsupported_conditional_writes_take_the_fallback(self):
        with recording(NS) as rec:
            rec.s3_conditional = "unsupported"
            rec.add_bucket(B)
            got = write_owner_marker(_boto(), B, ME)
            assert got.ours and got.mode == "unconditional"
            assert _marker(rec) == ME

    def test_fallback_detects_a_writer_inside_the_window(self):
        with recording(NS) as rec:
            rec.s3_conditional = "unsupported"
            rec.add_bucket(B)

            def overwritten(_s):
                rec.buckets_store[B][OWNER_MARKER_KEY] = json.dumps(THEM).encode()

            with patch.object(ownership, "_marker_sleep", overwritten):
                got = write_owner_marker(_boto(), B, ME)
            assert not got.ours

    def test_mode_is_cached_per_endpoint(self):
        with recording(NS) as rec:
            rec.add_bucket(B)
            rec.add_bucket("u01-silver")
            write_owner_marker(_boto(), B, ME)
            probes_before = [c for c in rec.calls if "probe-" in (c.name or "")]
            write_owner_marker(_boto(), "u01-silver", ME)
            probes_after = [c for c in rec.calls if "probe-" in (c.name or "")]
            assert probes_before and probes_after == probes_before


class TestMarkerIgnoredByEmptiness:
    @pytest.mark.parametrize(
        ("keys", "prefix", "holds"),
        [
            ([OWNER_MARKER_KEY], "", False),
            ([OWNER_MARKER_KEY, ".lakebench/probe-x"], "", False),
            # d1's MaxKeys=2 read two marker keys as empty whatever followed them.
            ([OWNER_MARKER_KEY, ".lakebench/probe-x", "zz/data"], "", True),
            (["a/data", OWNER_MARKER_KEY], "", True),
            ([OWNER_MARKER_KEY], ".", False),
            ([OWNER_MARKER_KEY, ".m"], ".", True),
            ([OWNER_MARKER_KEY], "data/", False),
            (["data/x"], "data/", True),
            ([OWNER_MARKER_KEY], ".lakebench/", False),
        ],
    )
    def test_has_user_objects(self, keys, prefix, holds):
        with recording(NS) as rec:
            rec.add_bucket(B, keys)
            assert has_user_objects(_boto(), B, prefix) is holds

    def test_client_listings_skip_marker(self):
        from lakebench.s3 import S3Client

        with recording(NS) as rec:
            rec.add_bucket(B, {OWNER_MARKER_KEY: b"{}", "d/1": b"abc"})
            s3 = S3Client(endpoint="http://10.0.1.50", access_key="a", secret_key="b")
            info = s3.get_bucket_size(B)
            assert (info.object_count, info.size_bytes) == (1, 3)
            assert s3.get_bucket_info(B).object_count == 1
            assert s3.has_user_objects(B)
            rec.buckets_store[B].pop("d/1")
            assert s3.get_bucket_size(B).object_count == 0
            assert not s3.has_user_objects(B)

    def test_empty_bucket_keeps_the_marker_unless_releasing(self):
        from lakebench.s3 import S3Client

        with recording(NS) as rec:
            rec.add_bucket(B, {OWNER_MARKER_KEY: b"{}", "d/1": b"x", "d/2": b"y"})
            s3 = S3Client(endpoint="http://10.0.1.50", access_key="a", secret_key="b")
            assert s3.empty_bucket(B) == 2  # verify loop does not wait on the marker
            assert list(rec.buckets_store[B]) == [OWNER_MARKER_KEY]
            s3.empty_bucket(B, keep_prefixes=())
            assert rec.buckets_store[B] == {}

    def test_delete_prefix_refuses_lakebench_keys(self):
        from lakebench.s3 import S3Client

        with recording(NS) as rec:
            rec.add_bucket(B, {OWNER_MARKER_KEY: b"{}"})
            s3 = S3Client(endpoint="http://10.0.1.50", access_key="a", secret_key="b")
            with pytest.raises(ValueError):
                s3.delete_prefix(B, ".lakebench")
            assert OWNER_MARKER_KEY in rec.buckets_store[B]


# ---------------------------------------------------------------------------
# Deploy writes the marker; clean and destroy treat it as described
# ---------------------------------------------------------------------------


def _deploy(rec, force_legacy=False):
    from lakebench.deploy.engine import DeploymentEngine
    from lakebench.k8s.client import K8sClient
    from tests.conftest import make_config

    cfg = make_config(name=NS)
    rec.for_config(cfg)
    if ("namespaces", None, NS) not in rec.store:
        rec.add_namespace(NS, annotations={"lakebench.deployment/name": NS})
    engine = DeploymentEngine(cfg, k8s_client=K8sClient(namespace=NS))
    with patch("lakebench.deploy.ownership.api_server_fingerprint", return_value="fp-here"):
        return engine._deploy_buckets(force_legacy=force_legacy)


class TestDeployWritesTheMarker:
    def test_created_tagless_buckets_are_marked(self):
        with recording(NS) as rec:
            rec.s3_tagging = False
            result = _deploy(rec)
            assert result.status.value == "success", result.message
            for b in ("u01-bronze", "u01-silver", "u01-gold"):
                m = _marker(rec, b)
                assert (m["deployment"], m["cluster"]) == (NS, "fp-here")

    def test_another_clusters_marker_refuses_the_deploy(self):
        with recording(NS) as rec:
            rec.s3_tagging = False
            rec.add_bucket(B, {OWNER_MARKER_KEY: json.dumps(THEM).encode()})
            result = _deploy(rec)
            assert result.status.value == "failed"
            assert "another cluster" in result.message
            assert _marker(rec) == THEM

    def test_recorded_legacy_tagless_bucket_is_marked(self):
        """Row 3 on FlashBlade: a 1.6 bucket in the created record gets its marker."""
        with recording(NS) as rec:
            rec.s3_tagging = False
            rec.add_namespace(
                NS,
                annotations={
                    "lakebench.deployment/name": NS,
                    "lakebench.deployment/created-buckets": B,
                },
            )
            rec.add_bucket(B, ["data/part-0"])
            result = _deploy(rec)
            assert result.status.value == "success", result.message
            assert _marker(rec)["cluster"] == "fp-here"
            assert "data/part-0" in rec.buckets_store[B]


def test_regenerate_and_clean_keep_owner_marker():
    """Only destroy's release passes keep_prefixes; clean and --regenerate keep
    the marker, so the next deploy still reads the bucket as owned."""
    tree = {p: ast.parse(p.read_text()) for p in SRC.rglob("*.py") if "spark/scripts" not in str(p)}
    bad = []
    for path, t in tree.items():
        for node in ast.walk(t):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "empty_bucket"
                and any(k.arg == "keep_prefixes" for k in node.keywords)
                and path.name != "destroy.py"
            ):
                bad.append(f"{path.relative_to(SRC)}:{node.lineno}")
    assert bad == []


def test_no_raw_listing():
    """[static] every object listing goes through s3/client.py, which skips the marker."""
    allowed = {"s3/client.py", "s3/conformance.py"}
    bad = []
    for path in SRC.rglob("*.py"):
        rel = path.relative_to(SRC).as_posix()
        if rel in allowed or rel.startswith("spark/scripts/"):
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr == "list_objects_v2":
                bad.append(f"{rel}:{node.lineno} list_objects_v2")
            if (
                node.func.attr == "get_paginator"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == "list_objects_v2"
            ):
                bad.append(f"{rel}:{node.lineno} get_paginator('list_objects_v2')")
    assert bad == []
