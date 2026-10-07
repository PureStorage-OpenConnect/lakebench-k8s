"""The corpus series marker and the multi-cycle corpus rules (CD-18, C36-1).

DESIGN ch05 section 7.1: one window function (``config.c360_run``), the
series marker ``<prefix>/_corpus/series.json`` (``deploy.corpus``), and the
rules: ``generate`` and ``run --generate`` refused with cycles > 1; a run
that reuses the corpus needs a finished marker for the config; a generating
multi-cycle run goes through the bronze gate before cycle 0.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from lakebench.config.c360_run import cycle_windows, series_clock
from lakebench.deploy import corpus
from tests.conftest import make_config
from tests.fixtures.memory_s3 import MemoryBoto

BUCKET = "test-fixture-bronze"
SERIES = "customer/interactions/_corpus/series.json"
D1 = "sha256:" + "1" * 64
D2 = "sha256:" + "2" * 64


def _legacy_range(i, n, s=None, e=None):
    """``DatagenDeployer._cycle_timestamp_range`` before CD-18 (copied)."""
    start = datetime.strptime(s or "2024-01-01", "%Y-%m-%d")
    end = datetime.strptime(e or "2025-12-31", "%Y-%m-%d")
    per = (end - start).days // n
    lo = start + timedelta(days=per * i)
    hi = end if i == n - 1 else start + timedelta(days=per * (i + 1))
    return lo.strftime("%Y-%m-%d"), hi.strftime("%Y-%m-%d")


def test_windows_match_legacy():
    """cycle_windows is today's arithmetic: every window of every cycle count
    the old function served (it ran only for cycles > 1 with no window; one
    cycle with no window is the generator's own default end, 2025-01-01)."""
    for window in [(None, None), ("2024-01-01", "2024-12-31"), ("2023-03-05", None)]:
        for n in range(1, 7):
            s, e = window
            got = cycle_windows(n, s, e)
            if n == 1 and e is None:
                assert got == [(s or "2024-01-01", "2025-01-01")]
                continue
            assert got == [_legacy_range(i, n, s, e) for i in range(n)]


def test_window_bounds_read_their_first_ten_characters():
    assert cycle_windows(2, "2024-01-01T00:00:00", "2024-12-31 00:00") == cycle_windows(
        2, "2024-01-01", "2024-12-31"
    )


def _cfg(cycles=1, schema="customer360", **dg):
    datagen = {"seed": 42 if schema == "customer360" else 43, "scale": 1, **dg}
    return make_config(
        architecture={
            "pipeline": {"mode": "batch", "cycles": cycles},
            "workload": {"schema": schema, "datagen": datagen},
        }
    )


def test_series_clock_spans_every_cycle():
    assert series_clock(_cfg(4)) == ("2024-01-01", "2025-12-31")
    assert series_clock(_cfg(1)) == ("2024-01-01", "2025-01-01")


class S3:
    """``S3Client`` over a dict (bucket exists)."""

    _init_error = None

    def __init__(self, store=None, exists=True):
        self.store = {} if store is None else store
        self.exists = exists

    def bucket_exists(self, bucket):
        return self.exists

    @property
    def raw_client(self):
        return MemoryBoto(self.store)


def _marker(s3):
    return json.loads(s3.store[(BUCKET, SERIES)])


def _write_series(s3, cfg, cycles, complete, run_id="r1", **over):
    body = corpus._body(cfg, cycles, complete, run_id, D1, None, None)
    body.update(over)
    s3.store[(BUCKET, SERIES)] = json.dumps(body).encode()


# -- the reuse rule ------------------------------------------------------------


def test_incomplete_series_refuses_skip_generate():
    cfg = _cfg(4)
    s3 = S3()
    _write_series(s3, cfg, 4, [0, 1])
    why = corpus.series_problem(cfg, corpus.read_series(cfg, s3))
    assert why == (
        "series incomplete: cycle(s) [2, 3] missing (the generate that wrote it did not finish)"
    )


def test_complete_series_through_json_is_accepted():
    """Windows read back from JSON are lists; the rule compares them as such."""
    cfg = _cfg(4)
    s3 = S3()
    _write_series(s3, cfg, 4, [0, 1, 2, 3])
    assert corpus.series_problem(cfg, corpus.read_series(cfg, s3)) is None


def test_single_cycle_skip_generate_refuses_changed_generation():
    cfg = _cfg(1)
    s3 = S3()
    _write_series(s3, cfg, 1, [0])
    edited = _cfg(1, scale=2)
    why = corpus.series_problem(edited, corpus.read_series(edited, s3))
    assert why is not None and why.startswith("series made with")


def test_skip_generate_ignores_image_digest():
    cfg = _cfg(1)
    s3 = S3()
    _write_series(s3, cfg, 1, [0])
    body = _marker(s3)
    body["generation"]["image_digest"] = None
    body["generation"]["image_digest_reason"] = "datagen pods ran different images"
    s3.store[(BUCKET, SERIES)] = json.dumps(body).encode()
    assert corpus.series_problem(cfg, corpus.read_series(cfg, s3)) is None


def test_other_seed_is_refused_without_printing_it():
    cfg = _cfg(1)
    s3 = S3()
    _write_series(s3, cfg, 1, [0])
    other = _cfg(1, seed=4242)
    why = corpus.series_problem(other, corpus.read_series(other, s3))
    assert why == "series made with another seed than the config's"
    assert "4242" not in why


@pytest.mark.parametrize(
    ("cycles", "series_cycles", "complete", "expect"),
    [
        (1, 4, [0, 1, 2, 3], "series made with 4 cycle(s), config says 1"),
        (4, 1, [0], "series made with 1 cycle(s), config says 4"),
    ],
)
def test_cycle_count_must_match(cycles, series_cycles, complete, expect):
    s3 = S3()
    _write_series(s3, _cfg(series_cycles), series_cycles, complete)
    cfg = _cfg(cycles)
    assert corpus.series_problem(cfg, corpus.read_series(cfg, s3)) == expect


def test_explicit_default_window_is_the_same_corpus():
    s3 = S3()
    _write_series(s3, _cfg(1), 1, [0])
    explicit = _cfg(1, timestamp_start="2024-01-01", timestamp_end="2025-01-01")
    assert corpus.series_problem(explicit, corpus.read_series(explicit, s3)) is None


def test_no_marker_is_allowed_only_for_one_cycle():
    s3 = S3()
    assert corpus.series_problem(_cfg(1), corpus.read_series(_cfg(1), s3)) is None
    why = corpus.series_problem(_cfg(3), corpus.read_series(_cfg(3), s3))
    assert why is not None and why.startswith("no series marker at s3://")
    missing = S3(exists=False)
    assert corpus.read_series(_cfg(1), missing).error is None


def test_an_unreadable_marker_is_never_absent():
    s3 = S3({(BUCKET, SERIES): b"{not json"})
    read = corpus.read_series(_cfg(1), s3)
    assert read.present and read.series is None
    why = corpus.series_problem(_cfg(1), read)
    assert why is not None and "unusable" in why


def test_a_marker_that_disagrees_with_the_node_markers_is_refused():
    cfg = _cfg(1)
    read = corpus.SeriesRead(series=corpus._body(cfg, 1, [0], "r", D1, None, None))
    read.series_check = "series.json names another seed than the corpus markers"
    assert "does not describe the corpus" in corpus.series_problem(cfg, read)


def test_s3_failure_is_an_error_not_an_absence():
    class Broken(S3):
        @property
        def raw_client(self):
            raise RuntimeError("endpoint down")

    read = corpus.read_series(_cfg(1), Broken())
    assert read.error and "endpoint down" in read.error


def test_financial_seed_ref_is_never_plaintext():
    # datagen_seed.seed_ref (CD-3+4) is the official form: the salted hash
    # the datagen pods write into their corpus markers (config_seed_ref), so
    # the series marker and the markers name one seed the same way.
    from lakebench.config.seed_secret import config_seed_ref

    cfg = _cfg(1, schema="financial")
    ref = corpus.seed_ref(cfg)
    assert ref == config_seed_ref(cfg)
    assert re.fullmatch(r"[0-9a-f]{64}", ref) and "43" != ref
    assert corpus.seed_ref(_cfg(1)) == "42"


# -- writing ------------------------------------------------------------------


def test_record_cycle_builds_a_complete_series():
    cfg = _cfg(3)
    s3 = S3()
    corpus.begin_series(cfg, s3, 3, "run-a")
    assert _marker(s3)["cycles_complete"] == []
    for c in range(3):
        assert corpus.record_cycle(cfg, s3, c, 3, "run-a", D1, None) == "written"
    m = _marker(s3)
    assert m["cycles_complete"] == [0, 1, 2] and m["generation"]["image_digest"] == D1
    assert m["windows"] == [list(w) for w in cycle_windows(3)]
    assert corpus.series_problem(cfg, corpus.read_series(cfg, s3)) is None


def test_record_cycle_never_claims_another_runs_marker():
    """Another generate wrote the marker since this run began (two runs in
    one namespace): this run's cycle is not recorded, the marker is left as
    the other run wrote it, and the caller is told (it fails the run)."""
    cfg = _cfg(1)
    s3 = S3()
    _write_series(s3, cfg, 1, [], run_id="other-run")
    before = dict(s3.store)
    assert corpus.record_cycle(cfg, s3, 0, 1, "this-run", D1, None) == "conflict"
    assert s3.store == before


def test_record_cycle_after_a_missed_cycle_leaves_the_marker_incomplete():
    cfg = _cfg(3)
    s3 = S3()
    _write_series(s3, cfg, 3, [0], run_id="r")  # cycle 1's write failed
    assert corpus.record_cycle(cfg, s3, 2, 3, "r", D1, None) == "unwritten"
    assert _marker(s3)["cycles_complete"] == [0]


def test_record_cycle_with_no_marker_records_the_cycle_alone():
    """The bucket did not exist when the generate began (no begin marker)."""
    cfg = _cfg(1)
    s3 = S3()
    assert corpus.record_cycle(cfg, s3, 0, 1, "r", D1, None) == "written"
    assert _marker(s3)["cycles_complete"] == [0]


def test_single_cycle_without_a_marker_refuses_a_multi_cycle_corpus():
    s3 = S3(
        {
            (BUCKET, "customer/interactions/part-000000.parquet"): b"x",
            (BUCKET, "customer/interactions/part-c001-000000.parquet"): b"x",
        }
    )
    why = corpus.series_problem(_cfg(1), corpus.read_series(_cfg(1), s3))
    assert why is not None and "of cycles after the first (part-cNNN-*)" in why


def test_series_marker_records_image_digest():
    cfg = _cfg(2)
    s3 = S3()
    corpus.begin_series(cfg, s3, 2, "r")
    corpus.record_cycle(cfg, s3, 0, 2, "r", D1, None)
    corpus.record_cycle(cfg, s3, 1, 2, "r", D2, None)
    gen = _marker(s3)["generation"]
    assert gen["image_digest"] is None
    assert gen["image_digest_reason"] == "cycles ran different images"
    assert corpus.digest_from_image_ids(["docker.io/x@" + D1, "docker.io/x@" + D1]) == (D1, None)
    assert corpus.digest_from_image_ids(["a@" + D1, "a@" + D2]) == (
        None,
        "datagen pods ran different images",
    )
    assert corpus.digest_from_image_ids([]) == (None, "no pod image id")
    assert corpus.digest_from_image_ids(["docker.io/x:1.6.0"])[0] is None


def test_record_cycle_failure_is_unwritten_not_raised():
    class ReadOnly(S3):
        @property
        def raw_client(self):
            boto = MemoryBoto(self.store)

            def refuse(**kw):
                raise RuntimeError("AccessDenied")

            boto.put_object = refuse  # type: ignore[method-assign]
            return boto

    assert corpus.record_cycle(_cfg(1), ReadOnly(), 0, 1, "r", D1, None) == "unwritten"
    with pytest.raises(corpus.SeriesWriteError):
        corpus.begin_series(_cfg(1), ReadOnly(), 1, "r")


def test_stale_label_rides_the_marker():
    cfg = _cfg(1)
    s3 = S3()
    corpus.begin_series(cfg, s3, 1, "r", stale={"allowed": True, "objects_before": 9})
    corpus.record_cycle(cfg, s3, 0, 1, "r", D1, None)
    assert _marker(s3)["stale_bronze"] == {"allowed": True, "objects_before": 9}


# -- the deployer writes the begin marker after its clear ------------------------


def _deployer(monkeypatch, s3, owned=True, allow=False):
    from lakebench.deploy import datagen as dg

    monkeypatch.setattr(dg, "_s3_client_for", lambda cfg: s3)
    monkeypatch.setattr(dg, "deployment_may_empty", lambda *a, **k: owned)
    monkeypatch.setattr(dg, "stop_previous_datagen", lambda cfg: None)
    monkeypatch.setenv("LB_RUN_ID", "run-x")
    engine = SimpleNamespace(
        config=_cfg(1),
        k8s=SimpleNamespace(apply_manifest=lambda *a, **k: None),
        renderer=SimpleNamespace(render=lambda *a, **k: "kind: Job\n"),
        context={},
        dry_run=False,
    )
    d = dg.DatagenDeployer(engine, allow_stale_bronze=allow)  # type: ignore[arg-type]
    monkeypatch.setattr(
        d,
        "_build_datagen_context",
        lambda: {
            "datagen_path_prefix": "customer/interactions",
            "datagen_parallelism": 1,
            "datagen_target_tb": "0.001",
        },
    )
    return d


class PrefixS3(S3):
    def has_user_objects(self, bucket, prefix=""):
        return any(k.startswith(prefix) for b, k in self.store if b == bucket)

    def delete_prefix(self, bucket, prefix, abort_multipart=False, keep_keys=frozenset()):
        gone = [
            k
            for k in self.store
            if k[0] == bucket and k[1].startswith(prefix + "/") and k[1] not in keep_keys
        ]
        for k in gone:
            del self.store[k]
        return len(gone)


def test_begin_marker_survives_the_fresh_clear(monkeypatch):
    s3 = PrefixS3({(BUCKET, "customer/interactions/part-000000.parquet"): b"x"})
    result = _deployer(monkeypatch, s3).deploy()
    assert result.status.value == "success", result.message
    assert set(s3.store) == {(BUCKET, SERIES)}
    m = _marker(s3)
    assert m["cycles_complete"] == [] and m["written_by_run"] == "run-x"


def test_unowned_stale_generate_labels_the_marker(monkeypatch):
    s3 = PrefixS3({(BUCKET, "customer/interactions/part-000000.parquet"): b"x"})
    result = _deployer(monkeypatch, s3, owned=False, allow=True).deploy()
    assert result.status.value == "success", result.message
    assert _marker(s3)["stale_bronze"] == {"allowed": True}


def test_continuous_reset_keeps_the_clearing_marker(monkeypatch):
    """The continuous reset clears the datagen prefix too: the marker that
    says a clear is under way is written first and kept by the clear."""
    from unittest.mock import MagicMock

    from lakebench.cli import _sustained
    from tests.fixtures.c360_reset_helpers import _c360_cfg

    cfg = _c360_cfg()
    monkeypatch.setattr(_sustained, "_require_reset_ownership", lambda c: None)
    client = MagicMock()
    client.delete_prefix.return_value = 0
    monkeypatch.setattr("lakebench.s3.S3Client", lambda **kw: client)
    _sustained._reset_continuous_state(cfg, clear_raw=True)
    put = client.raw_client.put_object.call_args.kwargs
    assert put["Key"] == SERIES and json.loads(put["Body"])["clearing"] is True
    last = client.delete_prefix.call_args_list[-1]
    assert last.args[1] == "customer/interactions"
    assert last.kwargs["keep_keys"] == frozenset({SERIES})
    assert all(
        c.kwargs["keep_keys"] == frozenset() for c in client.delete_prefix.call_args_list[:-1]
    )


def test_clearing_marker_refuses_reuse():
    cfg = _cfg(1)
    s3 = S3()
    corpus.mark_clearing(cfg, s3, "r")
    why = corpus.series_problem(cfg, corpus.read_series(cfg, s3))
    assert why is not None and "clear of the datagen prefix stopped part way" in why


def test_a_transient_read_is_not_taken_for_no_marker():
    """A GET that fails for another reason than a missing key leaves the
    marker alone: recording the cycle alone could claim another run's corpus."""
    from botocore.exceptions import ClientError

    class Flaky(S3):
        @property
        def raw_client(self):
            boto = MemoryBoto(self.store)

            def boom(**kw):
                raise ClientError({"Error": {"Code": "SlowDown", "Message": "x"}}, "GetObject")

            boto.get_object = boom  # type: ignore[method-assign]
            return boto

    s3 = Flaky()
    assert corpus.record_cycle(_cfg(1), s3, 0, 1, "r", D1, None) == "unwritten"
    assert s3.store == {}
