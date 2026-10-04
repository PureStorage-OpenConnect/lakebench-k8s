"""financial reproduce (AML-10) without a cluster: the record it reads, the
refusals before any cluster call, the outcome codes, and the pieces gold
and the scorer contribute (the [read-snapshot] lines, the scorer arguments,
the shared rule parameters, one attempt for the job).
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.metrics import read_snapshots as rs

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "src" / "lakebench" / "spark" / "scripts"
RUN = "20261003-120000-aaaaaa"
SNAPS = [
    {"table": t, "snapshot": i + 11, "total_records": 5, "rows": 5, "fp": "1", "cols_sha": "c"}
    for i, t in enumerate(
        ("silver.transactions", "silver.entities", "silver.silver_batch_versions")
    )
]


# --- the [read-snapshot] lines and the scorer arguments ------------------------


def test_gold_lines_round_trip_through_the_parser(load_script, monkeypatch):
    import sys
    import types

    for name in ("pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.storagelevel"):
        monkeypatch.setitem(sys.modules, name, MagicMock(name=name))
    monkeypatch.setitem(sys.modules, "pyspark.sql.window", types.ModuleType("w"))
    gf = load_script("gold_finalize_financial")
    lines = [
        "[lb] 2026-10-03T12:00:00 - " + gf.read_snapshot_line("silver.transactions", 11, 5),
        "[lb] 2026-10-03T12:00:00 - " + gf.read_snapshot_line("silver.entities", None, None),
        "[lb] 2026-10-03T12:00:00 - "
        + gf.read_snapshot_line("silver.silver_batch_versions", "unknown", None),
    ]
    got = rs.parse_read_snapshots("\n".join(lines))
    assert got == [
        {"table": "silver.transactions", "snapshot": 11, "total_records": 5},
        {"table": "silver.entities", "snapshot": "none", "total_records": None},
        {"table": "silver.silver_batch_versions", "snapshot": "unknown", "total_records": None},
    ]


def test_a_restarted_driver_logs_again_and_the_last_line_wins():
    log = "\n".join(
        [
            "[read-snapshot] table=silver.transactions snapshot=1 total_records=1",
            "[read-snapshot] table=silver.transactions snapshot=2 total_records=2",
        ]
    )
    assert rs.parse_read_snapshots(log) == [
        {"table": "silver.transactions", "snapshot": 2, "total_records": 2}
    ]


def test_score_arguments_round_trip():
    parsed = [{k: s[k] for k in ("table", "snapshot", "total_records")} for s in SNAPS]
    args = rs.score_arguments(
        parsed + [{"table": "silver.x", "snapshot": "unknown", "total_records": None}]
    )
    assert args[:2] == ["--read-snapshot", "silver.transactions=11:5"]
    values = args[1::2]
    assert [rs.parse_score_argument(v) for v in values] == parsed + [
        {"table": "silver.x", "snapshot": "unknown", "total_records": None}
    ]
    # The scorer's own pattern accepts exactly what the CLI writes.
    src = (SCRIPTS / "score_financial.py").read_text()
    pattern = re.search(r'_READ_SNAPSHOT_ARG = re.compile\(\s*r"(.+?)"\s*\)', src, re.S).group(1)
    assert all(re.match(pattern, v) for v in values)
    for bad in ("silver.t=1", "x;drop=1:2", "silver.t=one:2"):
        with pytest.raises(ValueError):
            rs.parse_score_argument(bad)
        assert not re.match(pattern, bad)


@pytest.mark.parametrize(
    ("snaps", "why"),
    [
        (None, "recorded no read snapshots"),
        ([], "recorded no read snapshots"),
        (SNAPS[:2], "2 read snapshots"),
        ([{**SNAPS[0], "snapshot": "unknown"}, *SNAPS[1:]], "no known snapshot"),
    ],
)
def test_a_record_that_cannot_drive_a_reproduction(snaps, why):
    assert why in rs.usable(snaps)


def test_a_usable_record_needs_no_fingerprint():
    assert rs.usable(SNAPS) is None
    assert rs.usable([{**s, "fp": None} for s in SNAPS]) is None


# --- shared rule parameters ------------------------------------------------------


def _rules(load_script, monkeypatch):
    import sys

    for name in ("pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types"):
        monkeypatch.setitem(sys.modules, name, MagicMock(name=name))
    return load_script("detection_rules")


def test_rule_params_follow_the_signature(load_script, monkeypatch):
    rules = _rules(load_script, monkeypatch)
    ents = object()

    def w1(txns, run_id, max_vertices=5):
        return None

    def w2(txns, run_id, silver_entities=None):
        return None

    def w4(txns, run_id, max_vertices_local=None):
        return None

    monkeypatch.delenv("LB_FINANCIAL_W1_MAX_VERTICES", raising=False)
    assert rules.rule_params(w1, "r", ents) == {"run_id": "r", "max_vertices": 8_000_000}
    assert rules.rule_params(w2, "r", ents) == {"run_id": "r", "silver_entities": ents}
    assert rules.rule_params(w2, "r", None) == {"run_id": "r"}
    assert rules.rule_params(w4, "r", ents) == {"run_id": "r"}
    monkeypatch.setenv("LB_FINANCIAL_W1_MAX_VERTICES", "12")
    assert rules.rule_params(w1, "r")["max_vertices"] == 12
    for off in ("0", "-3"):
        monkeypatch.setenv("LB_FINANCIAL_W1_MAX_VERTICES", off)
        assert "max_vertices" not in rules.rule_params(w1, "r")
    monkeypatch.setenv("LB_FINANCIAL_W1_MAX_VERTICES", "lots")
    assert rules.rule_params(w1, "r")["max_vertices"] == 8_000_000


def test_gold_replay_and_reproduce_build_rule_params_one_way():
    """No script builds its own run_id/silver_entities/max_vertices kwargs:
    each calls detection_rules.rule_params."""
    for script in ("gold_finalize_financial", "replay_financial", "reproduce_financial"):
        src = (SCRIPTS / f"{script}.py").read_text()
        assert "rule_params(" in src, script
        assert '["max_vertices"] =' not in src, script
        assert not re.search(r"\b_W1_MAX_VERTICES\b", src), script


# --- static: LB-226's second finding --------------------------------------------


@pytest.mark.parametrize("script", ["reproduce_financial", "replay_financial", "score_financial"])
def test_no_temp_view_feeds_a_merge(script):
    """A MERGE whose source is a temp view over a DSv2 table fails on Spark
    4.1 with Iceberg 1.11: these scripts register no temp view and run no
    MERGE from one."""
    tree = ast.parse((SCRIPTS / f"{script}.py").read_text())
    views = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Attribute) and n.attr in ("createOrReplaceTempView", "createTempView")
    ]
    merges = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Constant) and isinstance(n.value, str) and "MERGE INTO" in n.value
    ]
    assert not (views and merges), (script, len(views), len(merges))
    if script == "reproduce_financial":
        assert not views and not merges


# --- one attempt -------------------------------------------------------------------


def test_the_reproduce_job_is_not_retried():
    from lakebench.k8s.client import ClusterCapacity
    from lakebench.spark.job import JobType, SparkJobManager
    from tests.test_aml_bronze_ingest_profile import _config

    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = ClusterCapacity(
        total_cpu_millicores=434_000,
        total_memory_bytes=8 * 432 * 1024**3,
        node_count=8,
        largest_node_cpu_millicores=434_000 // 8,
        largest_node_memory_bytes=432 * 1024**3,
    )
    mgr = SparkJobManager(_config("financial", 1), k8s)
    policy = mgr._build_manifest(JobType.REPRODUCE_FINANCIAL)["spec"]["restartPolicy"]
    assert policy["onFailureRetries"] == 0 and policy["onSubmissionFailureRetries"] == 5
    other = mgr._build_manifest(JobType.REPLAY_FINANCIAL)["spec"]["restartPolicy"]
    assert other["onFailureRetries"] == 2


# --- the CLI -------------------------------------------------------------------------


def _write(runs: Path, run_id: str, **fields) -> None:
    d = runs / f"run-{run_id}"
    d.mkdir(parents=True)
    rec = {
        "run_id": run_id,
        "deployment_name": "aml-x",
        "start_time": f"2026-10-0{run_id[7]}T12:00:00+00:00",
        "experiment": {"workload": {"name": "financial"}, "mode": "batch"},
        "financial_scoring": {"run_id": f"{run_id}-c1", "read_snapshots": SNAPS},
    }
    for k, v in fields.items():
        rec[k] = v
    (d / "metrics.json").write_text(json.dumps(rec))


def test_the_record_is_the_latest_aml_batch_run_of_the_deployment(tmp_path):
    from lakebench.cli._financial import reproduce_record

    cfg = SimpleNamespace(name="aml-x")
    _write(tmp_path, "20261001-120000-aaaaaa")
    _write(tmp_path, "20261002-120000-aaaaaa")
    _write(
        tmp_path,
        "20261003-120000-cccccc",
        experiment={"workload": {"name": "financial"}, "mode": "sustained"},
    )
    _write(tmp_path, "20261004-120000-dddddd", deployment_name="other")
    _write(tmp_path, "20261005-120000-eeeeee", record_kind="benchmark")
    _write(
        tmp_path,
        "20261006-120000-ffffff",
        experiment={"workload": {"name": "customer360"}, "mode": "batch"},
    )
    rec, _where = reproduce_record(cfg, None, tmp_path)
    assert rec["run_id"] == "20261002-120000-aaaaaa"
    rec, where = reproduce_record(cfg, "20261001-120000-aaaaaa", tmp_path)
    assert rec["run_id"] == "20261001-120000-aaaaaa" and where.endswith("metrics.json")
    # --run names a record of another deployment, or not an AML batch run.
    assert reproduce_record(cfg, "20261004-120000-dddddd", tmp_path)[0] is None
    assert reproduce_record(cfg, "20261003-120000-cccccc", tmp_path)[0] is None
    assert reproduce_record(cfg, "20261009-000000-zzzzzz", tmp_path)[0] is None
    assert reproduce_record(SimpleNamespace(name="none"), None, tmp_path)[0] is None


class _Cluster:
    """Every cluster and S3 touch of the reproduce command, recorded."""

    def __init__(self, result=None, raw=None):
        self.calls: list[str] = []
        self.result = result
        self.raw = raw
        self.raw_client = self

    def job_manager(self, cfg):
        self.calls.append("job_manager")
        cluster = self

        class _Jobs:
            def submit_job(self, job_type, arguments=None, **k):
                cluster.calls.append(f"submit {job_type.value} {' '.join(arguments)}")
                return SimpleNamespace(state="SUBMITTED", message="ok")

        return _Jobs()

    def put_object(self, Bucket, Key, Body):  # noqa: N803
        self.calls.append(f"put {Key}")
        self.uploaded = json.loads(Body)

    def delete_object(self, Bucket, Key):  # noqa: N803
        self.calls.append(f"delete {Key}")

    def get_object(self, Bucket, Key):  # noqa: N803
        self.calls.append(f"get {Key}")
        if self.raw is None and self.result is None:
            raise FileNotFoundError(Key)
        if self.raw is not None:
            body = self.raw
        else:
            # The job echoes the nonce it was given, unless the case says not.
            result = {"nonce": self.uploaded["nonce"], **self.result}
            body = json.dumps(result)
        return {"Body": SimpleNamespace(read=lambda: body.encode())}


def _invoke(monkeypatch, tmp_path, cluster, *extra):
    import lakebench.cli._financial as fin

    cfg = SimpleNamespace(
        name="aml-x",
        platform=SimpleNamespace(
            kubernetes=SimpleNamespace(context=None),
            storage=SimpleNamespace(s3=SimpleNamespace(buckets=SimpleNamespace(gold="g"))),
        ),
        get_namespace=lambda: "ns-x",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(fin, "_load_config", lambda path, verb="financial": cfg)
    monkeypatch.setattr(fin, "_get_job_manager", cluster.job_manager)
    monkeypatch.setattr(fin, "_s3", lambda c: cluster)
    monkeypatch.setattr(fin, "_wait_for_sparkapp", lambda *a, **k: "COMPLETED")
    (tmp_path / "c.yaml").write_text("name: aml-x\n")
    argv = ["financial", "reproduce", str(tmp_path / "c.yaml"), "--alert-id", "abc-123", *extra]
    return CliRunner().invoke(app, argv)


@pytest.mark.parametrize(
    ("outcome", "code"),
    [
        ("reproduced", 0),
        ("mismatch", 1),
        ("rule_skipped", 1),
        ("not_found", 1),
        ("snapshot_gone", 4),
        ("something_new", 1),
    ],
)
def test_outcome_codes(monkeypatch, tmp_path, outcome, code):
    _write(tmp_path / "lakebench-output" / "runs", RUN)
    cluster = _Cluster(result={"outcome": outcome, "rule_id": "W2_structuring"})
    result = _invoke(monkeypatch, tmp_path, cluster)
    assert result.exit_code == code, result.output
    # The run id gold stamped on its alerts (a cycle's), not the record's.
    assert cluster.uploaded["run_id"] == f"{RUN}-c1"
    assert cluster.uploaded["read_snapshots"] == SNAPS and len(cluster.uploaded["nonce"]) == 32
    submit = next(c for c in cluster.calls if c.startswith("submit"))
    assert "--input s3a://g/scoring/reproduce/abc-123/input.json" in submit
    assert "--output s3a://g/scoring/reproduce/abc-123/result.json" in submit
    # The earlier result is cleared before the job runs.
    assert cluster.calls.index(
        "delete scoring/reproduce/abc-123/result.json"
    ) < cluster.calls.index(submit)


@pytest.mark.parametrize(
    "result, raw",
    [
        (None, None),
        ({"nonce": "an-earlier-reproduction", "outcome": "reproduced"}, None),
        (None, "not json"),
    ],
    ids=["no-result", "an-earlier-result", "garbage"],
)
def test_no_usable_result_is_a_failure(monkeypatch, tmp_path, result, raw):
    _write(tmp_path / "lakebench-output" / "runs", RUN)
    out = _invoke(monkeypatch, tmp_path, _Cluster(result=result, raw=raw))
    assert out.exit_code == 1, out.output


@pytest.mark.parametrize(
    ("scoring", "why"),
    [
        (None, "was not scored"),
        ({"recall": 0.5}, "was not scored"),
        ({"run_id": f"{RUN}-c1", "recall": 0.5}, "predates 1.7"),
        ({"run_id": f"{RUN}-c1", "read_snapshots": []}, "recorded no read snapshots"),
    ],
)
def test_a_record_without_read_snapshots_is_refused_before_any_cluster_call(
    monkeypatch, tmp_path, scoring, why
):
    _write(tmp_path / "lakebench-output" / "runs", RUN, financial_scoring=scoring)
    cluster = _Cluster()
    out = _invoke(monkeypatch, tmp_path, cluster)
    assert out.exit_code == 4 and why in out.output, out.output
    assert cluster.calls == []


def test_a_protected_record_is_refused_before_any_cluster_call(monkeypatch, tmp_path):
    from lakebench.aml import look_guard

    _write(tmp_path / "lakebench-output" / "runs", RUN)
    seen = {}

    def reason(record, **kw):
        seen.update(kw)
        return "corpus_role evaluation"

    monkeypatch.setattr(look_guard, "protected_record_reason", reason)
    cluster = _Cluster()
    out = _invoke(monkeypatch, tmp_path, cluster)
    assert out.exit_code == 2 and "protected AML corpus" in out.output, out.output
    assert cluster.calls == [] and seen.get("fail_closed") is True


def test_no_record_is_refused_before_any_cluster_call(monkeypatch, tmp_path):
    cluster = _Cluster()
    out = _invoke(monkeypatch, tmp_path, cluster, "--run", "20261009-000000-zzzzzz")
    assert out.exit_code == 2 and "--run" in out.output, out.output
    assert cluster.calls == []


@pytest.mark.parametrize(
    "argv",
    [
        ["--alert-id", "x' OR 1=1"],
        ["--alert-id", "a" * 129],
        ["--alert-id", "case/../x"],
        ["--alert-id", "abc", "--run", "../../etc"],
    ],
)
def test_a_bad_alert_or_run_id_is_refused_before_anything(monkeypatch, tmp_path, argv):
    import lakebench.cli._financial as fin

    called = []
    monkeypatch.setattr(fin, "_load_config", lambda *a, **k: called.append(1))
    out = CliRunner().invoke(app, ["financial", "reproduce", "c.yaml", *argv])
    assert out.exit_code == 2 and called == [], out.output


def test_the_batch_scorer_gets_what_gold_read(monkeypatch):
    """run_financial_scoring passes the parsed [read-snapshot] lines to the
    score job; covered mode does not."""
    from lakebench.cli import _aml_post as post

    seen = []

    class _Jobs:
        def submit_job(self, job_type, arguments=None, **k):
            seen.append(list(arguments))
            return SimpleNamespace(state=SimpleNamespace(), message="m")

    cfg = MagicMock()
    cfg.platform.storage.s3.buckets.bronze = "b"
    cfg.platform.storage.s3.buckets.gold = "g"
    monkeypatch.setattr("lakebench.deploy.datagen.bronze_datagen_prefix", lambda c: "pacs008/")
    snaps = [{k: s[k] for k in ("table", "snapshot", "total_records")} for s in SNAPS]
    monitor = MagicMock()
    monitor.wait_for_completion.return_value = SimpleNamespace(success=False, message="x")
    post.run_financial_scoring(cfg, RUN, _Jobs(), monitor, 60, read_snapshots=snaps)
    assert seen and seen[0][-6:] == rs.score_arguments(snaps)
