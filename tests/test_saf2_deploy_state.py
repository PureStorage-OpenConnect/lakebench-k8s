"""SAF-2 (b, c, f): deploy records its nonce; a nameless config may tear down
or read only a deployment it can prove is its own (CC-2).

The cluster is a fake ``CoreV1Api`` holding namespaces with their UID and
annotations; ``patch_namespace`` merges annotations like the API server.
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from kubernetes.client.rest import ApiException
from typer.testing import CliRunner

from lakebench.config import deploy_state as ds
from lakebench.config.loader import LoadPurpose, load_config
from lakebench.deploy.ownership import (
    ANNOTATION_CREATED_BUCKETS,
    ANNOTATION_DEPLOY_NONCE,
    ANNOTATION_DEPLOYMENT_NAME,
    ANNOTATION_STATE_SCHEMA,
    write_deploy_nonce,
)
from lakebench.exit_codes import PrerequisiteError, SafetyRefusal

NAME = "lb-x"
BUCKETS = f"{NAME}-bronze,{NAME}-gold,{NAME}-silver"


class FakeCore:
    """Namespaces by name: {"uid": str, "annotations": dict}."""

    def __init__(self) -> None:
        self.namespaces: dict[str, dict[str, Any]] = {}
        self.fail_reads = False
        self.reads = 0
        self.patches: list[tuple[str, dict]] = []

    def add(self, name: str, uid: str = "u1", **annotations: str) -> None:
        self.namespaces[name] = {"uid": uid, "annotations": dict(annotations)}

    def read_namespace(self, name: str, **kwargs: Any):
        self.reads += 1
        if self.fail_reads:
            raise ApiException(status=500, reason="boom")
        ns = self.namespaces.get(name)
        if ns is None:
            raise ApiException(status=404, reason="Not Found")
        return SimpleNamespace(
            metadata=SimpleNamespace(uid=ns["uid"], annotations=dict(ns["annotations"]))
        )

    def patch_namespace(self, name: str, body: dict) -> None:
        self.patches.append((name, body))
        self.namespaces[name]["annotations"].update(body["metadata"]["annotations"])


def _v16_namespace(core: FakeCore, nonce: str = "n16", **extra: str) -> None:
    core.add(
        NAME,
        **{
            ANNOTATION_DEPLOYMENT_NAME: NAME,
            ANNOTATION_DEPLOY_NONCE: nonce,
            ANNOTATION_CREATED_BUCKETS: BUCKETS,
            **extra,
        },
    )


def _nameless(d: Path, fname: str = "a.yaml") -> Path:
    p = d / fname
    p.write_text("recipe: hive-iceberg-spark-trino\n")
    return p


def _legacy_state(d: Path, name: str = NAME) -> None:
    (d / ".lakebench").mkdir(exist_ok=True)
    (d / ".lakebench" / "state.json").write_text(json.dumps({"name": name, "created": "x"}))


def _v17_state(cfg_path: Path, nonces: list[tuple[str, str]], **over: Any) -> Path:
    st = ds.new_state(cfg_path, NAME, NAME)
    st.nonces = [ds.NonceEntry(n, s, "t") for n, s in nonces]  # type: ignore[arg-type]
    for k, v in over.items():
        setattr(st, k, v)
    path = ds.state_path(cfg_path, NAME)
    ds.write_state(path, st)
    return path


def _load(cfg_path: Path, name: str | None = None, purpose=LoadPurpose.TEARDOWN):
    return load_config(cfg_path, purpose=purpose, name_override=name)


def _check(cfg_path: Path, core: FakeCore, name: str | None = None, **kw: Any) -> str | None:
    cfg = _load(cfg_path, name)
    return ds.check_nameless_target(cfg, lambda: core, config_path=cfg_path, **kw)


def _refused(path_name: str, fn, *a, **kw) -> SafetyRefusal:
    with pytest.raises(SafetyRefusal) as ei:
        fn(*a, **kw)
    assert ei.value.path == path_name, (ei.value.path, str(ei.value))
    return ei.value


# ---------------------------------------------------------------------------
# State file mechanics (b)
# ---------------------------------------------------------------------------


def test_state_round_trip_and_atomic_write(tmp_path):
    cfg = _nameless(tmp_path)
    path = _v17_state(cfg, [("a", "pending"), ("b", "confirmed")])
    st = ds.read_state_file(path)
    assert st is not None and st.kept_nonces() == ["a", "b"] and st.deploying
    assert not [p for p in path.parent.iterdir() if p.name.endswith(".tmp")]
    assert json.loads(path.read_text())["schema"] == ds.STATE_SCHEMA


def test_malformed_state_raises_state_error(tmp_path):
    p = tmp_path / ".lakebench" / f"{NAME}.json"
    p.parent.mkdir()
    p.write_text("{not json")
    with pytest.raises(ds.StateError):
        ds.read_state_file(p)


def test_reconcile_confirms_carried_pending(tmp_path):
    st = ds.new_state(tmp_path / "c.yaml", NAME, NAME)
    st.nonces = [ds.NonceEntry("p2", "pending", "t"), ds.NonceEntry("p1", "pending", "t")]
    core = FakeCore()
    core.add(NAME, uid="u9", **{ANNOTATION_DEPLOY_NONCE: "p1"})
    carried = ds.reconcile(st, ds.read_namespace_identity(core, NAME))
    assert carried is not None and carried.nonce == "p1" and carried.status == "confirmed"
    assert st.nonces[0].status == "pending"  # not carried: stays pending
    assert st.namespace_uid == "u9"


def test_nonce_list_capped_at_five(tmp_path):
    st = ds.new_state(tmp_path / "c.yaml", NAME, NAME)
    for i in range(9):
        ds.record_pending(st, f"n{i}", None)
    assert st.kept_nonces() == ["n8", "n7", "n6", "n5", "n4"]
    assert ds.STATE_NONCES_KEPT == 5


def test_carried_entry_survives_five_crashes(tmp_path):
    """The namespace carries a confirmed entry; five deploys then each record
    a nonce and crash before stamping. Plain truncation would evict it."""
    cfg = _nameless(tmp_path)
    core = FakeCore()
    _v16_namespace(core, nonce="good")
    st = ds.new_state(cfg, NAME, NAME)
    st.nonces = [ds.NonceEntry("good", "confirmed", "t")]
    for i in range(5):
        carried = ds.reconcile(st, ds.read_namespace_identity(core, NAME))
        ds.record_pending(st, f"crash{i}", carried)
    assert "good" in st.kept_nonces() and len(st.nonces) == 5
    ds.write_state(ds.state_path(cfg, NAME), st)
    assert _check(cfg, core, name=NAME) == "u1#good"


def test_two_crashed_deploys_own_namespace_accepted(tmp_path, monkeypatch):
    """Deploy 1 records and stamps P1, then crashes before confirming; deploy
    2 records P2 and crashes before stamping. The namespace carries P1, and
    a nameless teardown from the directory must still pass check 2. With a
    single pending slot P2 overwrites P1 and the user's own deployment is
    refused (the d1 defect)."""
    from lakebench.cli import _deploy

    named = _named(tmp_path)
    core = FakeCore()
    _v16_namespace(core, nonce="before")
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    cfg = load_config(named, purpose=LoadPurpose.MUTATE)
    p1 = _deploy._record_deploy_nonce(cfg, named, dry_run=False, nonce="P1")
    write_deploy_nonce(core, NAME, nonce=p1)  # deploy 1 stamps, then crashes
    _deploy._record_deploy_nonce(cfg, named, dry_run=False, nonce="P2")  # crashes
    nameless = _nameless(tmp_path)
    named.unlink()
    assert _check(nameless, core, name=NAME) == "u1#P1"


def test_pending_nonce_accepted(tmp_path):
    cfg = _nameless(tmp_path)
    core = FakeCore()
    _v16_namespace(core, nonce="P1")
    _v17_state(cfg, [("P1", "pending")])
    assert _check(cfg, core, name=NAME) == "u1#P1"


def test_concurrent_deploy_state_lock(tmp_path):
    """Two threads run steps 2 to 4 on one directory; both nonces survive."""
    cfg = _nameless(tmp_path)
    core = FakeCore()
    barrier = threading.Barrier(2)

    def deploy_steps(nonce: str) -> None:
        barrier.wait()
        with ds.state_lock(cfg, NAME):
            path = ds.state_path(cfg, NAME)
            st = ds.read_state_file(path) or ds.new_state(cfg, NAME, NAME)
            carried = ds.reconcile(st, ds.read_namespace_identity(core, NAME))
            ds.record_pending(st, nonce, carried)
            ds.write_state(path, st)

    threads = [threading.Thread(target=deploy_steps, args=(n,)) for n in ("t1", "t2")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    st = ds.read_state_file(ds.state_path(cfg, NAME))
    assert st is not None and sorted(st.kept_nonces()) == ["t1", "t2"]


def test_current_incarnation_reads_namespace(tmp_path):
    st = ds.new_state(tmp_path / "c.yaml", NAME, NAME)
    st.nonces = [ds.NonceEntry("X", "confirmed", "t")]
    core = FakeCore()
    core.add(NAME, uid="u1", **{ANNOTATION_DEPLOY_NONCE: "Z"})
    assert ds.current_incarnation(st, core) is None
    core.namespaces[NAME]["annotations"][ANNOTATION_DEPLOY_NONCE] = "X"
    assert ds.current_incarnation(st, core) == "u1#X"
    del core.namespaces[NAME]
    assert ds.current_incarnation(st, core) is None
    core.fail_reads = True
    with pytest.raises(ApiException):
        ds.current_incarnation(st, core)


def test_write_deploy_nonce_marks_v17_state_only_when_recorded():
    core = FakeCore()
    core.add("ns")
    assert write_deploy_nonce(core, "ns", nonce="abc") == "abc"
    anns = core.namespaces["ns"]["annotations"]
    assert anns[ANNOTATION_DEPLOY_NONCE] == "abc"
    assert anns[ANNOTATION_STATE_SCHEMA] == ds.STATE_SCHEMA
    core.add("ns2")
    fresh = write_deploy_nonce(core, "ns2")
    assert fresh and ANNOTATION_STATE_SCHEMA not in core.namespaces["ns2"]["annotations"]


# ---------------------------------------------------------------------------
# The nameless checks (c)
# ---------------------------------------------------------------------------


def test_named_config_is_not_checked(tmp_path):
    p = tmp_path / "n.yaml"
    p.write_text(f"name: {NAME}\nrecipe: hive-iceberg-spark-trino\n")
    _nameless(tmp_path, "other.yaml")
    called = []
    cfg = _load(p)
    assert ds.check_nameless_target(cfg, lambda: called.append(1), config_path=p) is None
    assert called == []


def test_two_nameless_destroy_second_refuses(tmp_path):
    a = _nameless(tmp_path, "a.yaml")
    _nameless(tmp_path, "b.yml")
    _legacy_state(tmp_path)
    core = FakeCore()
    _v16_namespace(core)
    err = _refused("nameless.ambiguous", _check, a, core)
    assert "b.yml" in str(err) and core.reads == 0


def test_two_nameless_with_name_passes_check1(tmp_path):
    a = _nameless(tmp_path, "a.yaml")
    _nameless(tmp_path, "b.yaml")
    _legacy_state(tmp_path)
    core = FakeCore()
    _v16_namespace(core)
    assert _check(a, core, name=NAME) == "u1#n16"


def test_single_config_dir_passes_check1(tmp_path):
    """Check 1 counts only the config's own directory, not subdirectories
    or non-config YAML."""
    d = tmp_path / "ns1"
    d.mkdir()
    a = _nameless(d)
    (d / "notes.yaml").write_text("foo: bar\n")
    sub = d / "sub"
    sub.mkdir()
    _nameless(sub)
    assert ds.nameless_configs_in(d) == [a.absolute()]


def test_ledger_dir_per_namespace_passes_check1(tmp_path):
    core = FakeCore()
    _v16_namespace(core)
    for ns in ("ns-a", "ns-b"):
        d = tmp_path / "ledger-configs" / ns
        d.mkdir(parents=True)
        cfg = _nameless(d)
        _legacy_state(d)
        assert _check(cfg, core, name=NAME) == "u1#n16"


def test_copied_dir_foreign_nonce_refused(tmp_path):
    """A state that names a live namespace whose nonce it never recorded."""
    cfg = _nameless(tmp_path)
    core = FakeCore()
    _v16_namespace(core, nonce="foreign")
    _v17_state(cfg, [("mine", "confirmed")])
    err = _refused("nameless.nonce_mismatch", _check, cfg, core, name=NAME)
    assert "foreign" in str(err)


def test_copied_dir_state_for_other_dir_refused(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    cfg_src = _nameless(src)
    _v17_state(cfg_src, [("n1", "confirmed")])
    dst = tmp_path / "dst"
    import shutil

    shutil.copytree(src, dst)
    core = FakeCore()
    _v16_namespace(core, nonce="n1")
    _refused("nameless.copied_dir", _check, dst / "a.yaml", core, name=NAME)


def test_state_from_other_host_refused(tmp_path):
    cfg = _nameless(tmp_path)
    _v17_state(cfg, [("n1", "confirmed")], host="elsewhere")
    core = FakeCore()
    _v16_namespace(core, nonce="n1")
    _refused("nameless.copied_dir", _check, cfg, core, name=NAME)


def test_v16_dir_needs_name(tmp_path):
    cfg = _nameless(tmp_path)
    _legacy_state(tmp_path)
    core = FakeCore()
    _v16_namespace(core)
    _refused("nameless.name_required", _check, cfg, core)


def test_v16_dir_name_stamp_mismatch(tmp_path):
    cfg = _nameless(tmp_path)
    _legacy_state(tmp_path)
    core = FakeCore()
    # Namespace stamped for another deployment.
    core.add(
        NAME,
        **{ANNOTATION_DEPLOYMENT_NAME: "lb-other", ANNOTATION_DEPLOY_NONCE: "n"},
    )
    _refused("nameless.stamp_mismatch", _check, cfg, core, name=NAME)
    # --name that is not the v1.6 name.
    _legacy_state(tmp_path, name="lb-y")
    _refused("nameless.stamp_mismatch", _check, cfg, core, name=NAME)


def test_v16_dir_buckets_must_be_stamped(tmp_path):
    cfg = _nameless(tmp_path)
    _legacy_state(tmp_path)
    core = FakeCore()
    _v16_namespace(core, **{ANNOTATION_CREATED_BUCKETS: f"{NAME}-bronze"})
    err = _refused("nameless.stamp_mismatch", _check, cfg, core, name=NAME)
    assert f"{NAME}-silver" in str(err)
    # A bucket whose own tag names the deployment counts.
    assert _check(cfg, core, name=NAME, bucket_owned=lambda b: b != f"{NAME}-bronze") == "u1#n16"


def test_v16_dir_name_stamp_match_proceeds(tmp_path):
    cfg = _nameless(tmp_path)
    _legacy_state(tmp_path)
    core = FakeCore()
    _v16_namespace(core)
    assert _check(cfg, core, name=NAME) == "u1#n16"


def test_v16_dir_without_legacy_state_and_name_proceeds(tmp_path):
    """No state at all: --name plus matching stamps (a config copied out of
    its v1.6 directory without state.json)."""
    cfg = _nameless(tmp_path)
    core = FakeCore()
    _v16_namespace(core)
    assert _check(cfg, core, name=NAME) == "u1#n16"


def test_check3_refuses_v17_stamped_namespace(tmp_path):
    cfg = _nameless(tmp_path)
    _legacy_state(tmp_path)
    core = FakeCore()
    _v16_namespace(core, **{ANNOTATION_STATE_SCHEMA: ds.STATE_SCHEMA})
    _refused("nameless.v17_state_elsewhere", _check, cfg, core, name=NAME)
    del core.namespaces[NAME]["annotations"][ANNOTATION_STATE_SCHEMA]
    assert _check(cfg, core, name=NAME) == "u1#n16"


def test_copied_legacy_state_destroy_with_name(tmp_path):
    """PC-1's byte copy of a v1.6 config and state.json takes check 3."""
    import shutil

    src = tmp_path / "worktree"
    src.mkdir()
    _nameless(src)
    _legacy_state(src)
    dst = tmp_path / "ledger-configs" / NAME
    shutil.copytree(src, dst)
    core = FakeCore()
    _v16_namespace(core)
    assert _check(dst / "a.yaml", core, name=NAME) == "u1#n16"
    _refused("nameless.name_required", _check, dst / "a.yaml", core)


def test_missing_namespace(tmp_path):
    cfg = _nameless(tmp_path)
    _legacy_state(tmp_path)
    core = FakeCore()
    _refused("nameless.namespace_missing", _check, cfg, core, name=NAME)
    assert _check(cfg, core, name=NAME, allow_absent=True) is None


def test_unreadable_namespace_is_a_prerequisite_error(tmp_path):
    cfg = _nameless(tmp_path)
    _legacy_state(tmp_path)
    core = FakeCore()
    core.fail_reads = True
    with pytest.raises(PrerequisiteError):
        _check(cfg, core, name=NAME)


# ---------------------------------------------------------------------------
# Relocation (f)
# ---------------------------------------------------------------------------


def test_relocated_state_passes_check2(tmp_path):
    src = tmp_path / "wt"
    src.mkdir()
    cfg = src / "c.yaml"
    cfg.write_text(f"name: {NAME}\nrecipe: hive-iceberg-spark-trino\n")
    _v17_state(cfg, [("n1", "confirmed")])
    dst_dir = tmp_path / "ledger" / NAME
    new_cfg = ds.relocate_state(cfg, dst_dir)
    assert new_cfg.read_bytes() == cfg.read_bytes()
    moved = ds.read_state_file(ds.state_path(new_cfg, NAME))
    assert moved is not None and moved.config_dir == str(dst_dir.absolute())
    assert moved.kept_nonces() == ["n1"] and moved.moved_from and not moved.moved_to
    source = ds.read_state_file(ds.state_path(cfg, NAME))
    assert source is not None and source.moved_to == str(dst_dir.absolute())
    # A nameless copy of the moved config passes check 2 from the new dir.
    nameless_new = _nameless(dst_dir, "x.yaml")
    new_cfg.unlink()
    core = FakeCore()
    _v16_namespace(core, nonce="n1")
    assert _check(nameless_new, core, name=NAME) == "u1#n1"


def test_relocated_source_refused(tmp_path):
    (tmp_path / "src").mkdir()
    cfg = _nameless(tmp_path / "src")
    _v17_state(cfg, [("n1", "confirmed")], moved_to=str(tmp_path / "new"))
    core = FakeCore()
    _v16_namespace(core, nonce="n1")
    _refused("nameless.moved", _check, cfg, core, name=NAME)
    named = _named(tmp_path / "src")
    with pytest.raises(ds.RelocateRefused, match="already moved"):
        ds.relocate_state(named, tmp_path / "again")


def test_relocate_refuses_other_host(tmp_path):
    cfg = tmp_path / "c.yaml"
    cfg.write_text(f"name: {NAME}\nrecipe: hive-iceberg-spark-trino\n")
    _v17_state(cfg, [("n1", "confirmed")], host="elsewhere")
    with pytest.raises(ds.RelocateRefused, match="host"):
        ds.relocate_state(cfg, tmp_path / "new")
    assert not (tmp_path / "new").exists()


def test_relocate_copies_legacy_state_and_module_entry(tmp_path, capsys):
    src = tmp_path / "src"
    src.mkdir()
    cfg = _nameless(src)
    _legacy_state(src)
    assert ds.main(["relocate", str(cfg), str(tmp_path / "dst")]) == 0
    assert (tmp_path / "dst" / ".lakebench" / "state.json").read_text() == (
        src / ".lakebench" / "state.json"
    ).read_text()
    assert ds.main(["relocate", str(cfg), str(tmp_path / "dst")]) == 3


# ---------------------------------------------------------------------------
# destroy_all's expected incarnation
# ---------------------------------------------------------------------------


def test_destroy_expected_incarnation_mismatch_deletes_nothing():
    from lakebench.deploy.destroy import destroy_all

    k8s = SimpleNamespace(
        namespace_exists=lambda ns: True,
        get_namespace_annotation=lambda ns, key: "Y",
        get_namespace_uid=lambda ns: "U",
    )
    cfg = SimpleNamespace(
        get_namespace=lambda: NAME,
        name=NAME,
        platform=SimpleNamespace(kubernetes=SimpleNamespace(context="")),
    )
    engine = SimpleNamespace(k8s=k8s, config=cfg)
    with patch("kubernetes.client.CoreV1Api") as core:
        results = destroy_all(engine, expected_incarnation="U#X")  # type: ignore[arg-type]
    assert len(results) == 1 and results[0].details["incarnation_mismatch"]
    assert "Destroy NOT started" in results[0].message
    assert not core.return_value.delete_namespace.called


# ---------------------------------------------------------------------------
# CLI: destroy, status, deploy
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_cluster(monkeypatch):
    core = FakeCore()
    import lakebench.cli._nameless as nameless

    monkeypatch.setattr(nameless, "_core_v1_factory", lambda cfg: lambda: core)
    monkeypatch.setattr(nameless, "_bucket_owned_factory", lambda cfg: lambda b: False)
    return core


class _RecordingEngine:
    calls: list[dict] = []

    def __init__(self, cfg, *a, **kw):
        self.cfg = cfg

    def destroy_all(self, **kw):
        _RecordingEngine.calls.append(kw)
        return []


def test_cli_destroy_passes_the_verified_incarnation(tmp_path, fake_cluster, monkeypatch):
    from lakebench.cli import app

    cfg = _nameless(tmp_path)
    _legacy_state(tmp_path)
    _v16_namespace(fake_cluster)
    _RecordingEngine.calls = []
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", _RecordingEngine)
    res = CliRunner().invoke(app, ["destroy", str(cfg), "--force", "--name", NAME])
    assert res.exit_code == 0, res.output
    assert _RecordingEngine.calls[0]["expected_incarnation"] == "u1#n16"


@pytest.mark.parametrize(
    ("argv_extra", "path_name"),
    [([], "nameless.name_required"), (["--name", "lb-y"], "nameless.stamp_mismatch")],
)
def test_cli_destroy_refused_makes_no_destroy_call(
    tmp_path, fake_cluster, monkeypatch, argv_extra, path_name
):
    from lakebench.cli import app

    cfg = _nameless(tmp_path)
    _legacy_state(tmp_path, name="lb-y" if argv_extra else NAME)
    _v16_namespace(fake_cluster)
    _RecordingEngine.calls = []
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", _RecordingEngine)
    res = CliRunner().invoke(app, ["destroy", str(cfg), "--force", *argv_extra])
    assert res.exit_code == 3, res.output
    assert _RecordingEngine.calls == []


def test_readonly_commands_create_no_files(tmp_path, fake_cluster, monkeypatch):
    """status and logs on a nameless config, with and without --name, leave
    the directory listing unchanged."""
    from lakebench.cli import app

    monkeypatch.chdir(tmp_path)
    cfg = _nameless(tmp_path)
    _legacy_state(tmp_path)
    _v16_namespace(fake_cluster)
    before = sorted(p.relative_to(tmp_path) for p in tmp_path.rglob("*"))
    runner = CliRunner()
    codes = [runner.invoke(app, ["status", str(cfg)]).exit_code]
    codes.append(runner.invoke(app, ["status", str(cfg), "--name", NAME]).exit_code)
    with patch("lakebench.k8s._pinned.subprocess.run"):
        codes.append(runner.invoke(app, ["logs", "hive", str(cfg), "--name", NAME]).exit_code)
    after = sorted(p.relative_to(tmp_path) for p in tmp_path.rglob("*"))
    assert after == before
    # Without --name a v1.6 directory is refused (3); with it the guard
    # passes and status goes on to its own read of the (unreachable) test
    # server, while logs, its kubectl mocked, completes.
    assert codes[0] == 3 and codes[1] not in (0, 3) and codes[2] == 0, codes


def _named(d: Path) -> Path:
    p = d / "named.yaml"
    p.write_text(f"name: {NAME}\nrecipe: hive-iceberg-spark-trino\n")
    return p


def test_dry_run_creates_no_state(tmp_path, monkeypatch):
    from lakebench.cli import _deploy

    cfg_path = _named(tmp_path)
    core = FakeCore()
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    cfg = load_config(cfg_path, purpose=LoadPurpose.MUTATE)
    before = sorted(os.listdir(tmp_path))
    assert _deploy._record_deploy_nonce(cfg, cfg_path, dry_run=True, nonce=None) is None
    assert sorted(os.listdir(tmp_path)) == before


def test_deploy_records_before_the_namespace_gets_it(tmp_path, monkeypatch):
    """Steps 1 to 4 then 7: the state holds the nonce pending before the
    namespace is stamped, and confirmed after."""
    from lakebench.cli import _deploy

    cfg_path = _named(tmp_path)
    core = FakeCore()
    _v16_namespace(core, nonce="old")
    st = ds.new_state(cfg_path, NAME, NAME)
    st.nonces = [ds.NonceEntry("old", "pending", "t")]
    ds.write_state(ds.state_path(cfg_path, NAME), st)
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    cfg = load_config(cfg_path, purpose=LoadPurpose.MUTATE)
    nonce = _deploy._record_deploy_nonce(cfg, cfg_path, dry_run=False, nonce="own")
    assert nonce == "own" and core.patches == []
    st = ds.read_state_file(ds.state_path(cfg_path, NAME))
    assert st is not None
    assert [(e.nonce, e.status) for e in st.nonces] == [("own", "pending"), ("old", "confirmed")]
    write_deploy_nonce(core, NAME, nonce="own")
    _deploy._confirm_deploy_nonce(cfg, cfg_path, "own")
    st = ds.read_state_file(ds.state_path(cfg_path, NAME))
    assert st is not None and st.nonces[0].status == "confirmed"


def test_deploy_state_write_failure_stops_before_any_change(tmp_path, monkeypatch):
    from lakebench.cli import _deploy

    cfg_path = _named(tmp_path)
    core = FakeCore()
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    cfg = load_config(cfg_path, purpose=LoadPurpose.MUTATE)

    def no_write(path, state):
        raise OSError("read-only file system")

    monkeypatch.setattr(ds, "write_state", no_write)
    with pytest.raises(PrerequisiteError, match="read-only"):
        _deploy._record_deploy_nonce(cfg, cfg_path, dry_run=False, nonce=None)
    assert core.patches == []


def test_deploy_refuses_moved_state(tmp_path, monkeypatch):
    from lakebench.cli import _deploy

    cfg_path = _named(tmp_path)
    _v17_state(cfg_path, [("n", "confirmed")], moved_to="/elsewhere")
    core = FakeCore()
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    cfg = load_config(cfg_path, purpose=LoadPurpose.MUTATE)
    _refused(
        "nameless.moved", _deploy._record_deploy_nonce, cfg, cfg_path, dry_run=False, nonce=None
    )


def test_deploy_cli_dry_run_writes_nothing(tmp_path, monkeypatch):
    """`deploy --dry-run` through the CLI leaves the directory unchanged."""
    from lakebench.cli import app

    cfg_path = _named(tmp_path)
    monkeypatch.chdir(tmp_path)
    core = FakeCore()
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)

    class DryEngine:
        def __init__(self, cfg, dry_run=False, **kw):
            self.results = []
            self.deploy_nonce = None

        def deploy_all(self, **kw):
            return []

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", DryEngine)
    monkeypatch.setattr("lakebench.cli._deploy.check_datagen_scale", lambda cfg: None)
    before = sorted(p.relative_to(tmp_path) for p in tmp_path.rglob("*"))
    res = CliRunner().invoke(app, ["deploy", str(cfg_path), "--dry-run"])
    assert res.exit_code == 0, res.output
    after = sorted(
        p.relative_to(tmp_path) for p in tmp_path.rglob("*") if "lakebench-journal" not in str(p)
    )
    assert after == before


# -- review fixes (CC-2 Full review, 10-01) ------------------------------------


def test_relocating_a_copied_directory_is_refused(tmp_path):
    """`cp -r A B` then relocating B would give A and the new directory the
    same deployment (the copy's state still names A)."""
    import shutil

    a = tmp_path / "A"
    a.mkdir()
    cfg_a = _named(a)
    _v17_state(cfg_a, [("nA", "confirmed")])
    b = tmp_path / "B"
    shutil.copytree(a, b)
    with pytest.raises(ds.RelocateRefused, match="only the directory that deployed"):
        ds.relocate_state(b / cfg_a.name, tmp_path / "C")
    assert not (tmp_path / "C").exists()
    st = ds.read_state_file(ds.state_path(cfg_a, NAME))
    assert st is not None and st.moved_to is None


def test_a_directory_recreated_at_the_same_path_is_not_the_original(tmp_path):
    """Same path, different directory (inode): the state is a copy."""
    import shutil

    a = tmp_path / "A"
    a.mkdir()
    cfg_a = _named(a)
    _v17_state(cfg_a, [("nA", "confirmed")])
    st = ds.read_state_file(ds.state_path(cfg_a, NAME))
    assert st is not None and ds.not_here(st, cfg_a) is None
    shutil.copytree(a, tmp_path / "copy")
    shutil.rmtree(a)
    (tmp_path / "copy").rename(a)
    st = ds.read_state_file(ds.state_path(cfg_a, NAME))
    assert st is not None and "a copy of" in (ds.not_here(st, cfg_a) or "")


def test_deploy_refuses_a_copied_state(tmp_path, monkeypatch):
    from lakebench.cli import _deploy

    cfg_path = _named(tmp_path)
    _v17_state(cfg_path, [("n1", "confirmed")], config_dir="/elsewhere")
    core = FakeCore()
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    cfg = load_config(cfg_path, purpose=LoadPurpose.MUTATE)
    _refused(
        "deploy.state_copied",
        _deploy._record_deploy_nonce,
        cfg,
        cfg_path,
        dry_run=False,
        nonce=None,
    )
    st = ds.read_state_file(ds.state_path(cfg_path, NAME))
    assert st is not None and st.kept_nonces() == ["n1"]


def test_deploy_to_a_new_namespace_starts_a_fresh_nonce_list(tmp_path, monkeypatch):
    """Nonces recorded for the old namespace never answer for the new one."""
    from lakebench.cli import _deploy

    cfg_path = tmp_path / "named.yaml"
    cfg_path.write_text(
        f"name: {NAME}\nrecipe: hive-iceberg-spark-trino\n"
        "platform:\n  kubernetes:\n    namespace: ns-b\n"
    )
    _v17_state(cfg_path, [("nA", "confirmed")])  # recorded for namespace lb-x
    core = FakeCore()
    core.add(NAME, uid="uA", **{ANNOTATION_DEPLOY_NONCE: "nA"})
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    cfg = load_config(cfg_path, purpose=LoadPurpose.MUTATE)
    _deploy._record_deploy_nonce(cfg, cfg_path, dry_run=False, nonce="nB")
    st = ds.read_state_file(ds.state_path(cfg_path, NAME))
    assert st is not None and st.namespace == "ns-b"
    assert st.kept_nonces() == ["nB"]
    assert ds.current_incarnation(st, core) is None  # ns-b not stamped yet


def test_check2_refuses_a_state_recorded_for_another_namespace(tmp_path, monkeypatch):
    cfg_path = _nameless(tmp_path)
    core = FakeCore()
    _v16_namespace(core, nonce="n1")
    _v17_state(cfg_path, [("n1", "confirmed")], namespace="lb-old")
    cfg = _load(cfg_path, NAME)
    with pytest.raises(SafetyRefusal) as ei:
        ds.check_nameless_target(cfg, lambda: core, config_path=cfg_path)
    assert ei.value.path == "nameless.nonce_mismatch"
    assert "lb-old" in str(ei.value)


def test_check2_accepts_the_same_directory_through_a_symlink(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    cfg_path = _nameless(real)
    core = FakeCore()
    _v16_namespace(core, nonce="n1")
    _v17_state(cfg_path, [("n1", "confirmed")])
    (tmp_path / "link").symlink_to(real)
    via_link = tmp_path / "link" / cfg_path.name
    for spelling in (via_link, real / ".." / "real" / cfg_path.name):
        cfg = _load(spelling, NAME)
        assert ds.check_nameless_target(cfg, lambda: core, config_path=spelling) == "u1#n1"


class _StampingEngine:
    """deploy_all creates the namespace and stamps it as the real engine does."""

    core: FakeCore
    raise_with: BaseException | None = None

    def __init__(self, cfg, dry_run=False, **kw):
        self.cfg = cfg
        self.results: list = []
        self.deploy_nonce = kw.get("deploy_nonce")

    def deploy_all(self, **kw):
        self.core.add(NAME, uid="u9")
        write_deploy_nonce(self.core, NAME, nonce=self.deploy_nonce)
        if self.raise_with is not None:
            raise self.raise_with
        return []


@pytest.mark.parametrize("raise_with", [None, RuntimeError("helm failed"), KeyboardInterrupt()])
def test_deploy_stamps_the_recorded_nonce_and_confirms_it(tmp_path, monkeypatch, raise_with):
    """Through the CLI: the namespace carries exactly the nonce the state
    recorded, with the v1.7 marker, and the entry is confirmed even when
    deploy_all fails or is interrupted after stamping."""
    from lakebench.cli import app

    cfg_path = _named(tmp_path)
    monkeypatch.chdir(tmp_path)
    core = FakeCore()
    _StampingEngine.core = core
    _StampingEngine.raise_with = raise_with
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", _StampingEngine)
    monkeypatch.setattr("lakebench.cli._deploy._preflight_check", lambda cfg: None)
    monkeypatch.setattr("lakebench.cli._deploy.check_datagen_scale", lambda cfg: None)
    CliRunner().invoke(app, ["deploy", str(cfg_path), "--yes"])
    st = ds.read_state_file(ds.state_path(cfg_path, NAME))
    assert st is not None and len(st.nonces) == 1
    anns = core.namespaces[NAME]["annotations"]
    assert anns[ANNOTATION_DEPLOY_NONCE] == st.nonces[0].nonce
    assert anns[ANNOTATION_STATE_SCHEMA] == ds.STATE_SCHEMA
    assert st.nonces[0].status == "confirmed"


def test_deploy_impl_honours_a_caller_nonce(tmp_path, monkeypatch):
    from lakebench.cli import _deploy

    cfg_path = _named(tmp_path)
    monkeypatch.chdir(tmp_path)
    core = FakeCore()
    _StampingEngine.core = core
    _StampingEngine.raise_with = None
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", _StampingEngine)
    monkeypatch.setattr(_deploy, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(_deploy, "check_datagen_scale", lambda cfg: None)
    try:
        _deploy._deploy_impl(cfg_path, yes=True, nonce="caller-nonce")
    except (SystemExit, Exception):  # noqa: BLE001 -- the outcome is the stamp
        pass
    assert core.namespaces[NAME]["annotations"][ANNOTATION_DEPLOY_NONCE] == "caller-nonce"


def test_unusable_state_directory_stops_deploy_with_exit_4(tmp_path, monkeypatch):
    """A plain file where .lakebench/ should be: the lock itself fails."""
    from lakebench.cli import app

    cfg_path = _named(tmp_path)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".lakebench").write_text("not a directory")
    core = FakeCore()
    _StampingEngine.core = core
    _StampingEngine.raise_with = None
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", _StampingEngine)
    monkeypatch.setattr("lakebench.cli._deploy._preflight_check", lambda cfg: None)
    monkeypatch.setattr("lakebench.cli._deploy.check_datagen_scale", lambda cfg: None)
    res = CliRunner().invoke(app, ["deploy", str(cfg_path), "--yes"])
    assert res.exit_code == 4, res.output
    assert core.patches == [] and NAME not in core.namespaces


def test_state_lock_times_out_instead_of_hanging(tmp_path):
    import fcntl

    cfg_path = _named(tmp_path)
    with ds.state_lock(cfg_path, NAME):
        lock = ds.state_path(cfg_path, NAME).with_suffix(".lock")
        with open(lock, "a") as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ds.StateError, match="held by another"):
            with ds.state_lock(cfg_path, NAME, timeout=0.3):
                pass


@pytest.mark.parametrize("bad", ["../evil", "a/b", ".hidden", "state", ""])
def test_state_path_refuses_names_that_are_not_plain_file_names(tmp_path, bad):
    with pytest.raises(ds.StateError):
        ds.state_path(tmp_path / "a.yaml", bad)


def test_relocate_removes_its_copies_when_the_source_cannot_be_marked(tmp_path, monkeypatch):
    (tmp_path / "src").mkdir()
    cfg_path = _named(tmp_path / "src")
    _v17_state(cfg_path, [("n1", "confirmed")])
    real_write = ds.write_state
    src_state = ds.state_path(cfg_path, NAME)

    def failing_write(path, state):
        if path == src_state:
            raise OSError("disk full")
        real_write(path, state)

    monkeypatch.setattr(ds, "write_state", failing_write)
    with pytest.raises(OSError):
        ds.relocate_state(cfg_path, tmp_path / "dst")
    dst = tmp_path / "dst"
    assert not (dst / cfg_path.name).exists()
    assert not ds.state_path(dst / cfg_path.name, NAME).exists()
    st = ds.read_state_file(src_state)
    assert st is not None and st.moved_to is None


def test_relocate_moves_a_nameless_state_keyed_by_name(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    cfg_path = _nameless(src)
    _v17_state(cfg_path, [("n1", "confirmed")])
    dst = ds.relocate_state(cfg_path, tmp_path / "dst", name=NAME)
    moved = ds.read_state_file(ds.state_path(dst, NAME))
    assert moved is not None and moved.kept_nonces() == ["n1"]
    assert moved.config_dir == str((tmp_path / "dst").resolve())
    old = ds.read_state_file(ds.state_path(cfg_path, NAME))
    assert old is not None and old.moved_to


# -- fix pass 2 -------------------------------------------------------------------


def test_a_failed_directory_fsync_does_not_fail_the_write(tmp_path, monkeypatch):
    """The rename has landed; raising then would make callers roll back a
    state that is already in place."""
    cfg_path = _named(tmp_path)
    real_fsync = os.fsync
    calls = {"n": 0}

    def fsync(fd):
        calls["n"] += 1
        if calls["n"] > 1:  # the file's own fsync passes, the directory's fails
            raise OSError(22, "Invalid argument")
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fsync)
    path = _v17_state(cfg_path, [("n1", "confirmed")])
    st = ds.read_state_file(path)
    assert st is not None and st.kept_nonces() == ["n1"]


def test_directory_identity_ignores_the_device_number(tmp_path, monkeypatch):
    """An NFS or overlay remount can change st_dev; only the inode counts."""
    cfg_path = _named(tmp_path)
    _v17_state(cfg_path, [("n1", "confirmed")])
    real_stat = os.stat

    def remounted(p, *a, **k):
        st = real_stat(p, *a, **k)
        return os.stat_result((st.st_mode, st.st_ino, st.st_dev + 1, *tuple(st)[3:]))

    monkeypatch.setattr(os, "stat", remounted)
    st = ds.read_state_file(ds.state_path(cfg_path, NAME))
    assert st is not None and ds.not_here(st, cfg_path) is None


def test_relocate_accepts_a_directory_renamed_with_mv(tmp_path):
    a = tmp_path / "a"
    a.mkdir()
    cfg_a = _named(a)
    _v17_state(cfg_a, [("n1", "confirmed")])
    b = tmp_path / "b"
    a.rename(b)
    st = ds.read_state_file(ds.state_path(b / cfg_a.name, NAME))
    assert st is not None and ds.not_here(st, b / cfg_a.name) is not None
    assert ds.moved_with_its_directory(st, b / cfg_a.name)
    dst = ds.relocate_state(b / cfg_a.name, tmp_path / "c")
    moved = ds.read_state_file(ds.state_path(dst, NAME))
    assert moved is not None and ds.not_here(moved, dst) is None


@pytest.mark.parametrize("bad", ["State", "STATE", "a\x00b", "a\\b"])
def test_state_path_refuses_more_unsafe_names(tmp_path, bad):
    with pytest.raises(ds.StateError):
        ds.state_path(tmp_path / "a.yaml", bad)


def test_relocate_name_must_match_the_config_name(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    cfg_path = _named(src)
    _v17_state(cfg_path, [("n1", "confirmed")])
    with pytest.raises(ds.RelocateRefused, match="does not match"):
        ds.relocate_state(cfg_path, tmp_path / "dst", name="lb-other")


def test_dry_run_reports_a_copied_state(tmp_path, monkeypatch, capsys):
    from lakebench.cli import _deploy

    cfg_path = _named(tmp_path)
    _v17_state(cfg_path, [("n1", "confirmed")], config_dir="/elsewhere")
    core = FakeCore()
    monkeypatch.setattr("kubernetes.client.CoreV1Api", lambda *a, **k: core)
    cfg = load_config(cfg_path, purpose=LoadPurpose.MUTATE)
    assert _deploy._record_deploy_nonce(cfg, cfg_path, dry_run=True, nonce=None) is None
    assert "exit 3" in capsys.readouterr().out
