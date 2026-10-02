"""SAF-1 (CC-4): nothing destroys what it did not create in this invocation.

reproduce refuses an existing namespace or bucket instead of destroying it,
deploys with its own nonce and ``require_new``, and destroys only the
``uid#nonce`` it created. ``destroy --expect-incarnation`` (hidden) only adds
a refusal. Cluster and S3 calls go to the recording fake (tests/fixtures/
recording_k8s.py), so "no delete of any kind" is checked on every call.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from lakebench.deploy.engine import DeploymentResult, DeploymentStatus
from lakebench.deploy.ownership import ANNOTATION_DEPLOY_NONCE, ANNOTATION_DEPLOYMENT_NAME
from lakebench.exit_codes import REFUSAL_DETAIL, PrerequisiteError, SafetyRefusal, UsageError
from tests.fixtures.recording_k8s import recording

NAME = "lbrepro"
CFG = (
    f"name: {NAME}\n"
    "platform:\n"
    "  storage:\n"
    "    s3:\n"
    "      endpoint: http://10.0.1.50:80\n"
    "      access_key: test-access\n"
    "      secret_key: test-secret\n"
)


@pytest.fixture
def cfg_path(tmp_path: Path) -> Path:
    p = tmp_path / "repro.yaml"
    p.write_text(CFG)
    return p


def _cfg(path: Path):
    from lakebench.config import LoadPurpose, load_config

    return load_config(path, purpose=LoadPurpose.RUN)


class FakeEngine:
    """Stands in for DeploymentEngine; the real _deploy_impl and _destroy_impl
    run around it. deploy_all creates the namespace and stamps the nonce the
    real _deploy_impl gave it, as the real namespace step does."""

    calls: list[tuple[str, Any]] = []
    after_deploy: Any = None
    during_run: Any = None
    destroy_results: list[DeploymentResult] = []

    def __init__(self, cfg: Any, dry_run: bool = False, require_new: bool = False, **_: Any):
        self.config = cfg
        self.dry_run = dry_run
        self.require_new = require_new
        self.results: list[DeploymentResult] = []
        self.deploy_nonce: str | None = None

    def deploy_all(self, **_: Any) -> list[DeploymentResult]:
        from kubernetes import client
        from kubernetes.client.models import V1Namespace, V1ObjectMeta

        from lakebench.deploy.ownership import write_deploy_nonce

        core = client.CoreV1Api()
        ns = self.config.get_namespace()
        core.create_namespace(body=V1Namespace(metadata=V1ObjectMeta(name=ns)))
        write_deploy_nonce(core, ns, nonce=self.deploy_nonce)
        FakeEngine.calls.append(("deploy_all", (self.deploy_nonce, self.require_new)))
        if FakeEngine.after_deploy is not None:
            FakeEngine.after_deploy(core, ns)
        return []

    def destroy_all(self, **kw: Any) -> list[DeploymentResult]:
        FakeEngine.calls.append(("destroy_all", kw.get("expected_incarnation")))
        return list(FakeEngine.destroy_results)


@pytest.fixture
def pipeline(monkeypatch):
    """Patch everything around the cluster: engine, preflight, generate, run."""
    import lakebench.cli._deploy as deploy_mod
    import lakebench.cli._generate as gen_mod
    import lakebench.cli._reproduce as rep
    import lakebench.cli._run as run_mod

    FakeEngine.calls = []
    FakeEngine.after_deploy = None
    FakeEngine.destroy_results = []
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", FakeEngine)
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)
    monkeypatch.setattr(
        gen_mod, "generate", lambda **k: FakeEngine.calls.append(("generate", None))
    )
    FakeEngine.during_run = None

    def run(**_):
        FakeEngine.calls.append(("run", None))
        if FakeEngine.during_run is not None:
            FakeEngine.during_run()

    monkeypatch.setattr(run_mod, "run", run)
    result = object()

    def find(storage, name, watermark):
        FakeEngine.calls.append(("find", watermark))
        return result

    monkeypatch.setattr(rep, "_find_reproduce_run", find)
    return result


def _names(calls: list[tuple[str, Any]]) -> list[str]:
    return [c[0] for c in calls]


def _deletes(rec) -> list:
    return [c for c in rec.mutations() if c.verb.startswith("delete")]


# ---------------------------------------------------------------------------
# reproduce: refusals before deploy
# ---------------------------------------------------------------------------


def test_reproduce_refuses_existing_namespace(cfg_path, pipeline):
    """A namespace carrying a foreign nonce gets no destroy call and no write."""
    from lakebench.cli._reproduce import _run_pipeline

    cfg = _cfg(cfg_path)
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        rec.add_namespace(
            cfg.get_namespace(),
            annotations={ANNOTATION_DEPLOYMENT_NAME: NAME, ANNOTATION_DEPLOY_NONCE: "foreign"},
        )
        with pytest.raises(SafetyRefusal) as ei:
            _run_pipeline(cfg_path, None, False)
        assert rec.mutations() == []
    assert ei.value.path == "reproduce.existing_namespace"
    assert FakeEngine.calls == []


def test_reproduce_has_no_yes_flag_to_bypass(tmp_path):
    """SAF-1: -y bypasses neither refusal; reproduce does not take -y at all."""
    from lakebench.cli import app

    res = CliRunner().invoke(app, ["reproduce", str(tmp_path / "pkg.yaml"), "-y"])
    assert res.exit_code == 2, res.output
    assert "No such option" in res.output


@pytest.mark.parametrize("which", ["bronze", "silver", "gold"])
def test_reproduce_refuses_existing_bucket(cfg_path, pipeline, which):
    from lakebench.cli._reproduce import _run_pipeline

    cfg = _cfg(cfg_path)
    bucket = getattr(cfg.platform.storage.s3.buckets, which)
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        rec.add_bucket(bucket, ["k"])
        with pytest.raises(SafetyRefusal) as ei:
            _run_pipeline(cfg_path, None, False)
        assert rec.mutations() == []
    assert ei.value.path == "reproduce.existing_namespace" and bucket in ei.value.what
    assert "lakebench destroy" not in (ei.value.next or "")
    assert FakeEngine.calls == []


def test_reproduce_bucket_read_error_fails_closed(cfg_path, pipeline):
    from lakebench.cli._reproduce import _run_pipeline
    from lakebench.s3.client import S3BucketError

    cfg = _cfg(cfg_path)
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        with patch(
            "lakebench.s3.S3Client.bucket_exists", side_effect=S3BucketError("Access denied")
        ):
            with pytest.raises(PrerequisiteError):
                _run_pipeline(cfg_path, None, False)
        assert rec.mutations() == []
    assert FakeEngine.calls == []


@pytest.mark.parametrize("error", ["api", "connection"])
def test_reproduce_namespace_read_error_fails_closed(cfg_path, pipeline, error):
    from lakebench.cli._reproduce import _run_pipeline
    from lakebench.k8s.client import K8sResourceError

    exc = K8sResourceError("boom") if error == "api" else ConnectionRefusedError("refused")
    with recording() as rec:
        with patch("lakebench.k8s.client.K8sClient.namespace_exists", side_effect=exc):
            with pytest.raises(PrerequisiteError):
                _run_pipeline(cfg_path, None, False)
        assert rec.mutations() == []
    assert FakeEngine.calls == []


@pytest.mark.parametrize(
    ("key", "line"),
    [
        ("create_namespace", "  kubernetes:\n    create_namespace: false\n"),
        ("create_buckets", "      create_buckets: false\n"),
    ],
)
def test_reproduce_refuses_settings_that_need_existing_resources(tmp_path, pipeline, key, line):
    from lakebench.cli._reproduce import _run_pipeline

    text = (
        CFG + line if key == "create_buckets" else CFG.replace("platform:\n", "platform:\n" + line)
    )
    p = tmp_path / "c.yaml"
    p.write_text(text)
    with recording() as rec:
        with pytest.raises(UsageError, match=key):
            _run_pipeline(p, None, False)
        assert rec.mutations() == []
    assert FakeEngine.calls == []


# ---------------------------------------------------------------------------
# reproduce: its own nonce, and only its own destroy
# ---------------------------------------------------------------------------


def test_reproduce_nonce_changed_after_deploy(cfg_path, pipeline):
    """A second writer stamps another nonce after our deploy: no generate, run
    or destroy. Comparing against a value read back would accept it."""
    from lakebench.cli._reproduce import _run_pipeline

    def second_writer(core, ns):
        core.patch_namespace(
            ns, {"metadata": {"annotations": {ANNOTATION_DEPLOY_NONCE: "foreign"}}}
        )

    FakeEngine.after_deploy = second_writer
    cfg = _cfg(cfg_path)
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        with pytest.raises(SafetyRefusal) as ei:
            _run_pipeline(cfg_path, None, False)
        assert _deletes(rec) == []
    assert ei.value.path == "reproduce.nonce_changed"
    assert _names(FakeEngine.calls) == ["deploy_all"]


def test_reproduce_destroys_only_own_incarnation(cfg_path, pipeline):
    from kubernetes import client

    from lakebench.cli._reproduce import _run_pipeline

    cfg = _cfg(cfg_path)
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        assert _run_pipeline(cfg_path, None, False) is pipeline
        ns = client.CoreV1Api().read_namespace(cfg.get_namespace())
        uid = ns.metadata.uid
        own = ns.metadata.annotations[ANNOTATION_DEPLOY_NONCE]
    assert _names(FakeEngine.calls) == ["deploy_all", "generate", "run", "find", "destroy_all"]
    nonce, require_new = FakeEngine.calls[0][1]
    assert require_new is True and nonce == own
    assert FakeEngine.calls[-1] == ("destroy_all", f"{uid}#{own}")


def test_reproduce_keep_destroys_nothing(cfg_path, pipeline):
    from lakebench.cli._reproduce import _run_pipeline

    cfg = _cfg(cfg_path)
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        _run_pipeline(cfg_path, None, True)
    assert "destroy_all" not in _names(FakeEngine.calls)


def test_reproduce_post_destroy_refusal_is_reported(cfg_path, pipeline):
    """The namespace was redeployed while the run went on: destroy_all refuses
    before any delete, and the refusal reaches the caller."""
    from lakebench.cli._reproduce import _run_pipeline

    FakeEngine.destroy_results = [
        DeploymentResult(
            component="ownership-check",
            status=DeploymentStatus.FAILED,
            message="Destroy NOT started: namespace is not the deployment this command checked",
            details={"incarnation_mismatch": True, "expected": "u#a", "found": "u#b"},
        )
    ]
    cfg = _cfg(cfg_path)
    refusals: list = []
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        assert _run_pipeline(cfg_path, None, False, refusals) is pipeline
    assert len(refusals) == 1 and refusals[0].path == "destroy.incarnation_mismatch"


def test_reproduce_exits_3_after_the_verdict_when_its_destroy_was_refused(monkeypatch, tmp_path):
    import lakebench.cli._reproduce as rep
    from lakebench.cli import app
    from tests.test_exit_codes import _reproduce_package

    pkg = _reproduce_package(tmp_path, "abc")
    monkeypatch.setattr(rep, "_current_commit_sha", lambda: "abc")

    def pipeline(config_file, timeout, keep, refusals=None):
        refusals.append(SafetyRefusal("Destroy NOT started", path="destroy.incarnation_mismatch"))
        return object()

    monkeypatch.setattr(rep, "_run_pipeline", pipeline)
    for check in ("_sample_mismatch", "_policy_refusal", "_experiment_refusal"):
        monkeypatch.setattr(rep, check, lambda *a, **k: None)
    monkeypatch.setattr(rep, "_benchmark_samples", lambda m: 1)
    monkeypatch.setattr(rep, "_run_maintenance_policy", lambda m: None)
    monkeypatch.setattr(rep, "_run_query_set", lambda m: None)
    monkeypatch.setattr(rep, "_measure_actual_numbers", lambda m: {"scale_ratio": 0.992})
    res = CliRunner().invoke(app, ["reproduce", str(pkg)])
    assert res.exit_code == 3, res.output


# ---------------------------------------------------------------------------
# The engine under require_new: refuse, never adopt
# ---------------------------------------------------------------------------


def _engine(require_new: bool = True, exists: bool = False):
    from lakebench.deploy.engine import DeploymentEngine
    from tests.test_deploy import _make_config, _mock_k8s

    k8s = _mock_k8s()
    k8s.namespace_exists.return_value = exists
    with patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False):
        eng = DeploymentEngine(_make_config(), k8s_client=k8s, require_new=require_new)
    return eng, k8s


def test_engine_require_new_refuses_an_existing_namespace():
    eng, k8s = _engine(exists=True)
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
    ):
        result = eng._deploy_namespace()
    assert result.status is DeploymentStatus.FAILED
    assert result.details[REFUSAL_DETAIL] == "reproduce.existing_namespace"
    k8s.apply_manifest.assert_not_called()
    stamp.assert_not_called()


def test_engine_require_new_refuses_a_namespace_created_meanwhile():
    """The check passed, then a competing create landed: the plain create gets
    409 and the namespace is refused, not adopted and stamped."""
    from kubernetes.client.rest import ApiException

    eng, k8s = _engine(exists=False)
    core = MagicMock()
    core.create_namespace.side_effect = ApiException(status=409, reason="AlreadyExists")
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("kubernetes.client.CoreV1Api", return_value=core),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
        patch("lakebench.deploy.ownership.write_deploy_nonce") as nonce,
    ):
        result = eng._deploy_namespace()
    assert result.details[REFUSAL_DETAIL] == "reproduce.existing_namespace"
    k8s.apply_manifest.assert_not_called()
    stamp.assert_not_called()
    nonce.assert_not_called()
    assert eng.config.get_namespace() not in eng._namespace_created_this_run


def test_engine_without_require_new_still_adopts_an_existing_namespace():
    """deploy's own behaviour is unchanged (redeploy into its namespace)."""
    eng, k8s = _engine(require_new=False, exists=True)
    k8s.get_namespace_phase.return_value = "Active"
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
        patch("lakebench.deploy.ownership.write_deploy_nonce"),
        patch("kubernetes.client.CoreV1Api"),
    ):
        stamp.return_value = MagicMock(verdict=MagicMock())
        eng._deploy_namespace()
    stamp.assert_called_once()


@patch("lakebench.deploy.ownership.write_bucket_ownership_tag")
@patch("lakebench.deploy.ownership.record_created_buckets")
@patch("lakebench.s3.S3Client")
def test_engine_require_new_refuses_an_existing_bucket(s3_cls, record, write_tag):
    eng, _ = _engine()
    client = MagicMock()
    client._init_error = None
    # The check passes; silver then appears before its create (a race).
    client.bucket_exists.return_value = False
    client.ensure_buckets.return_value = {
        "lakebench-bronze": True,
        "lakebench-silver": False,
        "lakebench-gold": True,
    }
    s3_cls.return_value = client
    with patch("lakebench.k8s.get_k8s_client"), patch("kubernetes.client.CoreV1Api"):
        result = eng._deploy_buckets()
    assert result.status is DeploymentStatus.FAILED
    assert result.details[REFUSAL_DETAIL] == "reproduce.existing_namespace"
    assert "lakebench-silver" in result.message
    write_tag.assert_not_called()
    # The two it created are recorded, so a destroy can remove them.
    assert sorted(record.call_args.args[2]) == ["lakebench-bronze", "lakebench-gold"]


# ---------------------------------------------------------------------------
# destroy --expect-incarnation (hidden): only a refusal
# ---------------------------------------------------------------------------


class _Engine:
    """Real destroy_all over the recorder's fake API."""

    def __init__(self, cfg: Any, **_: Any):
        from lakebench.k8s.client import K8sClient

        self.config = cfg
        self.k8s = K8sClient(namespace=cfg.get_namespace())

    def destroy_all(self, **kw: Any):
        from lakebench.deploy.destroy import destroy_all

        return destroy_all(self, **kw)


def test_destroy_cli_expect_incarnation_mismatch(cfg_path, monkeypatch):
    from kubernetes import client

    from lakebench.cli import app

    cfg = _cfg(cfg_path)
    ns = cfg.get_namespace()
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", _Engine)
    with recording(ns) as rec:
        rec.for_config(cfg)
        rec.add_namespace(
            ns, annotations={ANNOTATION_DEPLOYMENT_NAME: NAME, ANNOTATION_DEPLOY_NONCE: "Y"}
        )
        uid = client.CoreV1Api().read_namespace(ns).metadata.uid
        res = CliRunner().invoke(
            app,
            ["destroy", str(cfg_path), "--yes", "--force", "--expect-incarnation", f"{uid}#X"],
        )
        assert res.exit_code == 3, res.output
        assert rec.mutations() == []
    assert "Destroy NOT started" in res.output


def test_destroy_expect_incarnation_is_hidden():
    from lakebench.cli import app

    res = CliRunner().invoke(app, ["destroy", "--help"])
    assert res.exit_code == 0
    assert "--expect-incarnation" not in res.output


@pytest.mark.parametrize("token", ["", "abc", "#n", "u#", "u#n#m"])
def test_destroy_expect_incarnation_malformed_is_a_usage_error(cfg_path, token):
    from lakebench.cli import app

    with recording() as rec:
        res = CliRunner().invoke(
            app, ["destroy", str(cfg_path), "--yes", "--expect-incarnation", token]
        )
        assert rec.calls == []
    assert res.exit_code == 2, res.output
    assert "is not UID#NONCE" in res.output


def test_destroy_expect_incarnation_refuses_local(cfg_path):
    from lakebench.cli import app

    res = CliRunner().invoke(
        app, ["destroy", str(cfg_path), "--yes", "--local", "--expect-incarnation", "u#n"]
    )
    assert res.exit_code == 2, res.output
    assert "does not apply to --local" in res.output


def test_destroy_expect_incarnation_reaches_destroy_all(cfg_path, monkeypatch):
    from lakebench.cli import app

    seen: list = []

    class Engine:
        def __init__(self, cfg, **_):
            pass

        def destroy_all(self, **kw):
            seen.append(kw.get("expected_incarnation"))
            return []

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)
    res = CliRunner().invoke(
        app, ["destroy", str(cfg_path), "--yes", "--expect-incarnation", "u1#n1"]
    )
    assert res.exit_code == 0, res.output
    assert seen == ["u1#n1"]


def test_destroy_without_the_flag_passes_no_expectation(cfg_path, monkeypatch):
    from lakebench.cli import app

    seen: list = []

    class Engine:
        def __init__(self, cfg, **_):
            pass

        def destroy_all(self, **kw):
            seen.append(kw.get("expected_incarnation"))
            return []

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)
    res = CliRunner().invoke(app, ["destroy", str(cfg_path), "--yes"])
    assert res.exit_code == 0, res.output
    assert seen == [None]


def test_destroy_expect_incarnation_must_equal_the_nameless_check(tmp_path, monkeypatch):
    """A nameless config's verified incarnation and the flag must agree."""
    import lakebench.cli._nameless as nameless
    from lakebench.cli import app
    from tests import test_saf2_deploy_state as t

    core = t.FakeCore()
    monkeypatch.setattr(nameless, "_core_v1_factory", lambda cfg: lambda: core)
    monkeypatch.setattr(nameless, "_bucket_owned_factory", lambda cfg: lambda b: False)
    cfg = t._nameless(tmp_path)
    t._legacy_state(tmp_path)
    t._v16_namespace(core)
    called: list = []

    class Engine:
        def __init__(self, cfg, **_):
            pass

        def destroy_all(self, **kw):  # pragma: no cover -- must not be reached
            called.append(kw)
            return []

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)
    res = CliRunner().invoke(
        app,
        ["destroy", str(cfg), "--force", "--name", t.NAME, "--expect-incarnation", "u1#other"],
    )
    assert res.exit_code == 3, res.output
    assert called == []


def _redeploy(ns: str):
    def go():
        from kubernetes import client

        client.CoreV1Api().patch_namespace(
            ns, {"metadata": {"annotations": {ANNOTATION_DEPLOY_NONCE: "theirs"}}}
        )

    return go


@pytest.mark.parametrize("keep", [False, True])
def test_reproduce_refuses_a_redeploy_during_the_run(cfg_path, pipeline, keep):
    """With or without --keep: no verdict from a run that may not be ours, and
    nothing destroyed."""
    from lakebench.cli._reproduce import _run_pipeline

    cfg = _cfg(cfg_path)
    FakeEngine.during_run = _redeploy(cfg.get_namespace())
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        with pytest.raises(SafetyRefusal) as ei:
            _run_pipeline(cfg_path, None, keep)
        assert _deletes(rec) == []
    assert ei.value.path == "reproduce.nonce_changed"
    assert _names(FakeEngine.calls) == ["deploy_all", "generate", "run"]


def test_reproduce_watermark_is_taken_after_generate(cfg_path, pipeline, monkeypatch):
    """A run another shell started during deploy or generate is older than
    the watermark, so it is not taken for this reproduce's run."""
    from datetime import datetime, timezone

    import lakebench.cli._generate as gen_mod
    from lakebench.cli._reproduce import _run_pipeline

    stamps: list = []
    monkeypatch.setattr(gen_mod, "generate", lambda **k: stamps.append(datetime.now(timezone.utc)))
    cfg = _cfg(cfg_path)
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        _run_pipeline(cfg_path, None, True)
    watermark = next(arg for name, arg in FakeEngine.calls if name == "find")
    assert stamps and watermark >= stamps[0]


def test_reproduce_journals_the_incarnation_it_created(cfg_path, pipeline, monkeypatch):
    from kubernetes import client

    from lakebench.cli._reproduce import _run_pipeline
    from lakebench.journal import EventType

    journal = MagicMock()
    monkeypatch.setattr("lakebench.cli._helpers.journal_open", lambda *a, **k: journal)
    cfg = _cfg(cfg_path)
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        _run_pipeline(cfg_path, None, True)
        ns = client.CoreV1Api().read_namespace(cfg.get_namespace())
    want = f"{ns.metadata.uid}#{ns.metadata.annotations[ANNOTATION_DEPLOY_NONCE]}"
    events = [
        c
        for c in journal.record.call_args_list
        if c.args[0] is EventType.REPRODUCE_CREATED_INCARNATION
    ]
    assert len(events) == 1 and events[0].kwargs["details"]["incarnation"] == want


def test_a_refused_destroy_turns_a_protocol_mismatch_into_3(monkeypatch, tmp_path):
    """The refused destroy outranks exit 14: the measurement may not be ours."""
    import lakebench.cli._reproduce as rep
    from lakebench.cli import app
    from tests.test_exit_codes import _reproduce_package

    pkg = _reproduce_package(tmp_path, "abc")
    monkeypatch.setattr(rep, "_current_commit_sha", lambda: "abc")

    def pipeline(config_file, timeout, keep, refusals=None):
        refusals.append(SafetyRefusal("Destroy NOT started", path="destroy.incarnation_mismatch"))
        return object()

    monkeypatch.setattr(rep, "_run_pipeline", pipeline)
    # Only the post-run check (the run's own sample count, 99) mismatches.
    monkeypatch.setattr(
        rep, "_sample_mismatch", lambda meta, n, *a, **k: "samples differ" if n == 99 else None
    )
    for check in ("_policy_refusal", "_experiment_refusal"):
        monkeypatch.setattr(rep, check, lambda *a, **k: None)
    monkeypatch.setattr(rep, "_benchmark_samples", lambda m: 99)
    res = CliRunner().invoke(app, ["reproduce", str(pkg)])
    assert res.exit_code == 3, res.output


def test_engine_require_new_accepts_its_own_create_after_a_lost_response():
    """The create landed but its response was lost: the retry finds the
    namespace carrying this deploy's nonce and goes on, instead of refusing
    (and leaking) its own namespace."""
    eng, k8s = _engine(exists=True)
    eng.deploy_nonce = "mine"
    eng._namespace_created_this_run.add(eng.config.get_namespace())
    k8s.get_namespace_annotation.return_value = "mine"
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
        patch("lakebench.deploy.ownership.write_deploy_nonce") as nonce,
        patch("kubernetes.client.CoreV1Api") as core,
    ):
        from lakebench.deploy.ownership import IdentityVerdict

        stamp.return_value = MagicMock(verdict=IdentityVerdict.MATCH)
        result = eng._deploy_namespace()
    assert not (result.details or {}).get(REFUSAL_DETAIL), result.message
    core.return_value.create_namespace.assert_not_called()
    assert stamp.call_args.kwargs["force_legacy"] is True
    assert nonce.call_args.kwargs["nonce"] == "mine"


def test_engine_require_new_refuses_a_seeded_name_with_another_nonce():
    eng, k8s = _engine(exists=True)
    eng.deploy_nonce = "mine"
    eng._namespace_created_this_run.add(eng.config.get_namespace())
    k8s.get_namespace_annotation.return_value = "theirs"
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
    ):
        result = eng._deploy_namespace()
    assert result.details[REFUSAL_DETAIL] == "reproduce.existing_namespace"
    stamp.assert_not_called()


def test_engine_require_new_create_carries_the_nonce():
    eng, _ = _engine(exists=False)
    eng.deploy_nonce = "mine"
    core = MagicMock()
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("kubernetes.client.CoreV1Api", return_value=core),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
        patch("lakebench.deploy.ownership.write_deploy_nonce"),
    ):
        from lakebench.deploy.ownership import IdentityVerdict

        stamp.return_value = MagicMock(verdict=IdentityVerdict.MATCH)
        eng._deploy_namespace()
    body = core.create_namespace.call_args.kwargs["body"]
    assert body["metadata"]["annotations"][ANNOTATION_DEPLOY_NONCE] == "mine"


def test_engine_stamps_the_recorded_nonce_not_a_fresh_one():
    """The real namespace step writes engine.deploy_nonce (the one deploy
    recorded); reproduce and the nameless checks both depend on it."""
    eng, k8s = _engine(require_new=False, exists=False)
    eng.deploy_nonce = "recorded"
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
        patch("lakebench.deploy.ownership.write_deploy_nonce") as nonce,
        patch("kubernetes.client.CoreV1Api"),
    ):
        from lakebench.deploy.ownership import IdentityVerdict

        stamp.return_value = MagicMock(verdict=IdentityVerdict.MATCH)
        eng._deploy_namespace()
    assert nonce.call_args.kwargs["nonce"] == "recorded"


@patch("lakebench.deploy.ownership.record_created_buckets")
@patch("lakebench.s3.S3Client")
def test_engine_require_new_checks_every_bucket_before_creating_any(s3_cls, record):
    eng, _ = _engine()
    client = MagicMock()
    client._init_error = None
    client.bucket_exists.side_effect = lambda b: b == "lakebench-gold"
    s3_cls.return_value = client
    result = eng._deploy_buckets()
    assert result.details[REFUSAL_DETAIL] == "reproduce.existing_namespace"
    assert "lakebench-gold" in result.message
    client.ensure_buckets.assert_not_called()
    client.create_bucket.assert_not_called()
    record.assert_not_called()


def test_engine_require_new_refuses_preprovisioned_buckets():
    eng, _ = _engine()
    eng.config.platform.storage.s3.create_buckets = False
    with patch.object(eng, "_record_preprovisioned_empty_buckets") as adopt:
        result = eng._deploy_buckets()
    assert result.details[REFUSAL_DETAIL] == "reproduce.existing_namespace"
    adopt.assert_not_called()


def test_plain_deploy_does_not_require_new(cfg_path, monkeypatch):
    """`lakebench deploy` keeps adopting its own namespace on a re-run."""
    import lakebench.cli._deploy as deploy_mod
    from lakebench.cli import app

    seen: list = []

    class Engine(FakeEngine):
        def __init__(self, cfg, dry_run=False, require_new=False, **kw):
            seen.append(require_new)
            super().__init__(cfg, dry_run=dry_run, require_new=require_new, **kw)

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)
    cfg = _cfg(cfg_path)
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        res = CliRunner().invoke(app, ["deploy", str(cfg_path), "--yes"])
    assert res.exit_code == 0, res.output
    assert seen == [False]


def test_engine_require_new_refuses_a_matching_nonce_it_did_not_create():
    """The nonce alone is not proof: the namespace must be one this engine
    tried to create."""
    eng, k8s = _engine(exists=True)
    eng.deploy_nonce = "mine"
    k8s.get_namespace_annotation.return_value = "mine"
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
    ):
        result = eng._deploy_namespace()
    assert result.details[REFUSAL_DETAIL] == "reproduce.existing_namespace"
    stamp.assert_not_called()


def test_own_incarnation_retries_one_failed_read(monkeypatch):
    import lakebench.cli._reproduce as rep
    from lakebench.config import deploy_state

    monkeypatch.setattr(rep.time, "sleep", lambda s: None)
    reads: list = []

    def read(core, ns):
        reads.append(ns)
        if len(reads) == 1:
            raise ConnectionResetError("blip")
        return deploy_state.NamespaceIdentity("u1", "own", NAME, "", frozenset())

    monkeypatch.setattr(deploy_state, "read_namespace_identity", read)
    cfg = MagicMock()
    cfg.get_namespace.return_value = NAME
    with patch("kubernetes.client.CoreV1Api"):
        assert rep._own_incarnation(cfg, Path("c.yaml"), "own", after="run") == "u1#own"
    assert len(reads) == 2
    reads.clear()
    monkeypatch.setattr(
        deploy_state,
        "read_namespace_identity",
        lambda core, ns: (_ for _ in ()).throw(ConnectionResetError("down")),
    )
    with patch("kubernetes.client.CoreV1Api"), pytest.raises(PrerequisiteError, match="after run"):
        rep._own_incarnation(cfg, Path("c.yaml"), "own", after="run")


# ---------------------------------------------------------------------------
# deploy --require-new (hidden): the harness's deploy refuses, never adopts
# ---------------------------------------------------------------------------


def test_deploy_require_new_reaches_the_engine(cfg_path, monkeypatch):
    import lakebench.cli._deploy as deploy_mod
    from lakebench.cli import app

    seen: list = []

    class Engine(FakeEngine):
        def __init__(self, cfg, dry_run=False, require_new=False, **kw):
            seen.append(require_new)
            super().__init__(cfg, dry_run=dry_run, require_new=require_new, **kw)

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)
    cfg = _cfg(cfg_path)
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        res = CliRunner().invoke(app, ["deploy", str(cfg_path), "--yes", "--require-new"])
    assert res.exit_code == 0, res.output
    assert seen == [True]


def test_deploy_require_new_refusal_exits_3(cfg_path, monkeypatch):
    """The engine's refusal reaches the user as exit 3, nothing adopted."""
    import lakebench.cli._deploy as deploy_mod
    from lakebench.cli import app

    class Engine(FakeEngine):
        def deploy_all(self, **_):
            assert self.require_new
            return [
                DeploymentResult(
                    component="namespace",
                    status=DeploymentStatus.FAILED,
                    message="Refused: namespace exists",
                    details={REFUSAL_DETAIL: "reproduce.existing_namespace"},
                )
            ]

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)
    monkeypatch.setattr(deploy_mod, "_preflight_check", lambda cfg: None)
    monkeypatch.setattr(deploy_mod, "check_datagen_scale", lambda cfg: None)
    cfg = _cfg(cfg_path)
    with recording(cfg.get_namespace()) as rec:
        rec.for_config(cfg)
        res = CliRunner().invoke(app, ["deploy", str(cfg_path), "--yes", "--require-new"])
    assert res.exit_code == 3, res.output


def test_deploy_require_new_is_hidden():
    from lakebench.cli import app

    res = CliRunner().invoke(app, ["deploy", "--help"])
    assert res.exit_code == 0 and "--require-new" not in res.output


@pytest.mark.parametrize("other", ["--local", "--force-legacy"])
def test_deploy_require_new_refuses_contradicting_flags(cfg_path, other):
    from lakebench.cli import app

    with recording() as rec:
        res = CliRunner().invoke(app, ["deploy", str(cfg_path), "--yes", "--require-new", other])
        assert rec.calls == []
    assert res.exit_code == 2, res.output
    assert "--require-new does not combine" in res.output


def test_expect_incarnation_mismatch_says_the_expectation_was_not_met(cfg_path, monkeypatch):
    """With the flag the refusal names the caller's expectation, not a
    redeploy after a check this command never made."""
    from lakebench.cli import app

    class Engine:
        def __init__(self, cfg, **_):
            pass

        def destroy_all(self, **kw):
            return [
                DeploymentResult(
                    component="ownership-check",
                    status=DeploymentStatus.FAILED,
                    message="Destroy NOT started",
                    details={"incarnation_mismatch": True, "expected": "u#x", "found": "u#y"},
                )
            ]

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)
    res = CliRunner().invoke(
        app, ["destroy", str(cfg_path), "--yes", "--expect-incarnation", "u#x"]
    )
    assert res.exit_code == 3, res.output
    assert "not the incarnation the caller expected" in res.output
    assert "redeployed after this command checked it" not in res.output
