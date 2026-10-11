"""SAF-1 (CC-4): a harness destroys only what it created.

``deploy --require-new`` (hidden) refuses an existing namespace or bucket
instead of adopting it (exit 3, ``deploy.existing_namespace``), and
``destroy --expect-incarnation`` (hidden) only adds a refusal. Cluster and S3
calls go to the recording fake (tests/fixtures/recording_k8s.py), so "no
delete of any kind" is checked on every call.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from lakebench.deploy.engine import DeploymentResult, DeploymentStatus
from lakebench.deploy.ownership import ANNOTATION_DEPLOY_NONCE, ANNOTATION_DEPLOYMENT_NAME
from lakebench.exit_codes import REFUSAL_DETAIL
from tests.fixtures.recording_k8s import recording

NAME = "lbreqnew"
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
    p = tmp_path / "reqnew.yaml"
    p.write_text(CFG)
    return p


def _cfg(path: Path):
    from lakebench.config import LoadPurpose, load_config

    return load_config(path, purpose=LoadPurpose.RUN)


class FakeEngine:
    """Stands in for DeploymentEngine; the real _deploy_impl runs around it."""

    def __init__(self, cfg: Any, dry_run: bool = False, require_new: bool = False, **_: Any):
        self.config = cfg
        self.dry_run = dry_run
        self.require_new = require_new
        self.results: list[DeploymentResult] = []
        self.deploy_nonce: str | None = None


# ---------------------------------------------------------------------------
# The engine under require_new: refuse, never adopt
# ---------------------------------------------------------------------------


def _engine(require_new: bool = True, exists: bool = False):
    from lakebench.deploy.engine import DeploymentEngine
    from tests.fixtures.deploy_helpers import _make_config, _mock_k8s

    k8s = _mock_k8s()
    k8s.namespace_exists.return_value = exists
    with patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False):
        eng = DeploymentEngine(_make_config(), k8s_client=k8s, require_new=require_new)
    return eng, k8s


@pytest.mark.parametrize(
    ("created_this_run", "annotation"),
    [
        (False, None),  # an existing namespace
        (True, "theirs"),  # a seeded name carrying another nonce
        # The nonce alone is not proof: the namespace must be one this engine
        # tried to create.
        (False, "mine"),
    ],
    ids=["existing", "seeded-other-nonce", "matching-nonce-not-created"],
)
def test_engine_require_new_refuses_a_namespace_it_did_not_create(created_this_run, annotation):
    eng, k8s = _engine(exists=True)
    eng.deploy_nonce = "mine"
    if created_this_run:
        eng._namespace_created_this_run.add(eng.config.get_namespace())
    k8s.get_namespace_annotation.return_value = annotation
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
    ):
        result = eng._deploy_namespace()
    assert result.status is DeploymentStatus.FAILED
    assert result.details[REFUSAL_DETAIL] == "deploy.existing_namespace"
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
    assert result.details[REFUSAL_DETAIL] == "deploy.existing_namespace"
    k8s.apply_manifest.assert_not_called()
    stamp.assert_not_called()
    nonce.assert_not_called()
    assert eng.config.get_namespace() not in eng._namespace_created_this_run


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
    with (
        patch("lakebench.k8s.get_k8s_client"),
        patch("kubernetes.client.CoreV1Api"),
        # Deploy stamps buckets with this cluster's fingerprint (SD-18).
        patch("lakebench.deploy.ownership.api_server_fingerprint", return_value="fp-test"),
    ):
        result = eng._deploy_buckets()
    assert result.status is DeploymentStatus.FAILED
    assert result.details[REFUSAL_DETAIL] == "deploy.existing_namespace"
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


@pytest.mark.parametrize("token", ["", "abc", "#n", "u#", "u#n#m"])
def test_destroy_expect_incarnation_malformed_is_a_usage_error(cfg_path, token):
    from lakebench.cli import app

    with recording() as rec:
        res = CliRunner().invoke(
            app, ["destroy", str(cfg_path), "--yes", "--expect-incarnation", token]
        )
        assert rec.calls == []
    assert res.exit_code == 2, res.output


def test_destroy_expect_incarnation_refuses_local(cfg_path):
    from lakebench.cli import app

    res = CliRunner().invoke(
        app, ["destroy", str(cfg_path), "--yes", "--local", "--expect-incarnation", "u#n"]
    )
    assert res.exit_code == 2, res.output


def test_destroy_expect_incarnation_must_equal_the_nameless_check(tmp_path, monkeypatch):
    """A nameless config's verified incarnation and the flag must agree."""
    import lakebench.cli._nameless as nameless
    from lakebench.cli import app
    from tests.fixtures import saf2_deploy_state_helpers as t

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


def test_engine_stamps_the_recorded_nonce_not_a_fresh_one():
    """The real namespace step writes engine.deploy_nonce (the one deploy
    recorded); --expect-incarnation and the nameless checks depend on it."""
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
    assert result.details[REFUSAL_DETAIL] == "deploy.existing_namespace"
    assert "lakebench-gold" in result.message
    client.ensure_buckets.assert_not_called()
    client.create_bucket.assert_not_called()
    record.assert_not_called()


def test_engine_require_new_refuses_preprovisioned_buckets():
    eng, _ = _engine()
    eng.config.platform.storage.s3.create_buckets = False
    with patch.object(eng, "_record_preprovisioned_empty_buckets") as adopt:
        result = eng._deploy_buckets()
    assert result.details[REFUSAL_DETAIL] == "deploy.existing_namespace"
    adopt.assert_not_called()


# ---------------------------------------------------------------------------
# deploy --require-new (hidden): the harness's deploy refuses, never adopts
# ---------------------------------------------------------------------------


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
                    details={REFUSAL_DETAIL: "deploy.existing_namespace"},
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
