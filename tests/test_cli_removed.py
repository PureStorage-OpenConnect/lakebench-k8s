"""SAF-3 and CLI-8 (CC-5): `config upgrade` refuses; three dead flags are gone."""

from __future__ import annotations

import ast
import hashlib
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from lakebench.cli import app

SRC = Path(__file__).resolve().parents[1] / "src" / "lakebench"
SENTINEL = "SENTINEL-SECRET-0f3c9a7e"


def _stderr(result) -> str:
    try:
        return result.stderr
    except ValueError:  # Click < 8.2 without mix_stderr=False
        return result.output


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    "extra",
    [[], ["-o", "NEW.yaml"], ["--output", "NEW.yaml"]],
    ids=["in-place", "short-output", "long-output"],
)
def test_config_upgrade_refused(tmp_path, monkeypatch, extra):
    monkeypatch.chdir(tmp_path)
    cfg = tmp_path / "old-config.yaml"
    body = (
        "name: upgrade-me\n"
        "platform:\n  storage:\n    s3:\n"
        "      endpoint: http://10.0.1.50\n"
        "      access_key: ${LAKEBENCH_S3_ACCESS_KEY}\n"
        f"      secret_key: {SENTINEL}\n"
    )
    cfg.write_text(body)
    before = _sha(cfg)

    result = CliRunner().invoke(app, ["config", "upgrade", str(cfg), *extra])

    assert result.exit_code == 2, result.output
    assert _sha(cfg) == before
    assert not (tmp_path / "NEW.yaml").exists()
    out = result.output + _stderr(result)
    assert SENTINEL not in out
    assert "upgrade-me" not in out
    assert str(cfg) not in out and "old-config" not in out
    assert "ERROR" in out and "config upgrade` is removed" in out
    assert "lakebench init --from OLD.yaml -o NEW.yaml" in out


def test_config_upgrade_missing_path_reaches_the_refusal(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    missing = tmp_path / "does-not-exist-7d1.yaml"
    result = CliRunner().invoke(app, ["config", "upgrade", str(missing)])
    assert result.exit_code == 2
    out = result.output + _stderr(result)
    assert "does-not-exist-7d1" not in out
    assert "config upgrade` is removed" in out


def test_config_upgrade_hidden_from_help():
    result = CliRunner().invoke(app, ["config", "--help"])
    assert result.exit_code == 0
    assert "upgrade" not in result.output


@pytest.mark.parametrize(
    "argv",
    [
        ["generate", "--wait"],
        ["generate", "-w"],
        ["admin", "release-lock", "--expired-only"],
        ["deploy", "--include-observability"],
    ],
    ids=["generate-wait", "generate-w", "release-lock-expired-only", "deploy-observability"],
)
def test_dead_flags_unknown(argv, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KUBECONFIG", "/nonexistent/kubeconfig")
    result = CliRunner().invoke(app, argv)
    assert result.exit_code == 2, result.output
    assert "No such option" in result.output + _stderr(result)


def test_release_lock_releases_only_expired_by_default():
    with (
        patch("lakebench.cli._admin._get_core_v1"),
        patch("lakebench.deploy.cluster_lock.force_release_cluster_lock", return_value=None) as frc,
    ):
        result = CliRunner().invoke(app, ["admin", "release-lock"])
    assert result.exit_code == 0, result.output
    assert frc.call_args.kwargs["expired_only"] is True


def _calls_with_keyword(func_names: set[str], keyword: str) -> list[str]:
    hits = []
    for path in SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            if name in func_names and any(k.arg == keyword for k in node.keywords):
                hits.append(f"{path.relative_to(SRC)}:{node.lineno}")
    return hits


def test_no_internal_caller_passes_wait_to_generate():
    """`generate` lost its `wait` parameter; a caller passing it raises TypeError."""
    import inspect

    from lakebench.cli._generate import generate

    assert "wait" not in inspect.signature(generate).parameters
    assert not _calls_with_keyword({"_generate_cmd", "generate"}, "wait")


def test_no_internal_caller_passes_include_observability_to_deploy():
    import inspect

    from lakebench.cli._deploy import deploy

    assert "include_observability" not in inspect.signature(deploy).parameters
    assert not _calls_with_keyword({"_deploy_cmd", "deploy"}, "include_observability")


def test_no_hint_names_a_removed_flag():
    stale = ("--expired-only", "--include-observability", "generate --wait")
    hits = [
        f"{p.relative_to(SRC)}: {s}"
        for p in SRC.rglob("*.py")
        for s in stale
        if s in p.read_text(encoding="utf-8")
    ]
    assert not hits
