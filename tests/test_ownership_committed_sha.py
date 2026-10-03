"""The namespace's committed-sha stamp names the code that deployed.

``build_identity_from_config`` ran ``git rev-parse HEAD`` in the working
directory, so a deploy from one checkout with the shell inside another
repository stamped that other repository's commit. The stamp now comes from
the lakebench package's own checkout, the same reading as the run record's
``provenance.git_sha``, and is None outside a checkout.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from lakebench.deploy import ownership
from lakebench.metrics import provenance


def _cfg() -> SimpleNamespace:
    workload = SimpleNamespace(schema_type=SimpleNamespace(value="customer360"))
    return SimpleNamespace(name="lb-sha", architecture=SimpleNamespace(workload=workload))


def _git(cwd: Path, *args: str) -> str:
    env_args = [
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.invalid",
        "-c",
        "commit.gpgsign=false",
    ]
    out = subprocess.run(
        ["git", *env_args, *args], cwd=cwd, capture_output=True, text=True, check=True
    )
    return out.stdout.strip()


def test_stamp_is_the_package_checkout_not_the_working_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    for var in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR"):
        monkeypatch.delenv(var, raising=False)
    other = tmp_path / "other-repo"
    other.mkdir()
    _git(other, "init", "-q")
    (other / "f").write_text("x\n")
    _git(other, "add", "f")
    _git(other, "commit", "-q", "-m", "other")
    other_head = _git(other, "rev-parse", "HEAD")
    monkeypatch.chdir(other)

    code = provenance.sample()
    expected = None
    if code["install"] == provenance.INSTALL_CHECKOUT:
        expected = code["git_sha"][:7] + ("" if code["git_dirty"] is False else "-dirty")

    identity = ownership.build_identity_from_config(_cfg())
    assert identity.committed_sha != other_head[:7]
    assert identity.committed_sha == expected


@pytest.mark.parametrize(
    "code",
    [
        {"install": provenance.INSTALL_WHEEL, "git_sha": "a" * 40},
        {"install": provenance.INSTALL_UNKNOWN, "git_sha": None},
    ],
    ids=["wheel", "unknown"],
)
def test_no_stamp_outside_a_checkout(code, tmp_path, monkeypatch):
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(provenance, "sample", lambda: dict(code))
    assert ownership.build_identity_from_config(_cfg()).committed_sha is None


def test_a_failed_code_read_stamps_nothing(tmp_path, monkeypatch):
    def boom() -> dict:
        raise OSError("unreadable package")

    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(provenance, "sample", boom)
    identity = ownership.build_identity_from_config(_cfg())
    assert identity.committed_sha is None
    assert identity.name == "lb-sha"


def test_a_modified_checkout_is_marked_dirty(tmp_path, monkeypatch):
    monkeypatch.setenv("KUBECONFIG", "/nonexistent")
    monkeypatch.chdir(tmp_path)
    code = {"install": provenance.INSTALL_CHECKOUT, "git_sha": "b" * 40}
    monkeypatch.setattr(provenance, "sample", lambda: {**code, "git_dirty": True})
    assert ownership.build_identity_from_config(_cfg()).committed_sha == "bbbbbbb-dirty"
    monkeypatch.setattr(provenance, "sample", lambda: {**code, "git_dirty": None})
    assert ownership.build_identity_from_config(_cfg()).committed_sha == "bbbbbbb-dirty"
    monkeypatch.setattr(provenance, "sample", lambda: {**code, "git_dirty": False})
    assert ownership.build_identity_from_config(_cfg()).committed_sha == "bbbbbbb"
