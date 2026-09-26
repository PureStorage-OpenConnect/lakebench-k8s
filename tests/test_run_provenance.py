"""metrics.json records which lakebench produced the run (GOALS P9.1)."""

from __future__ import annotations

import subprocess
from pathlib import Path

import lakebench
from lakebench.metrics.collector import MetricsCollector
from lakebench.metrics.provenance import run_provenance


def test_provenance_names_version_and_checkout_commit():
    prov = run_provenance()
    assert prov["lakebench_version"] == lakebench.__version__
    pkg_dir = Path(lakebench.__file__).resolve().parent
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=pkg_dir, capture_output=True, text=True, check=False
    )
    if head.returncode == 0:
        assert prov["git_sha"] == head.stdout.strip()
        assert isinstance(prov["git_dirty"], bool)
    else:  # installed without a checkout
        assert prov["git_sha"] is None and prov["git_dirty"] is None


def test_a_new_run_stamps_provenance_into_metrics_json(tmp_path):
    from lakebench.metrics.storage import MetricsStorage

    run = MetricsCollector().start_run("20260926-000000-cccccc", "d", {})
    d = run.to_dict()
    assert d["provenance"]["lakebench_version"] == lakebench.__version__
    assert "git_sha" in d["provenance"]
    storage = MetricsStorage(tmp_path)
    storage.save_run(run)
    assert storage.load_run(run.run_id).provenance == d["provenance"]


def test_old_record_without_provenance_loads_as_none(tmp_path):
    import json

    from lakebench.metrics.storage import MetricsStorage

    storage = MetricsStorage(tmp_path)
    run = MetricsCollector().start_run("20260926-000000-dddddd", "d", {})
    path = storage.save_run(run)
    raw = json.loads(path.read_text())
    del raw["provenance"]
    path.write_text(json.dumps(raw))
    loaded = storage.load_run(run.run_id)
    assert loaded.provenance is None
    assert "provenance" not in loaded.to_dict()  # never invented on re-save


def test_git_outside_a_checkout_gives_none(tmp_path):
    from lakebench.metrics import provenance as prov_mod

    assert prov_mod._git(["rev-parse", "HEAD"], tmp_path / "missing") is None


def test_package_inside_a_foreign_checkout_records_no_sha(tmp_path, monkeypatch):
    """A wheel in a venv under another repo must not record that repo's commit."""
    import shutil

    from lakebench.metrics import provenance as prov_mod

    if shutil.which("git") is None:
        return
    repo = tmp_path / "proj"
    pkg = repo / ".venv" / "site-packages" / "lakebench" / "metrics"
    pkg.mkdir(parents=True)
    (repo / "README").write_text("x")
    env = {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }
    for args in (["init", "-q"], ["add", "README"], ["commit", "-qm", "x"]):
        subprocess.run(["git", *args], cwd=repo, check=True, env={**env, "PATH": "/usr/bin:/bin"})
    (pkg.parent / "__init__.py").write_text("")
    fake = pkg / "provenance.py"
    fake.write_text("")
    monkeypatch.setattr(prov_mod, "__file__", str(fake))
    prov_mod.run_provenance.cache_clear()
    try:
        prov = prov_mod.run_provenance()
    finally:
        prov_mod.run_provenance.cache_clear()
    assert prov["git_sha"] is None and prov["git_dirty"] is None


def test_inherited_git_dir_is_ignored(tmp_path, monkeypatch):
    from lakebench.metrics import provenance as prov_mod

    monkeypatch.setenv("GIT_DIR", str(tmp_path / "nowhere"))
    pkg_dir = Path(lakebench.__file__).resolve().parent
    direct = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=pkg_dir,
        capture_output=True,
        text=True,
        check=False,
        env={k: v for k, v in __import__("os").environ.items() if k != "GIT_DIR"},
    )
    if direct.returncode == 0:
        assert prov_mod._git(["rev-parse", "HEAD"], pkg_dir) == direct.stdout.strip()
