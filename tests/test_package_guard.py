"""scripts/package_guard.py on planted artifacts, plus one real build.

Planted values are built at run time, so this file carries no key-shaped
string and no local-only path for the repository's own scans to find.
"""

from __future__ import annotations

import io
import tarfile
import zipfile
from pathlib import Path

import pytest

from tests.conftest import exec_repo_script

ROOT = Path(__file__).resolve().parents[1]
pg = exec_repo_script(ROOT / "scripts/package_guard.py", "package_guard")

# A FlashBlade-shaped access key id from a fixed fake alphabet.
FAKE_PSFB = "PSFB" + "QWERTYUIOPASDFGHJKLZXCVBNMQWERTYUIOPAS"


def _sdist(path: Path, members: dict[str, str]) -> Path:
    with tarfile.open(path, "w:gz") as t:
        for name, body in members.items():
            data = body.encode()
            info = tarfile.TarInfo(f"lakebench_k8s-9.9.9/{name}")
            info.size = len(data)
            t.addfile(info, io.BytesIO(data))
    return path


@pytest.mark.parametrize(
    "member",
    [
        "sub/dir/CLAUDE" + ".md",
        "../escape.txt",
    ],
)
def test_planted_member_name_fails(member):
    found = pg.check_names([member, "lakebench/__init__.py"])
    assert len(found) == 1 and found[0].status == pg.FAIL and found[0].detail.startswith(member)


def test_planted_docs_internal_member_in_an_sdist_fails(tmp_path):
    sdist = _sdist(tmp_path / "x.tar.gz", {"docs/internal/x.md": "hi", "README.md": "ok"})
    members = pg.sdist_members(sdist)
    assert set(members) == {"docs/internal/x.md", "README.md"}  # prefix stripped
    assert [f.detail for f in pg.check_names(members)] == [
        "docs/internal/x.md: docs/internal must not ship"
    ]


def test_planted_key_pattern_fails_without_printing_it():
    found = pg.check_content({"w.whl/lakebench/x.yaml": f"ok\nkey = {FAKE_PSFB}\n".encode()})
    assert [f.detail for f in found] == ["w.whl/lakebench/x.yaml:2: flashblade-access-key"]
    assert all(f.status == pg.FAIL for f in found)
    assert FAKE_PSFB not in " ".join(f.render() for f in found)


def test_link_member_in_an_sdist_fails(tmp_path):
    path = tmp_path / "x.tar.gz"
    with tarfile.open(path, "w:gz") as t:
        info = tarfile.TarInfo("lakebench_k8s-9.9.9/evil")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        t.addfile(info)
    members = pg.sdist_members(path)
    links = [n for n, d in members.items() if d == pg.LINK_MARK]
    assert [f.detail for f in pg.check_names(members, links)] == [
        "evil: a link member must not ship"
    ]
    assert pg.check_content(members) == []


@pytest.mark.parametrize(
    ("gitleaks", "baseline", "status"),
    [
        (None, False, "SKIP"),  # no gitleaks: SKIP, never a silent pass
        ("/bin/true", False, "FAIL"),  # an empty tree scanned nothing
        ("/bin/true", True, "FAIL"),  # a .gitleaksignore at the scan root would be honoured
    ],
)
def test_gitleaks_check_fails_closed(tmp_path, monkeypatch, gitleaks, baseline, status):
    monkeypatch.setattr(pg.shutil, "which", lambda name: gitleaks)
    if baseline:
        (tmp_path / ".gitleaksignore").write_text("x\n")
    assert [f.status for f in pg.check_gitleaks(tmp_path)] == [getattr(pg, status)]


def _absence(mode, problems):
    return lambda texts: (mode, problems(texts))


@pytest.mark.parametrize(("mode", "status"), [("report", pg.PENDING), ("enforce", pg.FAIL)])
def test_planted_heldout_token_follows_the_files_mode(tmp_path, mode, status):
    hashes = tmp_path / "heldout_hashes.json"
    hashes.write_text("{}")
    seen = {}

    def problems(texts):
        seen.update(texts)
        return [
            f"{k}: an integer token hashes to a held-out evaluation seed"
            for k in texts
            if "777" in texts[k]
        ]

    members = {
        "w.whl/lakebench/a.py": b"x = 777\n",
        "w.whl/lakebench/b.bin": b"\x00777",
        "s/c.md": b"clean",
    }
    found = pg.check_heldout(members, _absence(mode, problems), hashes)
    assert [(f.status, f.detail.split(":")[0]) for f in found] == [(status, "w.whl/lakebench/a.py")]
    # Text members, and the member names as one more text.
    assert set(seen) == {"w.whl/lakebench/a.py", "s/c.md", "<member names>"}
    assert "w.whl/lakebench/b.bin" in seen["<member names>"]
    assert pg.exit_code(found) == (1 if status == pg.FAIL else 0)
    assert pg.exit_code(found, require_all=True) == 1


def _raising(exc):
    def load(path=None):
        raise exc

    return load


@pytest.mark.parametrize(
    ("hash_text", "ds_patch", "absence", "status", "detail"),
    [
        (None, None, None, "SKIP", "held-out check is off"),
        ("{}", None, "broken", "FAIL", "no absence check"),
        ("{}", "no_absence_fn", None, "FAIL", "no absence check"),
        (
            '{"absence_check": "report"}',
            _raising(RuntimeError("compiled held-out floor is not initialised")),
            None,
            "PENDING",
            "cannot be loaded yet",
        ),
        (
            '{"absence_check": "enforce"}',
            _raising(ValueError("role hash is not hex")),
            None,
            "FAIL",
            "do not load",
        ),
        ("not json", _raising(ValueError("x")), None, "FAIL", ""),
    ],
    ids=[
        "no-hash-file-is-off",
        "absence-check-raises",
        "no-absence-function",
        "report-mode-cannot-load-is-pending",
        "enforce-mode-cannot-load-fails",
        "unparseable-hash-file-fails",
    ],
)
def test_heldout_check_states(tmp_path, monkeypatch, hash_text, ds_patch, absence, status, detail):
    from lakebench.config import datagen_seed as ds

    hashes = tmp_path / "heldout_hashes.json"
    if hash_text is not None:
        hashes.write_text(hash_text)
    monkeypatch.setattr(pg, "HELDOUT_HASHES", hashes)
    if ds_patch == "no_absence_fn":
        monkeypatch.delattr(ds, "absence_problems", raising=False)
    elif ds_patch is not None:
        monkeypatch.setattr(ds, "load_heldout", ds_patch, raising=False)
        monkeypatch.setattr(ds, "absence_problems", lambda texts, held=None: [], raising=False)
    check = None
    if absence == "broken":

        def check(texts):
            raise RuntimeError("no absence check")

    (found,) = pg.check_heldout({"a": b"1"}, check, hashes)
    assert found.status == getattr(pg, status) and detail in found.detail


def test_symlink_in_a_wheel_fails(tmp_path):
    path = tmp_path / "x.whl"
    with zipfile.ZipFile(path, "w") as z:
        info = zipfile.ZipInfo("lakebench/evil")
        info.external_attr = 0o120777 << 16
        z.writestr(info, "/etc/passwd")
        z.writestr("lakebench/ok.py", "x = 1\n")
    members = pg.wheel_members(path)
    assert members["lakebench/evil"] == pg.LINK_MARK
    links = [n for n, d in members.items() if d == pg.LINK_MARK]
    assert [f.detail for f in pg.check_names(members, links)] == [
        "lakebench/evil: a link member must not ship"
    ]


def test_planted_heldout_token_with_the_datagen_fixture(monkeypatch):
    from lakebench.config import datagen_seed as ds
    from tests.fixtures import heldout_test_seeds as ts

    held = ts.use_fixture(monkeypatch)
    texts = {
        "lakebench/x.py": f"seed = {ts.TEST_EVALUATION_SEED}\n",
        "lakebench/y.py": "seed = 43\n",
    }
    problems = ds.absence_problems(texts, held, exclude=[])
    assert [p.split(":")[0] for p in problems] == ["lakebench/x.py"]
    assert str(ts.TEST_EVALUATION_SEED) not in " ".join(problems)


@pytest.mark.slow
def test_real_build_passes_non_seed_checks(tmp_path):
    pytest.importorskip("build")
    pytest.importorskip("hatchling")
    dist = tmp_path / "dist"
    pg.build(dist)
    found = pg.guard(dist, tmp_path / "work")
    assert [f for f in found if f.status == pg.FAIL] == [], [f.render() for f in found]
    by = {f.check: f for f in found}
    assert by["names"].status == by["content"].status == pg.PASS
