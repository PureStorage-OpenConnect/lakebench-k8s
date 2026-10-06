"""scripts/package_guard.py on planted artifacts, plus one real build.

Planted values are built at run time, so this file carries no key-shaped
string and no local-only path for the repository's own scans to find.
"""

from __future__ import annotations

import base64
import hashlib
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
FAKE_AKIA = "AKIA" + "Z9Y8X7W6V5U4T3S2"
PEM = "-----BEGIN " + "RSA PRIVATE KEY-----"
# High entropy, 40 characters: what the s3-secret-key-assignment rule is for.
FAKE_SECRET = base64.b64encode(hashlib.sha256(b"package-guard").digest()).decode()[:40]


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
        "docs/internal/x.md",
        "dev" + "-artifacts/notes.md",
        "CLAUDE" + ".md",
        "sub/dir/CLAUDE" + ".md",
        "docs/reference/CREDENTIALS.md",
        ".claude/settings.json",
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


def test_ordinary_member_names_pass():
    names = ["lakebench/cli/__init__.py", "docs/internals-of-x.md", "docs/aml-scoring.md"]
    assert pg.check_names(names) == []


@pytest.mark.parametrize(
    ("body", "rule"),
    [
        (f"key = {FAKE_PSFB}\n", "flashblade-access-key"),
        (f"aws: {FAKE_AKIA}\n", "aws-access-key"),
        (f"{PEM}\nMIIE\n", "private-key"),
        (f'secret_key: "{FAKE_SECRET}"\n', "gitleaks rule s3-secret-key-assignment"),
    ],
)
def test_planted_key_pattern_fails_without_printing_it(body, rule):
    found = pg.check_content({"w.whl/lakebench/x.yaml": ("ok\n" + body).encode()})
    assert [f.detail for f in found] == [f"w.whl/lakebench/x.yaml:2: {rule}"]
    assert all(f.status == pg.FAIL for f in found)
    for secret in (FAKE_PSFB, FAKE_AKIA, FAKE_SECRET):
        assert secret not in " ".join(f.render() for f in found)


def test_allowlisted_and_low_entropy_values_pass():
    members = {
        # .gitleaks.toml allowlists this fixed local-only value.
        "a.yaml": b"secret_key: 0123456789abcdef0123456789abcdef\n",
        "b.yaml": b"secret_key: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n",
    }
    assert pg.check_content(members) == []


def test_a_value_on_the_line_after_its_key_is_found():
    found = pg.check_content({"m.yaml": f"x: 1\nsecret_key:\n  {FAKE_SECRET}\n".encode()})
    assert [f.detail for f in found] == ["m.yaml:2: gitleaks rule s3-secret-key-assignment"]


def test_binary_member_fails_unscanned():
    found = pg.check_content({"c.bin": b"\0" + FAKE_PSFB.encode()})
    assert [f.detail for f in found] == ["c.bin: binary member, not scanned"]


def test_a_rule_python_cannot_compile_fails(tmp_path):
    cfg = tmp_path / "gitleaks.toml"
    cfg.write_text("[[rules]]\nid = 'bad'\nregex = 'a\\z'\n")
    found = pg.check_content({"a.txt": b"ok"}, cfg)
    assert [f.status for f in found] == [pg.FAIL] and "rule bad" in found[0].detail


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


def test_gitleaks_runs_on_the_extracted_members_or_skips(tmp_path, monkeypatch):
    monkeypatch.setattr(pg.shutil, "which", lambda name: None)
    assert [f.status for f in pg.check_gitleaks(tmp_path)] == [pg.SKIP]


def test_gitleaks_ignores_inline_allow_comments_and_names_the_member(tmp_path):
    if pg.shutil.which("gitleaks") is None:
        pytest.skip("gitleaks is not on PATH")
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "x.yaml").write_text(f"ok: 1\naccess_key: {FAKE_PSFB}  # gitleaks:allow\n")
    (found,) = pg.check_gitleaks(tmp_path)
    assert found.status == pg.FAIL
    assert found.detail == "pkg/x.yaml:2: pure-flashblade-s3-access-key"


def test_gitleaks_on_an_empty_tree_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(pg.shutil, "which", lambda name: "/bin/true")
    (found,) = pg.check_gitleaks(tmp_path)
    assert found.status == pg.FAIL and "no files" in found.detail


def _absence(mode, problems):
    return lambda texts: (mode, problems(texts))


def test_heldout_check_is_off_without_the_hash_file(tmp_path):
    (found,) = pg.check_heldout({"a": b"1"}, hashes=tmp_path / "heldout_hashes.json")
    assert found.status == pg.SKIP


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


def test_heldout_check_that_cannot_run_fails(tmp_path):
    hashes = tmp_path / "heldout_hashes.json"
    hashes.write_text("{}")

    def broken(texts):
        raise RuntimeError("no absence check")

    (found,) = pg.check_heldout({"a": b"1"}, broken, hashes)
    assert found.status == pg.FAIL and "no absence check" in found.detail


def test_hash_file_that_cannot_load_yet_is_pending(tmp_path, monkeypatch):
    # The datagen lane ships the hash file before the maintainers' commit
    # writes its compiled floor; until then loading it raises.
    from lakebench.config import datagen_seed as ds

    hashes = tmp_path / "heldout_hashes.json"
    hashes.write_text('{"absence_check": "report"}')
    monkeypatch.setattr(pg, "HELDOUT_HASHES", hashes)

    def not_yet(path=None):
        raise RuntimeError("compiled held-out floor is not initialised")

    monkeypatch.setattr(ds, "load_heldout", not_yet, raising=False)
    monkeypatch.setattr(ds, "absence_problems", lambda texts, held=None: [], raising=False)
    (found,) = pg.check_heldout({"a": b"1"}, hashes=hashes)
    assert found.status == pg.PENDING and "cannot be loaded yet" in found.detail


def test_hash_file_marked_enforce_that_does_not_load_fails(tmp_path, monkeypatch):
    from lakebench.config import datagen_seed as ds

    hashes = tmp_path / "heldout_hashes.json"
    hashes.write_text('{"absence_check": "enforce"}')
    monkeypatch.setattr(pg, "HELDOUT_HASHES", hashes)

    def broken(path=None):
        raise ValueError("role hash is not hex")

    monkeypatch.setattr(ds, "load_heldout", broken, raising=False)
    monkeypatch.setattr(ds, "absence_problems", lambda texts, held=None: [], raising=False)
    (found,) = pg.check_heldout({"a": b"1"}, hashes=hashes)
    assert found.status == pg.FAIL and "do not load" in found.detail
    hashes.write_text("not json")
    (found,) = pg.check_heldout({"a": b"1"}, hashes=hashes)
    assert found.status == pg.FAIL


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


def test_a_path_only_rule_is_left_to_gitleaks(tmp_path):
    cfg = tmp_path / "gitleaks.toml"
    cfg.write_text("[[rules]]\nid = 'no-pem'\npath = '[.]pem$'\n")
    assert pg.check_content({"a.txt": b"ok"}, cfg) == []


def test_a_baseline_at_the_scan_root_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(pg.shutil, "which", lambda name: "/bin/true")
    (tmp_path / ".gitleaksignore").write_text("x\n")
    (found,) = pg.check_gitleaks(tmp_path)
    assert found.status == pg.FAIL and "would be honoured" in found.detail


def test_hash_file_without_an_absence_check_fails(tmp_path, monkeypatch):
    from lakebench.config import datagen_seed as ds

    hashes = tmp_path / "heldout_hashes.json"
    hashes.write_text("{}")
    monkeypatch.delattr(ds, "absence_problems", raising=False)
    (found,) = pg.check_heldout({"a": b"1"}, hashes=hashes)
    assert found.status == pg.FAIL and "no absence check" in found.detail


def test_planted_heldout_token_with_the_datagen_fixture(monkeypatch):
    # Runs once the held-out hash file and its test fixture are in the tree.
    ts = pytest.importorskip("tests.fixtures.heldout_test_seeds")
    from lakebench.config import datagen_seed as ds

    held = ts.use_fixture(monkeypatch)
    texts = {
        "lakebench/x.py": f"seed = {ts.TEST_EVALUATION_SEED}\n",
        "lakebench/y.py": "seed = 43\n",
    }
    problems = ds.absence_problems(texts, held, exclude=[])
    assert [p.split(":")[0] for p in problems] == ["lakebench/x.py"]
    assert str(ts.TEST_EVALUATION_SEED) not in " ".join(problems)


def test_require_all_counts_skips():
    found = [pg.Finding(pg.PASS, "names", ""), pg.Finding(pg.SKIP, "gitleaks", "")]
    assert pg.exit_code(found) == 0 and pg.exit_code(found, require_all=True) == 1


def test_missing_artifact_fails(tmp_path):
    (found,) = pg.guard(tmp_path, tmp_path / "work")
    assert found.status == pg.FAIL and "needs a wheel and an sdist" in found.detail


def test_real_build_passes_non_seed_checks(tmp_path):
    pytest.importorskip("build")
    pytest.importorskip("hatchling")
    dist = tmp_path / "dist"
    pg.build(dist)
    found = pg.guard(dist, tmp_path / "work")
    assert [f for f in found if f.status == pg.FAIL] == [], [f.render() for f in found]
    by = {f.check: f for f in found}
    assert by["names"].status == by["content"].status == pg.PASS
    # The script maps were rendered from the wheel and scanned with it.
    assert int(by["names"].detail.split()[0]) > 100


def test_release_gate_check_maps_pending_to_skip(monkeypatch):
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location(
        "release_gate", ROOT / "scripts" / "release_gate.py"
    )
    rg = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "release_gate", rg)
    spec.loader.exec_module(rg)
    real = rg._load_script

    def fake(name):
        mod = real(name)
        if name == "package_guard":
            mod.build = lambda outdir: None
            mod.guard = lambda dist, work: findings
        return mod

    monkeypatch.setattr(rg, "_load_script", fake)
    findings = [pg.Finding(pg.PASS, "names", "x"), pg.Finding(pg.PENDING, "heldout", "m: hit")]
    assert rg.check_package_guard().status == rg.SKIP
    findings = [pg.Finding(pg.FAIL, "content", "m:1: aws-access-key")]
    res = rg.check_package_guard()
    assert res.status == rg.FAIL and "aws-access-key" in res.detail
    findings = [pg.Finding(pg.PASS, "names", "x")]
    assert rg.check_package_guard().status == rg.PASS
