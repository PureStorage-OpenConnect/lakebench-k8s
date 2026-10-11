"""lb_deps.py: the stdlib resolver, verifier and server of DEP-2 (ch01 s2.3-2.6).

A fake ``spark-submit`` and a fake ``pip`` stand in for the stock image's
tools; a local HTTP server stands in for the DuckDB extension repository.
The real tools on the stock images were exercised by the SD-3 offline run
(four release-matrix rows) and the SD-1 live spike.
"""

from __future__ import annotations

import http.server
import json
import os
import signal
import threading
from pathlib import Path

import pytest

from lakebench.deploy.deps_tools import lb_deps
from lakebench.deps import request as req_mod
from tests.fixtures.lb_deps_helpers import BUNDLE as BUNDLE
from tests.fixtures.lb_deps_helpers import HADOOP as HADOOP
from tests.fixtures.lb_deps_helpers import Env as Env
from tests.fixtures.lb_deps_helpers import _zip_bytes as _zip_bytes


@pytest.fixture
def env(tmp_path, monkeypatch):
    return Env(tmp_path, monkeypatch)


@pytest.fixture(autouse=True)
def _restore_signal_handlers():
    """``resolve`` installs a SIGTERM handler for the pod's PID 1; run in this
    process it would outlive the test and take a later test's signal (SD-22's
    lease deferral test got lb_deps.Terminated)."""
    saved = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    yield
    for s, handler in saved.items():
        signal.signal(s, handler)


def _last_error(out: str) -> str:
    lines = [ln for ln in out.splitlines() if ln.startswith("LB_DEPS_ERROR")]
    assert len(lines) == 1, out
    return lines[0]


# --- request guard ----------------------------------------------------------------


def test_request_json_must_match_pod_env(env):
    env.request()
    env.mp.setenv("LB_DEPS_REQUEST_SHA256", "0" * 64)
    assert env.run("resolve", "spark") == lb_deps.EXIT_REQUEST


# --- resolve: jars ------------------------------------------------------------


def test_missing_direct_coordinate_exits_3(env):
    """Today's resolve-deps ends in `|| true`, so this passed silently."""
    r = env.request()
    env.mp.setenv("FAKE_SKIP", BUNDLE)
    assert env.run("resolve", "spark") == lb_deps.EXIT_MISSING
    assert not (env.root / "requests" / f"{r['sha']}.json").exists()


def test_two_iceberg_runtimes_exit_5(env, capsys):
    env.request()
    env.mp.setenv("FAKE_TRANSITIVE", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0")
    assert env.run("resolve", "spark") == lb_deps.EXIT_UX_D2
    assert "iceberg-spark-runtime-" in _last_error(capsys.readouterr().out)


def test_corrupt_jar_exits_4(env, capsys, monkeypatch):
    """A jar a killed copy truncated must not enter a self-consistent set."""
    env.request()
    real = lb_deps.shutil.copyfile

    def truncating(src, dst, *a, **k):
        real(src, dst)
        if "iceberg-aws-bundle" in dst:
            with open(dst, "r+b") as f:
                f.truncate(os.path.getsize(dst) // 2)
        return dst

    monkeypatch.setattr(lb_deps.shutil, "copyfile", truncating)
    assert env.run("resolve", "spark") == lb_deps.EXIT_HASH
    assert "corrupt archive" in _last_error(capsys.readouterr().out)


# --- skip, reuse, recovery and one set -----------------------------------------


def test_pointer_with_a_bad_pinset_never_deletes_the_pvc(env):
    r = env.request()
    (env.root / "keep").write_text("x")
    (env.root / "requests").mkdir()
    (env.root / "requests" / f"{r['sha']}.json").write_text(json.dumps({"pinset_sha256": ".."}))
    assert env.run("resolve", "spark") == 0
    assert (env.root / "keep").exists()
    assert lb_deps.verify_set(env.set_dir(r["sha"]).name) is None


def test_symlink_in_staging_fails(env, capsys, monkeypatch, tmp_path):
    env.request()
    real = lb_deps.resolve_jars
    outside = tmp_path / "outside.jar"
    outside.write_bytes(_zip_bytes({"a": "b"}))

    def plus_link(req, st_set, meta):
        real(req, st_set, meta)
        os.symlink(outside, Path(st_set) / "jars" / "link.jar")

    monkeypatch.setattr(lb_deps, "resolve_jars", plus_link)
    assert env.run("resolve", "spark") == lb_deps.EXIT_HASH
    assert "symlink" in _last_error(capsys.readouterr().out)


def test_verify_set_refuses_unsafe_paths_and_dir_symlinks(env, tmp_path):
    r = env.request()
    assert env.run("resolve", "spark") == 0
    sd = env.set_dir(r["sha"])
    pinset = sd.name
    (tmp_path / "elsewhere").mkdir()
    os.symlink(tmp_path / "elsewhere", sd / "jars" / "sub")
    assert "symlink" in lb_deps.verify_set(pinset)
    (sd / "jars" / "sub").unlink()
    man = json.loads((sd / "manifest.json").read_text())
    man["groups"]["jars"].append({"file": "../manifest.json", "sha256": "0" * 64, "size": 1})
    man["jar_order"].append("../manifest.json")
    man["pinset_sha256"] = req_mod.pinset_sha256(man["groups"], man["jar_order"])
    bad = env.root / "sets" / man["pinset_sha256"]
    sd.rename(bad)
    (bad / "manifest.json").write_text(json.dumps(man))
    assert lb_deps.verify_set(bad.name).startswith("unsafe path")


# --- serve ------------------------------------------------------------------------


def test_realpath_confinement(env, tmp_path):
    """Defence in depth behind verify_set: a symlink planted after the
    start-up check still cannot reach outside the set."""
    r = env.request()
    assert env.run("resolve", "spark") == 0
    sd = env.set_dir(r["sha"])
    (tmp_path / "secret").write_text("x")
    os.symlink(tmp_path / "secret", sd / "jars" / "planted")
    h = lb_deps.SetHandler.__new__(lb_deps.SetHandler)
    h.pinset, h.base, h.directory = sd.name, str(sd), str(sd)
    h.path = f"/sets/{sd.name}/jars/planted"
    assert h._target() is None
    h.path = f"/sets/{sd.name}/jars/{lb_deps.coordinate_jar(HADOOP)}"
    assert h._target() == str(sd / "jars" / lb_deps.coordinate_jar(HADOOP))


# --- show and fetch -------------------------------------------------------------


class _SetServer(http.server.SimpleHTTPRequestHandler):
    """Serves the PVC root; ``short`` truncates the next response once."""

    short: list[str] = []

    def do_GET(self):  # noqa: N802
        if self.short and self.path.endswith(self.short[0]):
            self.short.pop()
            path = self.translate_path(self.path)
            body = open(path, "rb").read()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body[: len(body) // 2])
            self.close_connection = True
            return
        super().do_GET()

    def log_message(self, *a):
        pass


@pytest.fixture
def served_set(env, tmp_path, capsys):
    r = env.request()
    assert env.run("resolve", "spark") == 0
    capsys.readouterr()
    assert env.run("show") == 0
    man = tmp_path / "manifest.json"
    man.write_text(capsys.readouterr().out)
    _SetServer.short = []

    def handler(*a, **k):
        return _SetServer(*a, directory=str(env.root), **k)

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    pinset = json.loads(man.read_text())["pinset_sha256"]
    yield {
        "man": man,
        "url": f"http://127.0.0.1:{srv.server_address[1]}/sets/{pinset}",
        "sha": r["sha"],
    }
    srv.shutdown()


def _fetch(served_set, dest, url=None):
    return lb_deps.main(
        [
            "fetch",
            "--group",
            "jars",
            "--dest",
            str(dest),
            "--manifest",
            str(served_set["man"]),
            "--url",
            url or served_set["url"],
        ]
    )


def test_fetch_mismatch_exits_4_and_leaves_nothing(env, served_set, tmp_path, capsys):
    jar = lb_deps.coordinate_jar(BUNDLE)
    p = env.set_dir(served_set["sha"]) / "jars" / jar
    p.write_bytes(b"s" * p.stat().st_size)  # same length, other bytes
    dest = tmp_path / "out"
    assert _fetch(served_set, dest) == lb_deps.EXIT_HASH
    assert "hash mismatch " + jar in _last_error(capsys.readouterr().out)
    assert not (dest / jar).exists() and not (dest / (jar + ".part")).exists()


def test_fetch_rejects_an_edited_manifest(served_set, tmp_path):
    m = json.loads(served_set["man"].read_text())
    m["groups"]["jars"][0]["sha256"] = "0" * 64
    served_set["man"].write_text(json.dumps(m))
    assert _fetch(served_set, tmp_path / "o") == lb_deps.EXIT_HASH


def test_fetch_refuses_unsafe_paths(served_set, tmp_path):
    m = json.loads(served_set["man"].read_text())
    old = m["groups"]["jars"][0]["file"]
    m["groups"]["jars"][0]["file"] = "../escape.jar"
    m["jar_order"] = ["../escape.jar" if f == old else f for f in m["jar_order"]]
    m["pinset_sha256"] = req_mod.pinset_sha256(m["groups"], m["jar_order"])
    served_set["man"].write_text(json.dumps(m))
    url = served_set["url"].rsplit("/", 1)[0] + "/" + m["pinset_sha256"]
    assert _fetch(served_set, tmp_path / "o", url) == lb_deps.EXIT_HASH
    assert not (tmp_path / "escape.jar").exists()


# --- brief-pass fixes -------------------------------------------------------------


def test_a_reordered_set_on_the_pvc_fails_verification(env, capsys):
    """Editing only the order in a set's manifest is caught: the pinset
    names the order too."""
    r = env.request()
    assert env.run("resolve", "spark") == 0
    sd = env.set_dir(r["sha"])
    man = json.loads((sd / "manifest.json").read_text())
    man["jar_order"] = list(reversed(man["jar_order"]))
    (sd / "manifest.json").write_text(json.dumps(man))
    assert "does not hash" in lb_deps.verify_set(sd.name)
    capsys.readouterr()
    assert env.run("serve", "--port", "0") == lb_deps.EXIT_HASH


def test_fetch_refuses_a_reordered_manifest(served_set, tmp_path):
    m = json.loads(served_set["man"].read_text())
    m["jar_order"] = list(reversed(m["jar_order"]))
    served_set["man"].write_text(json.dumps(m))
    assert _fetch(served_set, tmp_path / "o") == lb_deps.EXIT_HASH
