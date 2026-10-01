"""install.sh against a local release server (OSS-5).

The script is run with bash, with ``uname`` stubbed on PATH and
``LB_INSTALL_BASE_URL`` pointing at an ``http.server`` in a thread that
serves the release assets. Outbound proxies point at a closed port, so a
download that escapes the override fails instead of reaching GitHub.
"""

from __future__ import annotations

import hashlib
import http.server
import os
import shutil
import stat
import subprocess
import sys
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INSTALL_SH = ROOT / "install.sh"
TAG = "v1.7.0"
FAKE_BINARY = b'#!/bin/sh\necho "lakebench fake 9.9.9"\n'

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None or shutil.which("curl") is None,
    reason="install.sh needs bash and curl",
)


@dataclass
class Release:
    """What the stub server serves under /dl/<tag>/, and what it was asked for."""

    assets: dict[str, bytes] = field(default_factory=dict)
    truncate: set[str] = field(default_factory=set)
    status: dict[str, int] = field(default_factory=dict)  # asset -> HTTP error, 0 = drop
    tag: str = TAG
    requests: list[str] = field(default_factory=list)


def _handler(release: Release) -> type[http.server.BaseHTTPRequestHandler]:
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 (http.server API)
            release.requests.append(self.path)
            prefix = f"/dl/{release.tag}/"
            name = self.path[len(prefix) :] if self.path.startswith(prefix) else ""
            if name in release.status:
                if release.status[name] == 0:  # drop the connection, no response
                    self.close_connection = True
                    return
                self.send_error(release.status[name])
                return
            body = release.assets.get(name)
            if body is None:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if name in release.truncate:
                # Half the body, then the connection closes: curl sees fewer
                # bytes than Content-Length promised.
                self.wfile.write(body[: len(body) // 2])
                self.wfile.flush()
                self.close_connection = True
                return
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            pass

    return Handler


@pytest.fixture
def release() -> Iterator[tuple[Release, str]]:
    rel = Release()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _handler(rel))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield rel, f"http://127.0.0.1:{server.server_address[1]}/dl"
    finally:
        server.shutdown()
        server.server_close()


def _sums(entries: dict[str, bytes], *, binary_mode: bool = False) -> bytes:
    sep = " *" if binary_mode else "  "
    lines = [f"{hashlib.sha256(body).hexdigest()}{sep}{name}" for name, body in entries.items()]
    return ("\n".join(lines) + "\n").encode()


def _write_exe(path: Path, text: str) -> None:
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


@dataclass
class Env:
    install_dir: Path
    tmpdir: Path
    vars: dict[str, str]


@pytest.fixture
def make_env(tmp_path: Path, release: tuple[Release, str]):
    _, base_url = release

    def make(
        os_name: str = "Linux",
        machine: str = "x86_64",
        decoy: bool = False,
        minimal_path: bool = False,
    ) -> Env:
        stubs = tmp_path / "stubs"
        stubs.mkdir(exist_ok=True)
        _write_exe(
            stubs / "uname",
            f'#!/bin/sh\ncase "$1" in\n  -s) echo {os_name} ;;\n  -m) echo {machine} ;;\n'
            f"  *) echo {os_name} ;;\nesac\n",
        )
        path = f"{stubs}{os.pathsep}{os.environ.get('PATH', '/usr/bin:/bin')}"
        if minimal_path:
            # Only the tools install.sh calls, with shasum in place of
            # sha256sum, as on a macOS without coreutils.
            for tool in _SCRIPT_TOOLS:
                found = shutil.which(tool)
                assert found, tool
                (stubs / tool).symlink_to(found)
            _write_exe(stubs / "shasum", _SHASUM_STUB.format(python=sys.executable))
            path = str(stubs)
        if decoy:
            decoys = tmp_path / "decoy"
            decoys.mkdir(exist_ok=True)
            _write_exe(decoys / "lakebench", '#!/bin/sh\necho "DECOY lakebench"\n')
            path = f"{decoys}{os.pathsep}{path}"
        install_dir = tmp_path / "bin"
        install_dir.mkdir(exist_ok=True)
        tmpdir = tmp_path / "tmp"
        tmpdir.mkdir(exist_ok=True)
        dead_proxy = "http://127.0.0.1:9"
        env_vars = {
            "PATH": path,
            "HOME": str(tmp_path),
            "TMPDIR": str(tmpdir),
            "INSTALL_DIR": str(install_dir),
            "VERSION": TAG,
            "LB_INSTALL_BASE_URL": base_url,
            "http_proxy": dead_proxy,
            "https_proxy": dead_proxy,
            "HTTPS_PROXY": dead_proxy,
            "no_proxy": "127.0.0.1",
            "NO_PROXY": "127.0.0.1",
        }
        return Env(install_dir, tmpdir, env_vars)

    return make


_SCRIPT_TOOLS = ("bash", "curl", "awk", "tr", "grep", "cut", "mktemp", "rm", "cp", "mv", "chmod")
_SHASUM_STUB = """#!{python}
import hashlib, sys
args = sys.argv[1:]
assert args[:2] == ["-a", "256"], args
for name in args[2:]:
    with open(name, "rb") as fh:
        print(hashlib.sha256(fh.read()).hexdigest() + "  " + name)
"""


def _run(env: Env, script: bytes | None = None) -> subprocess.CompletedProcess[str]:
    if script is None:
        args, stdin = ["bash", str(INSTALL_SH)], None
    else:
        args, stdin = ["bash", "-s"], script.decode()
    return subprocess.run(
        args, env=env.vars, input=stdin, capture_output=True, text=True, timeout=60
    )


def _publish(rel: Release, **assets: bytes) -> None:
    rel.assets.update(assets)
    binaries = {name: body for name, body in rel.assets.items() if name != "SHA256SUMS"}
    rel.assets["SHA256SUMS"] = _sums(binaries)


def test_installs_a_verified_binary(release, make_env):
    rel, _ = release
    _publish(rel, **{"lakebench-linux-amd64": FAKE_BINARY, "lakebench-macos-arm64": b"other"})
    env = make_env()
    (env.install_dir / "lakebench").write_text("old")

    result = _run(env)

    assert result.returncode == 0, result.stderr
    installed = env.install_dir / "lakebench"
    assert installed.read_bytes() == FAKE_BINARY
    assert os.access(installed, os.X_OK)
    assert "lakebench fake 9.9.9" in result.stdout
    assert sorted(p.name for p in env.install_dir.iterdir()) == ["lakebench"]
    assert list(env.tmpdir.iterdir()) == []


def test_verifies_the_installed_binary_not_the_one_on_path(release, make_env):
    rel, _ = release
    _publish(rel, **{"lakebench-linux-amd64": FAKE_BINARY})
    env = make_env(decoy=True)

    result = _run(env)

    assert result.returncode == 0, result.stderr
    assert "lakebench fake 9.9.9" in result.stdout
    assert "DECOY" not in result.stdout


def test_truncated_download_leaves_no_binary(release, make_env):
    rel, _ = release
    _publish(rel, **{"lakebench-linux-amd64": FAKE_BINARY * 4096})
    rel.truncate.add("lakebench-linux-amd64")
    env = make_env()

    result = _run(env)

    assert result.returncode != 0
    assert list(env.install_dir.iterdir()) == []
    assert list(env.tmpdir.iterdir()) == []


def test_bad_checksum_exits_nonzero(release, make_env):
    rel, _ = release
    _publish(rel, **{"lakebench-linux-amd64": FAKE_BINARY})
    rel.assets["lakebench-linux-amd64"] = FAKE_BINARY + b"# tampered\n"
    env = make_env()

    result = _run(env)

    assert result.returncode != 0
    assert "checksum mismatch" in result.stderr
    assert list(env.install_dir.iterdir()) == []
    assert list(env.tmpdir.iterdir()) == []


@pytest.mark.parametrize("version", ["v1.7.0", "1.7.1", "v1.10.0", "2.0.0", "v7", "vnext"])
def test_missing_sha256sums_on_a_1_7_or_later_release_is_refused(release, make_env, version):
    # 1.7.0 is the first release that publishes SHA256SUMS; a tag that does
    # not parse as <major>.<minor> counts as new.
    rel, _ = release
    rel.tag = "v" + version.removeprefix("v")
    rel.assets["lakebench-linux-amd64"] = FAKE_BINARY  # no SHA256SUMS
    env = make_env()
    env.vars["VERSION"] = version

    result = _run(env)

    assert result.returncode != 0
    assert "has no SHA256SUMS; refusing" in result.stderr
    assert "UNVERIFIED" not in result.stderr
    assert list(env.install_dir.iterdir()) == []


@pytest.mark.parametrize("version", ["1.6.0", "v1.6.2", "v1.0.2", "0.9.1"])
def test_pre_1_7_release_without_sha256sums_installs_with_a_warning(release, make_env, version):
    rel, _ = release
    rel.tag = "v" + version.removeprefix("v")
    rel.assets["lakebench-linux-amd64"] = FAKE_BINARY  # these releases publish no SHA256SUMS
    env = make_env()
    env.vars["VERSION"] = version

    result = _run(env)

    assert result.returncode == 0, result.stderr
    assert "UNVERIFIED" in result.stderr
    assert (env.install_dir / "lakebench").read_bytes() == FAKE_BINARY
    assert "lakebench fake 9.9.9" in result.stdout


def test_pre_1_7_release_with_sha256sums_is_still_checked(release, make_env):
    rel, _ = release
    rel.tag = "v1.6.0"
    _publish(rel, **{"lakebench-linux-amd64": FAKE_BINARY})
    rel.assets["lakebench-linux-amd64"] = FAKE_BINARY + b"# tampered\n"
    env = make_env()
    env.vars["VERSION"] = "1.6.0"

    result = _run(env)

    assert result.returncode != 0
    assert "checksum mismatch" in result.stderr
    assert list(env.install_dir.iterdir()) == []


def test_pre_1_7_release_with_a_failing_sha256sums_fetch_is_refused(release, make_env):
    # Only a 404 means "no SHA256SUMS"; a server error is not a reason to skip the check.
    rel, _ = release
    rel.tag = "v1.6.0"
    rel.assets["lakebench-linux-amd64"] = FAKE_BINARY
    rel.status["SHA256SUMS"] = 500
    env = make_env()
    env.vars["VERSION"] = "1.6.0"

    result = _run(env)

    assert result.returncode != 0
    assert "HTTP 500" in result.stderr
    assert "UNVERIFIED" not in result.stderr
    assert list(env.install_dir.iterdir()) == []


def test_sha256sums_without_an_entry_for_the_binary_is_refused(release, make_env):
    rel, _ = release
    _publish(rel, **{"lakebench-macos-arm64": b"other"})
    rel.assets["lakebench-linux-amd64"] = FAKE_BINARY
    env = make_env()

    result = _run(env)

    assert result.returncode != 0
    assert "no valid entry for lakebench-linux-amd64" in result.stderr
    assert list(env.install_dir.iterdir()) == []


@pytest.mark.parametrize("machine", ["aarch64", "arm64"])
def test_linux_arm64_refused(release, make_env, machine):
    rel, _ = release
    _publish(rel, **{"lakebench-linux-amd64": FAKE_BINARY})
    env = make_env(os_name="Linux", machine=machine)

    result = _run(env)

    assert result.returncode != 0
    assert rel.requests == []  # refused before any download
    for asset in ("lakebench-linux-amd64", "lakebench-macos-amd64", "lakebench-macos-arm64"):
        assert asset in result.stderr
    assert list(env.install_dir.iterdir()) == []


def test_macos_arm64_with_binary_mode_sums(release, make_env):
    rel, _ = release
    rel.assets["lakebench-macos-arm64"] = FAKE_BINARY
    rel.assets["SHA256SUMS"] = _sums({"lakebench-macos-arm64": FAKE_BINARY}, binary_mode=True)
    env = make_env(os_name="Darwin", machine="arm64")

    result = _run(env)

    assert result.returncode == 0, result.stderr
    assert f"/dl/{TAG}/lakebench-macos-arm64" in rel.requests
    assert (env.install_dir / "lakebench").read_bytes() == FAKE_BINARY


def test_truncated_script_runs_nothing(release, make_env):
    """``curl ... | bash`` with the pipe cut at any line runs no download."""
    rel, _ = release
    _publish(rel, **{"lakebench-linux-amd64": FAKE_BINARY})
    env = make_env()
    script = INSTALL_SH.read_bytes()
    cuts = [i + 1 for i, byte in enumerate(script) if byte == ord("\n")][:-1]
    assert len(cuts) > 50

    for cut in cuts:
        _run(env, script[:cut])
        assert rel.requests == [], f"a script cut after byte {cut} downloaded"
        assert list(env.install_dir.iterdir()) == [], f"a script cut after byte {cut} installed"

    full = _run(env, script)  # the same harness does install with the whole script
    assert full.returncode == 0, full.stderr
    assert (env.install_dir / "lakebench").read_bytes() == FAKE_BINARY


def test_shasum_is_used_when_sha256sum_is_absent(release, make_env):
    rel, _ = release
    rel.assets["lakebench-macos-amd64"] = FAKE_BINARY
    rel.assets["SHA256SUMS"] = _sums({"lakebench-macos-amd64": FAKE_BINARY})
    env = make_env(os_name="Darwin", machine="x86_64", minimal_path=True)
    assert shutil.which("sha256sum", path=env.vars["PATH"]) is None

    result = _run(env)

    assert result.returncode == 0, result.stderr
    assert (env.install_dir / "lakebench").read_bytes() == FAKE_BINARY

    rel.assets["lakebench-macos-amd64"] = FAKE_BINARY + b"# tampered\n"
    (env.install_dir / "lakebench").unlink()
    result = _run(env)
    assert result.returncode != 0
    assert "checksum mismatch" in result.stderr
    assert list(env.install_dir.iterdir()) == []


def test_installed_binary_is_755_under_a_strict_umask(release, make_env):
    rel, _ = release
    _publish(rel, **{"lakebench-linux-amd64": FAKE_BINARY})
    env = make_env()

    result = subprocess.run(
        ["bash", "-c", 'umask 077; exec bash "$0"', str(INSTALL_SH)],
        env=env.vars,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stderr
    assert stat.S_IMODE((env.install_dir / "lakebench").stat().st_mode) == 0o755


@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root can write anywhere")
def test_unwritable_install_dir_refused_before_download(release, make_env):
    rel, _ = release
    _publish(rel, **{"lakebench-linux-amd64": FAKE_BINARY})
    env = make_env()
    env.install_dir.chmod(0o555)
    try:
        result = _run(env)
    finally:
        env.install_dir.chmod(0o755)

    assert result.returncode != 0
    assert "cannot write to" in result.stderr
    assert rel.requests == []


def test_pre_1_7_release_with_an_unreachable_sha256sums_is_refused(release, make_env):
    rel, _ = release
    rel.tag = "v1.6.0"
    rel.assets["lakebench-linux-amd64"] = FAKE_BINARY
    rel.status["SHA256SUMS"] = 0
    env = make_env()
    env.vars["VERSION"] = "1.6.0"

    result = _run(env)

    assert result.returncode != 0
    assert "HTTP 000" in result.stderr
    assert "UNVERIFIED" not in result.stderr
    assert list(env.install_dir.iterdir()) == []


def test_binary_that_does_not_run_replaces_nothing(release, make_env):
    # A pre-1.6 binary built for the wrong architecture or a newer glibc
    # fails to run; the lakebench already installed must survive.
    rel, _ = release
    rel.tag = "v1.5.0"
    broken = b"#!/bin/sh\necho 'cannot execute binary file' >&2\nexit 126\n"
    rel.assets["lakebench-linux-amd64"] = broken
    env = make_env()
    env.vars["VERSION"] = "1.5.0"
    (env.install_dir / "lakebench").write_bytes(FAKE_BINARY)

    result = _run(env)

    assert result.returncode != 0
    assert "does not run here" in result.stderr
    assert sorted(p.name for p in env.install_dir.iterdir()) == ["lakebench"]
    assert (env.install_dir / "lakebench").read_bytes() == FAKE_BINARY
