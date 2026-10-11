"""A fake spark-submit, pip and lb-deps pod environment for the lb_deps tests."""

from __future__ import annotations

import gzip
import hashlib
import http.server
import io
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import pytest

from lakebench.deploy.deps_tools import lb_deps
from lakebench.deps import request as req_mod

TOOL = Path(lb_deps.__file__)


ICE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"


BUNDLE = "org.apache.iceberg:iceberg-aws-bundle:1.11.0"


HADOOP = "org.apache.hadoop:hadoop-aws:3.4.1"


COORDS = [ICE, BUNDLE, HADOOP]


# Transitives the fake resolve adds, in the order Spark would list them.
TRANSITIVE = [
    "software.amazon.awssdk:bundle:2.24.6",
    "org.wildfly.openssl:wildfly-openssl:1.1.3.Final",
]


PINS = ["numpy==2.2.6", "scikit-learn==1.7.2", "python-dateutil==2.9.0.post0"]


FAKE_SPARK_SUBMIT = r"""#!{python}
import os, shutil, sys, zipfile


def put(z, name, data, ct=zipfile.ZIP_DEFLATED):
    # Fixed timestamps: the same coordinate gives the same bytes every run.
    z.writestr(zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0)), data, compress_type=ct)


args = sys.argv[1:]
coords = args[args.index("--packages") + 1].split(",")
confs = dict(args[i + 1].split("=", 1) for i, a in enumerate(args) if a == "--conf")
ivy = confs["spark.jars.ivy"]
extra = [c for c in os.environ.get("FAKE_TRANSITIVE", "").split(",") if c]
skip = os.environ.get("FAKE_SKIP", "")
big = int(os.environ.get("FAKE_BIG_MB", "0"))
jars = os.path.join(ivy, "jars")
os.makedirs(jars, exist_ok=True)
print(":: resolving dependencies ::")
order = []
for c in coords + extra:
    if c == skip:
        continue
    g, a, v = c.split(":")
    print("\tfound %s#%s;%s in repo-0" % (g, a, v))
    fn = "%s_%s-%s.jar" % (g, a, v)
    path = os.path.join(jars, fn)
    # Ivy's retrieve keeps an existing file that is newer than its cache copy.
    if not os.path.exists(path):
        with zipfile.ZipFile(path, "w") as z:
            put(z, "META-INF/maven/%s/%s/pom.properties" % (g, a), "groupId=%s\nartifactId=%s\nversion=%s\n" % (g, a, v))
            put(z, "x/%s.class" % a, a * 50)
            if big and "aws-bundle" in a:
                put(z, "big.bin", os.urandom(big << 20), zipfile.ZIP_STORED)
    order.append("file://" + path)
print("(spark.jars," + ",".join(order) + ")")
print("Error: Failed to load class org.apache.spark.deploy.DummyNonExistent.")
sys.exit(101)
"""


FAKE_PIP = r"""import os, sys, zipfile
args = sys.argv[1:]
assert "--isolated" in args, args
if args[0] == "download":
    d = args[args.index("-d") + 1]
    os.makedirs(d, exist_ok=True)
    pins = [a for a in args if "==" in a]
    for p in pins:
        n, v = p.split("==")
        fn = "%s-%s-py3-none-any.whl" % (n.replace("-", "_"), v)
        with zipfile.ZipFile(os.path.join(d, fn), "w") as z:
            z.writestr("%s/__init__.py" % n.replace("-", "_"), "")
elif args[0] == "install":
    t = args[args.index("--target") + 1]
    os.makedirs(os.path.join(t, "duckdb"), exist_ok=True)
    open(os.path.join(t, "duckdb", "__init__.py"), "w").write(
        "class _R:\n    def fetchone(self):\n        return ['linux_amd64']\n"
        "def sql(q):\n    return _R()\n")
"""


FAKE_VARS = ("FAKE_SKIP", "FAKE_BIG_MB")


def _zip_bytes(names: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for n, body in names.items():
            z.writestr(n, body)
    return buf.getvalue()


class Env:
    """One lb-deps pod's filesystem and environment."""

    def __init__(self, tmp: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.tmp = tmp
        self.mp = monkeypatch
        self.root = tmp / "deps"
        self.work = tmp / "work"
        self.tools = tmp / "tools"
        self.spark = tmp / "spark"
        for d in (self.root, self.work, self.tools, self.spark / "bin", self.spark / "jars"):
            d.mkdir(parents=True)
        ss = self.spark / "bin" / "spark-submit"
        ss.write_text(FAKE_SPARK_SUBMIT.replace("{python}", sys.executable))
        ss.chmod(0o755)
        # The stock image always ships its own jars.
        (self.spark / "jars" / "spark-core_2.13-4.0.2.jar").write_bytes(
            _zip_bytes(
                {
                    "META-INF/maven/org.apache.spark/spark-core_2.13/pom.properties": "artifactId=spark-core_2.13\nversion=4.0.2\n"
                }
            )
        )
        pip = tmp / "fakepip.py"
        pip.write_text(FAKE_PIP)
        monkeypatch.setenv("LB_DEPS_ROOT", str(self.root))
        monkeypatch.setenv("LB_DEPS_WORK", str(self.work))
        monkeypatch.setenv("SPARK_HOME", str(self.spark))
        monkeypatch.setenv("LB_DEPS_PIP", f"{sys.executable} {pip}")
        monkeypatch.setenv("LB_DEPS_LOCK_WAIT", "5")
        for v in FAKE_VARS:
            monkeypatch.delenv(v, raising=False)
        monkeypatch.setenv("FAKE_TRANSITIVE", ",".join(TRANSITIVE))

    def request(self, **fields) -> dict:
        r = {
            "groups": ["jars"],
            "jar_coordinates": COORDS,
            "repositories": [
                req_mod.MAVEN_CENTRAL,
                "https://maven-central.storage-download.googleapis.com/maven2/",
            ],
            "spark_image": "apache/spark:4.0.2-python3",
            "tools_sha256": hashlib.sha256(TOOL.read_bytes()).hexdigest(),
        }
        r.update(fields)
        raw = json.dumps(r, sort_keys=True, separators=(",", ":")).encode()
        (self.tools / "request.json").write_bytes(raw)
        sha = hashlib.sha256(raw).hexdigest()
        self.mp.setenv("LB_DEPS_REQUEST_SHA256", sha)
        return {"sha": sha, **r}

    def run(self, *argv: str) -> int:
        if argv and argv[0] == "resolve":
            argv = (*argv, "--request", str(self.tools / "request.json"))
        return lb_deps.main(list(argv))

    def pointer(self, sha: str) -> dict:
        return json.loads((self.root / "requests" / f"{sha}.json").read_text())

    def set_dir(self, sha: str) -> Path:
        return self.root / "sets" / self.pointer(sha)["pinset_sha256"]

    def staged(self, sha: str) -> Path:
        return self.root / "staging" / sha / "set"


class _ExtRepo(http.server.BaseHTTPRequestHandler):
    files: dict[str, bytes] = {}
    agents: list[str] = []

    def do_GET(self):  # noqa: N802
        ua = self.headers.get("User-Agent", "")
        self.agents.append(ua)
        if ua.startswith("Python-urllib"):  # what extensions.duckdb.org does
            self.send_error(403)
            return
        body = self.files.get(self.path)
        if body is None:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


@pytest.fixture
def ext_repo():
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _ExtRepo)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    _ExtRepo.files = {
        f"/v1.5.5/linux_amd64/{n}.duckdb_extension.gz": gzip.compress(n.encode() * 100)
        for n in ("httpfs", "iceberg", "avro")
    }
    _ExtRepo.agents = []
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


_SERVE = """
import socket, sys
s = socket.socket()
s.bind(("127.0.0.1", 0))
port = s.getsockname()[1]
s.close()
open(sys.argv[2], "w").write(str(port))
sys.path.insert(0, sys.argv[1].rsplit("/", 1)[0])
import lb_deps
sys.exit(lb_deps.main(["serve", "--port", str(port)]))
"""


def _serve(port_file: Path):
    """Run serve in a child process, so SIGTERM reaches it as it would PID 1."""
    return subprocess.Popen(
        [sys.executable, "-c", _SERVE, str(TOOL), str(port_file)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=dict(os.environ),
    )


def _wait_port(proc, port_file: Path) -> int:
    for _ in range(200):
        if proc.poll() is not None:
            raise AssertionError(proc.communicate())
        if port_file.exists() and port_file.read_text():
            port = int(port_file.read_text())
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/ready", timeout=1)
                return port
            except (urllib.error.URLError, ConnectionError):
                pass
        time.sleep(0.1)
    raise AssertionError("serve did not come up")
