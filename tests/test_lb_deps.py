"""lb_deps.py: the stdlib resolver, verifier and server of DEP-2 (ch01 s2.3-2.6).

A fake ``spark-submit`` and a fake ``pip`` stand in for the stock image's
tools; a local HTTP server stands in for the DuckDB extension repository.
The real tools on the stock images were exercised by the SD-3 offline run
(four release-matrix rows) and the SD-1 live spike.
"""

from __future__ import annotations

import ast
import errno
import fcntl
import gzip
import hashlib
import http.server
import io
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import pytest

from lakebench.deploy.deps_tools import lb_deps
from lakebench.deps import request as req_mod

TOOL = Path(lb_deps.__file__)
ICE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"
BUNDLE = "org.apache.iceberg:iceberg-aws-bundle:1.11.0"
HADOOP = "org.apache.hadoop:hadoop-aws:3.4.1"
DELTA = "io.delta:delta-spark_2.13:4.0.0"
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
if os.environ.get("FAKE_SPARK_FAIL_IF_CALLED"):
    sys.exit("spark-submit must not run")
if os.environ.get("FAKE_KILL"):
    os.kill(os.getpid(), 9)
log = os.environ.get("FAKE_SPARK_ARGV")
if log:
    open(log, "a").write(repr(args) + "\n")
coords = args[args.index("--packages") + 1].split(",")
confs = dict(args[i + 1].split("=", 1) for i, a in enumerate(args) if a == "--conf")
ivy = confs["spark.jars.ivy"]
extra = [c for c in os.environ.get("FAKE_TRANSITIVE", "").split(",") if c]
skip = os.environ.get("FAKE_SKIP", "")
big = int(os.environ.get("FAKE_BIG_MB", "0"))
dup = os.environ.get("FAKE_DUP_CLASS", "").split(",")
jars = os.path.join(ivy, "jars")
os.makedirs(jars, exist_ok=True)
print(":: resolving dependencies ::")
if os.environ.get("FAKE_EGRESS"):
    print("Server access error at url https://repo1.maven.org/maven2/ (java.net.UnknownHostException: repo1.maven.org)")
    sys.exit(1)
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
            if a in dup:
                put(z, "shared/Dup.class", a)
            if os.environ.get("FAKE_MODULE_INFO"):
                put(z, "module-info.class", a)
            if big and "aws-bundle" in a:
                put(z, "big.bin", os.urandom(big << 20), zipfile.ZIP_STORED)
    order.append("file://" + path)
if not os.environ.get("FAKE_NO_VERBOSE"):
    print("(spark.jars," + ",".join(order) + ")")
print("Error: Failed to load class org.apache.spark.deploy.DummyNonExistent.")
sys.exit(101)
"""

FAKE_PIP = r"""import os, sys, zipfile
args = sys.argv[1:]
log = os.environ.get("FAKE_PIP_ARGV")
if log:
    open(log, "a").write(repr(args) + "\n")
assert "--isolated" in args, args
if args[0] == "download":
    d = args[args.index("-d") + 1]
    os.makedirs(d, exist_ok=True)
    pins = [a for a in args if "==" in a]
    if os.environ.get("FAKE_PIP_FULL"):
        print("ERROR: Could not install packages due to an OSError: [Errno 28] No space left on device")
        sys.exit(1)
    if os.environ.get("FAKE_PIP_FAIL"):
        print("WARNING: Retrying after connection broken by 'NewConnectionError: Failed to establish a new connection'")
        sys.exit(1)
    for p in pins + [x for x in os.environ.get("FAKE_PIP_EXTRA", "").split(",") if x]:
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

FAKE_VARS = (
    "FAKE_SKIP",
    "FAKE_SPARK_FAIL_IF_CALLED",
    "FAKE_NO_VERBOSE",
    "FAKE_PIP_EXTRA",
    "FAKE_PIP_FAIL",
    "FAKE_KILL",
    "FAKE_EGRESS",
    "FAKE_BIG_MB",
    "FAKE_DUP_CLASS",
    "FAKE_SPARK_ARGV",
    "FAKE_PIP_ARGV",
    "FAKE_MODULE_INFO",
    "FAKE_PIP_FULL",
)


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


# --- parity with lakebench.deps.request -------------------------------------


def test_constants_match_request_module():
    assert lb_deps.MANIFEST_GROUPS == req_mod.MANIFEST_GROUPS
    assert lb_deps.MANIFEST_DIRS == req_mod.MANIFEST_DIRS
    assert (lb_deps.GROUP_JARS, lb_deps.GROUP_PY_REFERENCE, lb_deps.GROUP_DUCKDB) == (
        req_mod.GROUP_JARS,
        req_mod.GROUP_PY_REFERENCE,
        req_mod.GROUP_DUCKDB,
    )
    assert req_mod.TOOLS_PATH == TOOL.resolve()
    assert req_mod.LB_DEPS_EXIT == {
        "missing": lb_deps.EXIT_MISSING,
        "hash": lb_deps.EXIT_HASH,
        "ux_d2": lb_deps.EXIT_UX_D2,
        "space": lb_deps.EXIT_SPACE,
        "internal": lb_deps.EXIT_INTERNAL,
        "request": lb_deps.EXIT_REQUEST,
    }


_AB = {"jars": [{"file": "b.jar", "sha256": "2" * 64}, {"file": "a.jar", "sha256": "1" * 64}]}


@pytest.mark.parametrize(
    ("groups", "order"),
    [
        (_AB, ["b.jar", "a.jar"]),
        (_AB, ["a.jar", "b.jar"]),
        (
            {
                "duckdb-ext": [
                    {"file": "v1/linux_amd64/x\u00e9.duckdb_extension", "sha256": "3" * 64}
                ]
            },
            [],
        ),
        ({"jars": [{"file": "a.jar", "sha256": "1" * 64}], "py-reference": []}, ["a.jar"]),
    ],
)
def test_pinset_matches_request_module(groups, order):
    assert lb_deps.pinset_sha256(groups, order) == req_mod.pinset_sha256(groups, order)


@pytest.mark.parametrize(
    "order", [["a.jar"], ["a.jar", "b.jar", "c.jar"], ["a.jar", "a.jar"], ["a.jar", "c.jar"]]
)
def test_pinset_refuses_an_order_that_is_not_the_jars_group(order):
    for fn in (lb_deps.pinset_sha256, req_mod.pinset_sha256):
        with pytest.raises(ValueError):
            fn(_AB, order)


def test_reordered_set_changes_the_pinset():
    """The same files in another order load other classes first, so they are
    another set (main lane decision 10-01)."""
    assert lb_deps.pinset_sha256(_AB, ["a.jar", "b.jar"]) != lb_deps.pinset_sha256(
        _AB, ["b.jar", "a.jar"]
    )
    assert req_mod.pinset_sha256(_AB, ["a.jar", "b.jar"]) != req_mod.pinset_sha256(
        _AB, ["b.jar", "a.jar"]
    )


def test_python38_grammar():
    ast.parse(TOOL.read_text(), feature_version=(3, 8))


def test_stdlib_only():
    tree = ast.parse(TOOL.read_text())
    mods = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module} | {
        a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names
    }
    tops = {m.split(".")[0] for m in mods}
    assert "lakebench" not in tops
    assert tops <= set(sys.stdlib_module_names) | {"__future__"}


def test_select_request_bytes_are_accepted(env, tmp_path):
    """The bytes SD-4a writes (canonical_json) are what load_request hashes."""
    from tests.test_deps_request import _cfg

    r = req_mod.select_request(_cfg())
    p = tmp_path / "request.json"
    p.write_bytes(r.canonical_json().encode())
    env.mp.setenv("LB_DEPS_REQUEST_SHA256", r.request_sha256)
    sha, loaded = lb_deps.load_request(str(p))
    assert sha == r.request_sha256
    assert loaded["jar_coordinates"] == list(r.jar_coordinates)


# --- request guard ----------------------------------------------------------------


def test_request_json_must_match_pod_env(env, capsys):
    env.request()
    env.mp.setenv("LB_DEPS_REQUEST_SHA256", "0" * 64)
    assert env.run("resolve", "spark") == lb_deps.EXIT_REQUEST
    assert "the pod expects" in _last_error(capsys.readouterr().out)


def test_request_must_name_this_resolver(env, capsys):
    env.request(tools_sha256="f" * 64)
    assert env.run("resolve", "spark") == lb_deps.EXIT_REQUEST
    assert "names resolver" in _last_error(capsys.readouterr().out)


# --- resolve: jars ------------------------------------------------------------


def test_resolve_writes_set_pointer_and_order(env, capsys):
    r = env.request()
    assert env.run("resolve", "spark") == 0
    ptr = env.pointer(r["sha"])
    sd = env.set_dir(r["sha"])
    man = json.loads((sd / "manifest.json").read_text())
    # The set's manifest is content only.
    assert set(man) == {"pinset_sha256", "groups", "jar_order"}
    assert (
        man["pinset_sha256"]
        == req_mod.pinset_sha256(man["groups"], man["jar_order"])
        == ptr["pinset_sha256"]
    )
    assert "jar_order" not in ptr
    want = [lb_deps.coordinate_jar(c) for c in COORDS + TRANSITIVE]
    # Spark's own order (direct coordinates first), not sorted.
    assert man["jar_order"] == want
    assert want != sorted(want)
    assert sorted(e["file"] for e in man["groups"]["jars"]) == sorted(want)
    assert ptr["repositories"] == r["repositories"]
    assert ptr["coordinates"] == {lb_deps.coordinate_jar(c): c for c in COORDS + TRANSITIVE}
    assert ptr["resolved_again"] == ""
    assert not (env.root / "staging" / r["sha"]).exists()
    assert "LB_DEPS_RESOLVED" in capsys.readouterr().out


def test_missing_direct_coordinate_exits_3(env, capsys):
    """Today's resolve-deps ends in `|| true`, so this passed silently."""
    r = env.request()
    env.mp.setenv("FAKE_SKIP", BUNDLE)
    assert env.run("resolve", "spark") == lb_deps.EXIT_MISSING
    err = _last_error(capsys.readouterr().out)
    assert BUNDLE in err and "egress:" not in err
    assert not (env.root / "requests" / f"{r['sha']}.json").exists()


def test_unreachable_repository_says_egress(env, capsys):
    env.request()
    env.mp.setenv("FAKE_EGRESS", "1")
    assert env.run("resolve", "spark") == lb_deps.EXIT_MISSING
    assert "egress: UnknownHostException" in _last_error(capsys.readouterr().out)


def test_killed_spark_submit_is_internal(env, capsys):
    """The Ivy JVM at its memory limit is SIGKILLed; that is not a missing
    artifact and must not get the mirror hint."""
    env.request()
    env.mp.setenv("FAKE_KILL", "1")
    assert env.run("resolve", "spark") == lb_deps.EXIT_INTERNAL
    assert "killed by signal 9" in _last_error(capsys.readouterr().out)


def test_no_verbose_jar_list_exits_3(env, capsys):
    env.request()
    env.mp.setenv("FAKE_NO_VERBOSE", "1")
    assert env.run("resolve", "spark") == lb_deps.EXIT_MISSING
    assert "did not list" in _last_error(capsys.readouterr().out)


def test_two_iceberg_runtimes_exit_5(env, capsys):
    env.request()
    env.mp.setenv("FAKE_TRANSITIVE", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0")
    assert env.run("resolve", "spark") == lb_deps.EXIT_UX_D2
    assert "iceberg-spark-runtime-" in _last_error(capsys.readouterr().out)


def test_delta_set_with_an_iceberg_runtime_exits_5(env):
    env.request(jar_coordinates=[DELTA, HADOOP])
    env.mp.setenv("FAKE_TRANSITIVE", ICE)
    assert env.run("resolve", "spark") == lb_deps.EXIT_UX_D2


def test_request_without_format_runtime_exits_5(env):
    env.request(jar_coordinates=[HADOOP])
    assert env.run("resolve", "spark") == lb_deps.EXIT_UX_D2


def test_delta_runtime_rule(env):
    env.request(jar_coordinates=[DELTA, HADOOP])
    assert env.run("resolve", "spark") == 0


def test_ivysettings_has_only_the_requested_resolvers():
    xml = lb_deps.ivysettings_xml(["https://a.example/m2/", 'http://b.example/x?a=1&b="2"'])
    tree = ET.fromstring(xml)
    chain = tree.find("resolvers/chain")
    assert [r.tag for r in tree.find("resolvers")] == ["chain"]
    assert [(r.tag, r.get("root")) for r in chain] == [
        ("ibiblio", "https://a.example/m2/"),
        ("ibiblio", 'http://b.example/x?a=1&b="2"'),
    ]
    assert tree.find("settings").get("defaultResolver") == chain.get("name")


def test_spark_submit_gets_ivysettings_and_verbose(env, tmp_path):
    env.request()
    log = tmp_path / "argv.txt"
    env.mp.setenv("FAKE_SPARK_ARGV", str(log))
    assert env.run("resolve", "spark") == 0
    argv = ast.literal_eval(log.read_text().splitlines()[0])
    assert "--verbose" in argv
    assert any(a.startswith("spark.jars.ivySettings=") for a in argv)
    assert "--repositories" not in argv


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


def test_work_dir_is_cleared_per_container(env, capsys):
    """Ivy keeps a retrieved jar newer than its cache copy, so a truncated
    one from a killed attempt survives unless the work dir is cleared."""
    env.request()
    stale = env.work / "spark" / "ivy" / "jars" / lb_deps.coordinate_jar(BUNDLE)
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"truncated")
    assert env.run("resolve", "spark") == 0, capsys.readouterr().out


def test_low_pvc_space_exits_6(env, capsys, monkeypatch):
    env.request()
    monkeypatch.setattr(
        lb_deps.shutil, "disk_usage", lambda p: type("U", (), {"free": 100 << 20})()
    )
    assert env.run("resolve", "spark") == lb_deps.EXIT_SPACE
    assert "lb-deps-data" in _last_error(capsys.readouterr().out)


def test_enospc_mid_copy_exits_6(env, capsys, monkeypatch):
    env.request()

    def full(src, dst, *a, **k):
        raise OSError(errno.ENOSPC, "No space left on device", dst)

    monkeypatch.setattr(lb_deps.shutil, "copyfile", full)
    assert env.run("resolve", "spark") == lb_deps.EXIT_SPACE
    assert "no space left" in _last_error(capsys.readouterr().out)


def test_overlaps(env):
    (env.spark / "jars" / "hadoop-aws-3.3.0.jar").write_bytes(
        _zip_bytes(
            {
                "META-INF/maven/org.apache.hadoop/hadoop-aws/pom.properties": "artifactId=hadoop-aws\nversion=3.3.0\n"
            }
        )
    )
    # No pom.properties: the name decides.
    (env.spark / "jars" / "wildfly-openssl-1.1.3.Final.jar").write_bytes(_zip_bytes({"a": "b"}))
    # A version with a dash and no pom.properties (zstd-jni in Spark 4.1.1).
    (env.spark / "jars" / "zstd-jni-1.5.7-6.jar").write_bytes(_zip_bytes({"a": "b"}))
    # An artifactId with a digit segment and no pom.properties.
    (env.spark / "jars" / "log4j-1.2-api-2.24.3.jar").write_bytes(_zip_bytes({"a": "b"}))
    env.mp.setenv(
        "FAKE_TRANSITIVE",
        ",".join(
            [
                *TRANSITIVE,
                "com.github.luben:zstd-jni:1.5.6-3",
                "org.apache.logging.log4j:log4j-1.2-api:2.25.3",
            ]
        ),
    )
    r = env.request()
    assert env.run("resolve", "spark") == 0
    ov = {
        o["artifact"]: (o["version"], o["image_version"]) for o in env.pointer(r["sha"])["overlaps"]
    }
    assert ov == {
        "hadoop-aws": ("3.4.1", "3.3.0"),
        "wildfly-openssl": ("1.1.3.Final", "1.1.3.Final"),
        "zstd-jni": ("1.5.6-3", "1.5.7-6"),
        "log4j-1.2-api": ("2.25.3", "2.24.3"),
    }


def test_empty_image_jar_dir_fails(env, capsys):
    for p in (env.spark / "jars").iterdir():
        p.unlink()
    env.request()
    assert env.run("resolve", "spark") == lb_deps.EXIT_INTERNAL
    assert "cannot check overlaps" in _last_error(capsys.readouterr().out)


def test_duplicate_classes_recorded_with_the_winner(env):
    env.mp.setenv("FAKE_DUP_CLASS", "iceberg-aws-bundle,bundle")
    r = env.request()
    assert env.run("resolve", "spark") == 0
    assert env.pointer(r["sha"])["duplicate_classes"] == [
        {
            "wins": lb_deps.coordinate_jar(BUNDLE),
            "shadowed": lb_deps.coordinate_jar(TRANSITIVE[0]),
            "classes": 1,
        }
    ]


def test_unknown_overlaps_keyed_by_both_versions():
    seen = [
        {"artifact": "log4j-core", "version": "2.25.3", "image_version": "2.24.3"},
        {"artifact": "antlr4-runtime", "version": "4.13.1", "image_version": "4.13.1"},
        {"artifact": "log4j-core", "version": "2.26.0", "image_version": "2.24.3"},
        {"artifact": "guava", "version": "33.0", "image_version": "14.0"},
    ]
    assert [(o["artifact"], o["version"]) for o in req_mod.unknown_overlaps(seen)] == [
        ("log4j-core", "2.26.0"),
        ("guava", "33.0"),
    ]


# --- skip, reuse, recovery and one set -----------------------------------------


def test_existing_pointer_skips_the_download(env, capsys):
    r = env.request()
    assert env.run("resolve", "spark") == 0
    env.mp.setenv("FAKE_SPARK_FAIL_IF_CALLED", "1")
    assert env.run("resolve", "spark") == 0
    assert f"LB_DEPS_SKIP request={r['sha']}" in capsys.readouterr().out


def test_killed_before_pointer_resolves_again(env, capsys):
    r = env.request()
    assert env.run("resolve", "spark") == 0
    pinset = env.pointer(r["sha"])["pinset_sha256"]
    (env.root / "requests" / f"{r['sha']}.json").unlink()  # rename done, pointer not
    capsys.readouterr()
    assert env.run("resolve", "spark") == 0
    assert "LB_DEPS_RESOLVED" in capsys.readouterr().out
    assert env.pointer(r["sha"])["pinset_sha256"] == pinset


def test_duckdb_row_killed_after_the_move_completes(env, ext_repo, capsys, monkeypatch):
    """On a DuckDB row kubelet retries only resolve-spark. A kill between
    moving the set and writing the pointer must not wedge it."""
    r = _duck_request(env, ext_repo)
    assert env.run("resolve", "duckdb") == 0
    real = lb_deps.publish
    monkeypatch.setattr(
        lb_deps, "publish", lambda *a: (_ for _ in ()).throw(RuntimeError("killed"))
    )
    assert env.run("resolve", "spark") == lb_deps.EXIT_INTERNAL
    monkeypatch.setattr(lb_deps, "publish", real)
    env.mp.setenv("FAKE_SPARK_FAIL_IF_CALLED", "1")
    capsys.readouterr()
    assert env.run("resolve", "spark") == 0
    assert "(completed)" in capsys.readouterr().out
    assert lb_deps.verify_set(env.pointer(r["sha"])["pinset_sha256"]) is None


def test_pointer_to_damaged_set_resolves_again(env, capsys):
    """A pointer is not trusted: a set that fails verification is removed and
    resolved again instead of wedging every start, and the reason is kept."""
    r = env.request()
    assert env.run("resolve", "spark") == 0
    victim = next((env.set_dir(r["sha"]) / "jars").iterdir())
    victim.write_bytes(b"changed")
    capsys.readouterr()
    assert env.run("resolve", "spark") == 0
    out = capsys.readouterr().out
    assert "LB_DEPS_RESOLVE_AGAIN hash mismatch" in out and "LB_DEPS_RESOLVED" in out
    assert lb_deps.verify_set(env.pointer(r["sha"])["pinset_sha256"]) is None
    assert env.pointer(r["sha"])["resolved_again"].startswith("hash mismatch")


def test_pointer_with_a_bad_pinset_never_deletes_the_pvc(env):
    r = env.request()
    (env.root / "keep").write_text("x")
    (env.root / "requests").mkdir()
    (env.root / "requests" / f"{r['sha']}.json").write_text(json.dumps({"pinset_sha256": ".."}))
    assert env.run("resolve", "spark") == 0
    assert (env.root / "keep").exists()


def test_reused_set_carries_the_new_requests_record(env):
    """A mirror that serves the same bytes reuses the set; the pointer, not
    the set, says which repositories built it (2.12 step 3)."""
    a = env.request()
    assert env.run("resolve", "spark") == 0
    pinset = env.pointer(a["sha"])["pinset_sha256"]
    b = env.request(repositories=["http://nexus.example:8081/repository/maven-central/"])
    assert b["sha"] != a["sha"]
    assert env.run("resolve", "spark") == 0
    assert env.pointer(b["sha"])["pinset_sha256"] == pinset
    assert env.pointer(b["sha"])["repositories"] == b["repositories"]
    assert not (env.root / "requests" / f"{a['sha']}.json").exists()
    shown = lb_deps.manifest_for(b["sha"])
    assert shown["repositories"] == b["repositories"] and shown["request_sha256"] == b["sha"]


def test_reuse_replaces_a_damaged_existing_set(env):
    a = env.request()
    assert env.run("resolve", "spark") == 0
    sd = env.set_dir(a["sha"])
    (env.root / "requests" / f"{a['sha']}.json").unlink()
    next((sd / "jars").iterdir()).write_bytes(b"bad")
    assert env.run("resolve", "spark") == 0
    assert lb_deps.verify_set(sd.name) is None


def test_pvc_holds_one_set(env):
    env.request()
    assert env.run("resolve", "spark") == 0
    env.request(jar_coordinates=[DELTA, HADOOP])
    assert env.run("resolve", "spark") == 0
    assert len(list((env.root / "sets").iterdir())) == 1
    assert len(list((env.root / "requests").iterdir())) == 1


def test_first_container_clears_staging(env):
    """A jars-only request: leftovers of another attempt in staging must not
    reach the set."""
    r = env.request()
    leftover = env.staged(r["sha"]) / "duckdb" / "wheels" / "old-1.0-py3-none-any.whl"
    leftover.parent.mkdir(parents=True)
    leftover.write_bytes(_zip_bytes({"a": "b"}))
    assert env.run("resolve", "spark") == 0


def test_retried_spark_part_starts_its_groups_empty(env, ext_repo):
    """On a DuckDB row a retried resolve-spark keeps the duckdb files but not
    a jar a killed attempt staged."""
    r = _duck_request(env, ext_repo)
    assert env.run("resolve", "duckdb") == 0
    stale = env.staged(r["sha"]) / "jars" / "stale-1.0.jar"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(_zip_bytes({"a": "b"}))
    assert env.run("resolve", "spark") == 0
    files = [
        e["file"]
        for e in json.loads((env.set_dir(r["sha"]) / "manifest.json").read_text())["groups"]["jars"]
    ]
    assert "stale-1.0.jar" not in files


def test_unselected_staged_group_fails(env, capsys, monkeypatch):
    env.request()
    real = lb_deps.resolve_jars

    def plus_wheels(req, st_set, meta):
        real(req, st_set, meta)
        w = Path(st_set) / "py-reference"
        w.mkdir()
        (w / "x-1-py3-none-any.whl").write_bytes(_zip_bytes({"a": "b"}))

    monkeypatch.setattr(lb_deps, "resolve_jars", plus_wheels)
    assert env.run("resolve", "spark") == lb_deps.EXIT_INTERNAL
    assert "did not select" in _last_error(capsys.readouterr().out)


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


def test_concurrent_resolve_waits_for_the_lock(env, capsys):
    env.request()
    env.mp.setenv("LB_DEPS_LOCK_WAIT", "1")
    with open(env.root / ".resolve.lock", "w") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        assert env.run("resolve", "spark") == lb_deps.EXIT_INTERNAL
    assert "another resolve holds" in _last_error(capsys.readouterr().out)
    assert env.run("resolve", "spark") == 0


# --- resolve: wheels and DuckDB -----------------------------------------------------


def test_reference_wheels_one_per_pin(env, tmp_path):
    log = tmp_path / "pip.txt"
    env.mp.setenv("FAKE_PIP_ARGV", str(log))
    r = env.request(
        groups=["jars", "py-reference"],
        py_reference=PINS,
        pypi_index="http://pypi.mirror.example/simple/",
    )
    assert env.run("resolve", "spark") == 0
    files = [
        e["file"]
        for e in json.loads((env.set_dir(r["sha"]) / "manifest.json").read_text())["groups"][
            "py-reference"
        ]
    ]
    assert len(files) == 3 and any(f.startswith("scikit_learn-1.7.2-") for f in files)
    argv = ast.literal_eval(log.read_text().splitlines()[0])
    assert argv[argv.index("--index-url") + 1] == "http://pypi.mirror.example/simple/"
    assert argv[argv.index("--trusted-host") + 1] == "pypi.mirror.example"
    assert "--isolated" in argv


def test_extra_wheel_exits_3(env):
    env.request(groups=["jars", "py-reference"], py_reference=PINS, pypi_index=req_mod.PYPI_INDEX)
    env.mp.setenv("FAKE_PIP_EXTRA", "six==1.17.0")
    assert env.run("resolve", "spark") == lb_deps.EXIT_MISSING


def test_unreachable_index_says_egress(env, capsys):
    env.request(groups=["jars", "py-reference"], py_reference=PINS, pypi_index=req_mod.PYPI_INDEX)
    env.mp.setenv("FAKE_PIP_FAIL", "1")
    assert env.run("resolve", "spark") == lb_deps.EXIT_MISSING
    assert "egress: NewConnectionError" in _last_error(capsys.readouterr().out)


def test_wheels_without_an_index_are_refused(env):
    env.request(groups=["jars", "py-reference"], py_reference=PINS)
    assert env.run("resolve", "spark") == lb_deps.EXIT_REQUEST


def test_https_index_has_no_trusted_host():
    assert lb_deps.pip_index_args("https://pypi.org/simple/") == [
        "--isolated",
        "--index-url",
        "https://pypi.org/simple/",
    ]


def test_short_count_of_wheels_fails(env, capsys, monkeypatch):
    env.request(groups=["jars", "py-reference"], py_reference=PINS, pypi_index=req_mod.PYPI_INDEX)
    real = lb_deps.resolve_py_reference

    def drop_one(req, st_set):
        real(req, st_set)
        d = Path(st_set) / "py-reference"
        sorted(d.iterdir())[0].unlink()

    monkeypatch.setattr(lb_deps, "resolve_py_reference", drop_one)
    assert env.run("resolve", "spark") == lb_deps.EXIT_MISSING
    assert "holds 2 files, needs 3" in _last_error(capsys.readouterr().out)


def test_empty_group_fails(env, capsys, monkeypatch):
    """A selected group with no files must not finalise, even when the step
    that fills it returned without error. pinset_sha256 ignores an empty
    group, so it would otherwise pass unnoticed."""
    env.request(groups=["jars", "py-reference"], py_reference=PINS, pypi_index=req_mod.PYPI_INDEX)
    monkeypatch.setattr(lb_deps, "resolve_py_reference", lambda *a: None)
    assert env.run("resolve", "spark") == lb_deps.EXIT_MISSING
    assert "group py-reference holds 0 files" in _last_error(capsys.readouterr().out)


def test_empty_pin_list_is_refused(env):
    env.request(groups=["jars", "py-reference"], py_reference=[], pypi_index=req_mod.PYPI_INDEX)
    assert env.run("resolve", "spark") == lb_deps.EXIT_REQUEST


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


def _duck_request(env, repo, **extra):
    fields = {
        "groups": ["jars", "duckdb"],
        "pypi_index": "http://pypi.mirror.example/simple/",
        "duckdb_version": "1.5.5",
        "duckdb_extensions": ["httpfs", "iceberg", "avro"],
        "duckdb_image": "python:3.11-slim",
        "duckdb_extension_repository": repo,
    }
    fields.update(extra)
    return env.request(**fields)


def test_duckdb_resolve(env, ext_repo, tmp_path):
    log = tmp_path / "pip.txt"
    env.mp.setenv("FAKE_PIP_ARGV", str(log))
    r = _duck_request(env, ext_repo)
    assert env.run("resolve", "duckdb") == 0
    assert env.run("resolve", "spark") == 0
    groups = json.loads((env.set_dir(r["sha"]) / "manifest.json").read_text())["groups"]
    assert [e["file"] for e in groups["duckdb-ext"]] == [
        f"v1.5.5/linux_amd64/{n}.duckdb_extension" for n in ("avro", "httpfs", "iceberg")
    ]
    ext = env.set_dir(r["sha"]) / "duckdb-ext/v1.5.5/linux_amd64/iceberg.duckdb_extension"
    assert ext.read_bytes() == b"iceberg" * 100
    assert len(groups["duckdb-wheels"]) == 1
    assert _ExtRepo.agents and all(a == lb_deps.USER_AGENT for a in _ExtRepo.agents)
    # The DuckDB wheel goes through the configured index too.
    download = ast.literal_eval(log.read_text().splitlines()[0])
    assert (
        "duckdb==1.5.5" in download and "--index-url" in download and "--trusted-host" in download
    )
    assert set(env.pointer(r["sha"])["python"]) == {"duckdb", "spark"}


def test_missing_extension_exits_3(env, ext_repo, capsys):
    _duck_request(env, ext_repo, duckdb_extensions=["httpfs", "nosuch"])
    assert env.run("resolve", "duckdb") == lb_deps.EXIT_MISSING
    assert "nosuch" in _last_error(capsys.readouterr().out)


def test_unreachable_extension_repository_says_egress(env, capsys):
    _duck_request(env, "http://127.0.0.1:9")
    assert env.run("resolve", "duckdb") == lb_deps.EXIT_MISSING
    assert "egress:" in _last_error(capsys.readouterr().out)


def test_spark_part_needs_the_duckdb_part(env, ext_repo, capsys):
    _duck_request(env, ext_repo)
    assert env.run("resolve", "spark") == lb_deps.EXIT_INTERNAL
    assert "delete the lb-deps pod" in _last_error(capsys.readouterr().out)


# --- serve ------------------------------------------------------------------------

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


def _status(url: str, method: str = "GET") -> int:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method=method), timeout=5) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


@pytest.fixture
def served(env, tmp_path, capsys):
    """A resolved set behind the real serve, plus its shown manifest."""
    env.mp.setenv("FAKE_BIG_MB", "24")
    r = env.request()
    assert env.run("resolve", "spark") == 0
    capsys.readouterr()
    assert env.run("show") == 0
    man = tmp_path / "manifest.json"
    man.write_text(capsys.readouterr().out)
    pinset = json.loads(man.read_text())["pinset_sha256"]
    proc = _serve(tmp_path / "port")
    port = _wait_port(proc, tmp_path / "port")
    yield {
        "proc": proc,
        "man": man,
        "sha": r["sha"],
        "pinset": pinset,
        "base": f"http://127.0.0.1:{port}",
        "url": f"http://127.0.0.1:{port}/sets/{pinset}",
    }
    if proc.poll() is None:
        proc.kill()
        proc.wait()


def test_serve_paths(env, served):
    base, pinset, sha = served["base"], served["pinset"], served["sha"]
    assert urllib.request.urlopen(f"{base}/ready").read().decode() == pinset
    jar = lb_deps.coordinate_jar(HADOOP)
    assert _status(f"{base}/sets/{pinset}/jars/{jar}") == 200
    assert _status(f"{base}/sets/{pinset}/jars/") == 200  # listing for pip --find-links
    assert _status(f"{base}/sets/{'0' * 64}/jars/{jar}") == 404
    assert _status(f"{base}/sets/{pinset}/../../requests/{sha}.json") == 404
    assert _status(f"{base}/sets/{pinset}/%2e%2e/%2e%2e/requests/{sha}.json") == 404
    assert _status(f"{base}/sets/{pinset}/jars/%00x") == 404
    assert _status(f"{base}/requests/{sha}.json") == 404
    assert _status(f"{base}/sets/{pinset}/jars/{jar}", "PUT") == 405
    assert _status(f"{base}/sets/{pinset}/jars/{jar}", "DELETE") == 405
    body = urllib.request.urlopen(f"{base}/sets/{pinset}/jars/{jar}").read()
    assert body == (env.set_dir(sha) / "jars" / jar).read_bytes()


def test_fetch_through_serve(served, tmp_path, capsys):
    dest = tmp_path / "out"
    args = [
        "fetch",
        "--group",
        "jars",
        "--dest",
        str(dest),
        "--manifest",
        str(served["man"]),
        "--url",
        served["url"],
    ]
    assert lb_deps.main(args) == 0
    assert sorted(p.name for p in dest.iterdir()) == sorted(
        lb_deps.coordinate_jar(c) for c in COORDS + TRANSITIVE
    )
    assert "LB_DEPS_FETCHED group=jars files=5" in capsys.readouterr().out


def test_sigterm_finishes_downloads_in_flight(env, served):
    jar = lb_deps.coordinate_jar(BUNDLE)  # about 24 MiB, more than the socket buffers
    want = (env.set_dir(served["sha"]) / "jars" / jar).read_bytes()
    with urllib.request.urlopen(f"{served['url']}/jars/{jar}", timeout=10) as r:
        head = r.read(1024)
        t0 = time.time()
        served["proc"].send_signal(signal.SIGTERM)
        time.sleep(0.5)
        body = head + r.read()
    assert body == want
    assert served["proc"].wait(timeout=10) == 0
    assert time.time() - t0 < 8
    out = served["proc"].stdout.read().decode()
    assert f"LB_DEPS_READY request={served['sha']} pinset={served['pinset']}" in out


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


@pytest.mark.parametrize("damage", ["modify", "delete", "extra"])
def test_serve_exits_4_on_a_changed_set(env, capsys, damage):
    r = env.request()
    assert env.run("resolve", "spark") == 0
    jars = env.set_dir(r["sha"]) / "jars"
    victim = next(jars.iterdir())
    if damage == "modify":
        victim.write_bytes(b"x")
    elif damage == "delete":
        victim.unlink()
    else:
        (jars / "planted.jar").write_bytes(b"x")
    capsys.readouterr()
    assert env.run("serve", "--port", "0") == lb_deps.EXIT_HASH
    err = _last_error(capsys.readouterr().out)
    assert ("hash mismatch" in err) if damage != "extra" else ("not in manifest" in err)


def test_serve_without_a_set_exits_4(env):
    env.request()
    assert env.run("serve", "--port", "0") == lb_deps.EXIT_HASH


# --- show and fetch -------------------------------------------------------------


def test_show_prints_record_and_entries(env, capsys):
    r = env.request()
    assert env.run("resolve", "spark") == 0
    capsys.readouterr()
    assert env.run("show") == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["request_sha256"] == r["sha"]
    assert shown["pinset_sha256"] == req_mod.pinset_sha256(shown["groups"], shown["jar_order"])
    assert shown["jar_order"][0] == lb_deps.coordinate_jar(ICE)


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


def test_fetch_retries_a_short_transfer(served_set, tmp_path, monkeypatch):
    """A server restarting mid-transfer is not tampering."""
    monkeypatch.setattr(lb_deps.time, "sleep", lambda s: None)
    _SetServer.short = [lb_deps.coordinate_jar(BUNDLE)]
    assert _fetch(served_set, tmp_path / "out") == 0
    assert not _SetServer.short


def test_fetch_rejects_a_url_for_another_pinset(served_set, tmp_path):
    url = served_set["url"].rsplit("/", 1)[0] + "/" + "0" * 64
    assert _fetch(served_set, tmp_path / "o", url) == lb_deps.EXIT_HASH


def test_fetch_rejects_an_edited_manifest(served_set, tmp_path):
    m = json.loads(served_set["man"].read_text())
    m["groups"]["jars"][0]["sha256"] = "0" * 64
    served_set["man"].write_text(json.dumps(m))
    assert _fetch(served_set, tmp_path / "o") == lb_deps.EXIT_HASH


def test_fetch_unreachable_server_exits_3(served_set, tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(lb_deps.time, "sleep", lambda s: None)
    url = "http://127.0.0.1:9/sets/" + served_set["url"].rsplit("/", 1)[1]
    assert _fetch(served_set, tmp_path / "o", url) == lb_deps.EXIT_MISSING
    assert "after 3 attempts" in _last_error(capsys.readouterr().out)
    assert not any(p.name.endswith(".part") for p in (tmp_path / "o").iterdir())


def test_fetch_keeps_already_verified_files(served_set, tmp_path, monkeypatch):
    dest = tmp_path / "out"
    assert _fetch(served_set, dest) == 0
    monkeypatch.setattr(lb_deps, "fetch_one", lambda *a: pytest.fail("refetched a verified file"))
    assert _fetch(served_set, dest) == 0


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


# --- errors -----------------------------------------------------------------------


def test_unexpected_error_is_one_line_exit_7(env, capsys, monkeypatch):
    env.request()
    monkeypatch.setattr(
        lb_deps, "resolve_jars", lambda *a: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    assert env.run("resolve", "spark") == lb_deps.EXIT_INTERNAL
    assert _last_error(capsys.readouterr().out) == "LB_DEPS_ERROR internal RuntimeError: boom"


def test_exit_codes_are_distinct():
    codes = list(req_mod.LB_DEPS_EXIT.values())
    assert len(set(codes)) == len(codes) and not {0, 1, 2} & set(codes)


# --- brief-pass fixes -------------------------------------------------------------


def test_duckdb_row_killed_before_the_move_retries_cleanly(env, ext_repo, capsys, monkeypatch):
    """finalise writes the staged manifest.json before the move; a kill in
    between must not leave a file that fails every retry."""
    r = _duck_request(env, ext_repo)
    assert env.run("resolve", "duckdb") == 0
    real = lb_deps.os.rename
    monkeypatch.setattr(
        lb_deps.os, "rename", lambda *a: (_ for _ in ()).throw(RuntimeError("killed"))
    )
    assert env.run("resolve", "spark") == lb_deps.EXIT_INTERNAL
    assert (env.staged(r["sha"]) / "manifest.json").exists()
    monkeypatch.setattr(lb_deps.os, "rename", real)
    capsys.readouterr()
    assert env.run("resolve", "spark") == 0, capsys.readouterr().out
    assert lb_deps.verify_set(env.pointer(r["sha"])["pinset_sha256"]) is None


def test_sigterm_during_verify_keeps_the_set(env, monkeypatch, capsys):
    """A termination is never read as a damaged set."""
    r = env.request()
    assert env.run("resolve", "spark") == 0
    pinset = env.pointer(r["sha"])["pinset_sha256"]

    def term(*a):
        raise lb_deps.Terminated(lb_deps.EXIT_INTERNAL, "terminated by signal 15")

    monkeypatch.setattr(lb_deps, "_walk_files", term)
    assert env.run("resolve", "spark") == lb_deps.EXIT_INTERNAL
    assert "RESOLVE_AGAIN" not in capsys.readouterr().out
    monkeypatch.undo()
    assert env.pointer(r["sha"])["pinset_sha256"] == pinset
    assert (env.root / "sets" / pinset / "manifest.json").exists()


def test_module_info_is_not_a_duplicate_class(env):
    env.mp.setenv("FAKE_MODULE_INFO", "1")
    r = env.request()
    assert env.run("resolve", "spark") == 0
    assert env.pointer(r["sha"])["duplicate_classes"] == []


@pytest.mark.parametrize(
    "line",
    [
        "javax.net.ssl.SSLHandshakeException: PKIX path building failed",
        "java.net.SocketTimeoutException: Read timed out",
        "java.net.SocketException: Connection reset",
        "Host repo1.maven.org: Name or service not known not found. url=https://repo1.maven.org/x.pom",
    ],
)
def test_egress_note_classifies_java_network_failures(line):
    assert lb_deps.egress_note("x\n" + line + "\ny").startswith(" egress: ")


def test_egress_note_ignores_a_plain_not_found():
    assert (
        lb_deps.egress_note(
            "module not found: org.example#nosuch;1.0\n::::: UNRESOLVED DEPENDENCIES"
        )
        == ""
    )


def test_child_out_of_space_exits_6(env, capsys):
    env.request(groups=["jars", "py-reference"], py_reference=PINS, pypi_index=req_mod.PYPI_INDEX)
    env.mp.setenv("FAKE_PIP_FULL", "1")
    assert env.run("resolve", "spark") == lb_deps.EXIT_SPACE


def test_publish_leaves_other_requests_staging(env):
    r = env.request()
    assert env.run("resolve", "spark") == 0
    other = env.root / "staging" / ("e" * 64) / "set" / "duckdb-ext"
    other.mkdir(parents=True)
    own = env.root / "staging" / r["sha"] / "meta"
    own.mkdir(parents=True)
    lb_deps.publish(r["sha"], env.pointer(r["sha"]))
    assert other.exists() and not own.exists()


def test_idle_connections_cannot_hold_shutdown():
    assert 0 < lb_deps.SetHandler.timeout <= 120


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
