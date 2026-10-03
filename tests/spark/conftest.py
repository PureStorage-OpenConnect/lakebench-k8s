"""Spark-tier harness (QA-2): one way to find the jars, one way to skip for a
missing jar, one shared Spark session and one way to run a Spark child.

Jars come only from ``LB_SPARK_TEST_JARS`` (comma-separated jar files; the
legacy ``LB_TEST_ICEBERG_JAR`` is still read, with a deprecation line). Each
jar is classified by file name and checked against the Spark line of the
installed pyspark with the product's own rules (``job.py``), so a 4.0 jar on
the 4.1 line is an error, not a silent skip or a wrong-runtime pass. There
is no ivy-cache fallback.

A test that needs a jar says so with ``@pytest.mark.requires_jars("iceberg")``
(or ``"delta"``, or both). Without the jar it skips with a reason starting
``LB-JARS missing:``; with ``LB_REQUIRE_JARS=1`` it fails instead.

When jars are set, they are put on the classpath of the first JVM this
process starts (``PYSPARK_SUBMIT_ARGS``), and children inherit it, so a
session built later in the same JVM never misses a jar class because an
earlier test started Spark without it.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
SCRIPTS = ROOT / "src" / "lakebench" / "spark" / "scripts"

ICEBERG_EXTENSION = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

JAR_KINDS = ("iceberg", "delta")
# Heap of the pytest process's JVM and of each Spark child. Spark's 1g default
# ran out on the 4.1 line after a few hundred tests in one process ("Not
# enough memory to build and broadcast the table"), and 2g did once in CI.
DRIVER_MEMORY = "3g"
SKIP_PREFIX = "LB-JARS missing:"
# Any skip reason that talks about jars, whatever words a test used.
_JAR_SKIP_RE = re.compile(r"(?i)LB-JARS|jar|iceberg|delta|LB_SPARK_TEST_JARS|LB_TEST_ICEBERG_JAR")
# For an xfail reason only jar words count: a known-bug reason names a
# format often ("LB-034: Delta Q2 ...") and is not a jar excuse.
_JAR_XFAIL_RE = re.compile(r"(?i)LB-JARS|\bjars?\b|LB_SPARK_TEST_JARS|LB_TEST_ICEBERG_JAR")
SKIP_ALLOWANCE = HERE / "skip_allowance.txt"

_ICEBERG_RE = re.compile(
    r"^iceberg-spark-runtime-(?P<spark>\d+\.\d+)_(?P<scala>2\.\d+)-(?P<version>\d[\w.\-]*)\.jar$"
)
_DELTA_RE = re.compile(
    r"^delta-spark(?:_(?P<spark>\d+\.\d+))?_(?P<scala>2\.\d+)-(?P<version>\d[\w.\-]*)\.jar$"
)
_DELTA_STORAGE_RE = re.compile(r"^delta-storage-(?P<version>\d[\w.\-]*)\.jar$")


class JarError(Exception):
    """LB_SPARK_TEST_JARS names a jar that is missing or built for another
    Spark line."""


@dataclass(frozen=True)
class SparkJars:
    leg: str
    iceberg: Path | None = None
    delta: tuple[Path, ...] = ()
    other: tuple[Path, ...] = ()

    def has(self, kind: str) -> bool:
        if kind == "iceberg":
            return self.iceberg is not None
        if kind == "delta":
            names = [p.name for p in self.delta]
            return any(n.startswith("delta-spark") for n in names) and any(
                n.startswith("delta-storage") for n in names
            )
        raise ValueError(f"unknown jar kind {kind!r} (one of {JAR_KINDS})")

    @property
    def paths(self) -> tuple[Path, ...]:
        head = (self.iceberg,) if self.iceberg else ()
        return (*head, *self.delta, *self.other)

    @property
    def classpath(self) -> str:
        """Comma-separated, for ``spark.jars`` and ``LB_SPARK_TEST_JARS``."""
        return ",".join(str(p) for p in self.paths)

    @property
    def python_files(self) -> tuple[Path, ...]:
        """Jars that carry a Python package: delta-spark ships ``delta``
        (``delta.tables``), which ``--packages`` puts on the Python path in
        the pod."""
        return tuple(p for p in self.delta if p.name.startswith("delta-spark"))

    def submit_args(self) -> str:
        """``PYSPARK_SUBMIT_ARGS`` for a JVM with these jars."""
        if not self.paths:
            return f"--driver-memory {DRIVER_MEMORY} pyspark-shell"
        args = f"--driver-memory {DRIVER_MEMORY} --jars {self.classpath}"
        if self.python_files:
            args += " --py-files " + ",".join(str(p) for p in self.python_files)
        return args + " pyspark-shell"

    @property
    def extensions(self) -> str:
        exts = [ICEBERG_EXTENSION] if self.has("iceberg") else []
        exts += [DELTA_EXTENSION] if self.has("delta") else []
        return ",".join(exts)


def pyspark_leg() -> str | None:
    """``"4.0"`` or ``"4.1"`` from the installed pyspark, or None."""
    try:
        import pyspark
    except ImportError:
        return None
    m = re.match(r"(\d+)\.(\d+)", pyspark.__version__)
    return f"{m.group(1)}.{m.group(2)}" if m else None


_DEPRECATION_SHOWN = False


def _jar_entries(env: Mapping[str, str]) -> list[str]:
    global _DEPRECATION_SHOWN
    entries = [e.strip() for e in env.get("LB_SPARK_TEST_JARS", "").split(",") if e.strip()]
    legacy = env.get("LB_TEST_ICEBERG_JAR", "").strip()
    if legacy:
        if not _DEPRECATION_SHOWN:
            print(
                "LB_TEST_ICEBERG_JAR is deprecated; list the jar in LB_SPARK_TEST_JARS",
                file=sys.stderr,
            )
            _DEPRECATION_SHOWN = True
        if os.path.realpath(legacy) not in {os.path.realpath(e) for e in entries}:
            entries.append(legacy)
    return entries


def resolve_jars(env: Mapping[str, str] | None = None, leg: str | None = None) -> SparkJars:
    """Classify the jars in ``LB_SPARK_TEST_JARS`` for the Spark line *leg*
    (default: the installed pyspark). Raises JarError for a path that is not
    a jar file, a jar built for another Spark or Scala line, an Iceberg or
    Delta version the product does not pair with this line (job.py
    ``_FORMAT_VERSION_COMPAT``), or two jars of one kind. Paths come back
    absolute."""
    from lakebench.modules.pipeline_engines.spark.job import (
        _FORMAT_VERSION_COMPAT,
        _delta_spark_artifact,
        iceberg_runtime_suffix_for,
    )

    env = os.environ if env is None else env
    leg = leg or pyspark_leg() or ""
    entries = _jar_entries(env)
    if not entries:
        return SparkJars(leg=leg)
    key = tuple(int(x) for x in leg.split(".")) if leg else None
    compat = _FORMAT_VERSION_COMPAT.get(key, {}) if key else {}
    # Spark 4 is built for Scala 2.13 only.
    scala = "2.13" if key and key[0] >= 4 else None
    iceberg: Path | None = None
    delta: list[Path] = []
    other: list[Path] = []
    delta_versions: set[str] = set()
    for entry in entries:
        if any(c.isspace() for c in entry):
            raise JarError(f"{entry!r}: jar paths with whitespace break PYSPARK_SUBMIT_ARGS")
        path = Path(entry).resolve()
        if not path.is_file() or path.suffix != ".jar":
            raise JarError(f"{entry} is not a jar file")
        name = path.name
        m = _ICEBERG_RE.match(name) or _DELTA_RE.match(name)
        if m and scala and m["scala"] != scala:
            raise JarError(
                f"{name} is built for Scala {m['scala']}; Spark {leg} runs Scala {scala}"
            )
        if m := _ICEBERG_RE.match(name):
            if key is not None and m["version"] not in compat.get("iceberg", []):
                raise JarError(
                    f"{name}: Iceberg {m['version']} does not run on the Spark {leg} line"
                )
            if key is not None:
                want = iceberg_runtime_suffix_for(key, m["version"])
                if m["spark"] != want:
                    raise JarError(
                        f"{name} is built for Spark {m['spark']}; the {leg} line runs "
                        f"iceberg-spark-runtime-{want}_{m['scala']} for Iceberg {m['version']}"
                    )
            if iceberg is not None:
                raise JarError(f"two Iceberg runtime jars: {iceberg.name} and {name}")
            iceberg = path
        elif m := _DELTA_RE.match(name):
            artifact = name[: -len(f"-{m['version']}.jar")]
            want = _delta_spark_artifact(f"_{m['scala']}", m["version"]).split(":")[1]
            if artifact != want:
                raise JarError(f"{name}: Delta {m['version']} is published as {want}")
            if key is not None and m["version"] not in compat.get("delta", []):
                raise JarError(f"{name}: Delta {m['version']} does not run on the Spark {leg} line")
            if any(p.name.startswith("delta-spark") for p in delta):
                raise JarError(f"two delta-spark jars in LB_SPARK_TEST_JARS ({name})")
            delta.append(path)
            delta_versions.add(m["version"])
        elif m := _DELTA_STORAGE_RE.match(name):
            delta.append(path)
            delta_versions.add(m["version"])
        else:
            other.append(path)
    if len(delta_versions) > 1:
        raise JarError(f"delta-spark and delta-storage versions differ: {sorted(delta_versions)}")
    return SparkJars(leg=leg, iceberg=iceberg, delta=tuple(delta), other=tuple(other))


def _jars_or_fail() -> SparkJars:
    try:
        return resolve_jars()
    except JarError as e:
        pytest.fail(f"LB_SPARK_TEST_JARS: {e}", pytrace=False)


def require_jars(*kinds: str) -> SparkJars:
    """Skip (or, with LB_REQUIRE_JARS=1, fail) unless every jar kind is set."""
    jars = _jars_or_fail()
    missing = [k for k in kinds if not jars.has(k)]
    if missing:
        msg = f"{SKIP_PREFIX} {', '.join(missing)}"
        if os.environ.get("LB_REQUIRE_JARS") == "1":
            pytest.fail(f"{msg} (set LB_SPARK_TEST_JARS; LB_REQUIRE_JARS=1)", pytrace=False)
        pytest.skip(msg)
    return jars


# ---------------------------------------------------------------------------
# Shards (--lb-shard K/N): CI splits each Spark leg and order into N jobs.
# ---------------------------------------------------------------------------

SHARD_WEIGHTS = HERE / "shard_weights.json"


def parse_shard(value: str) -> tuple[int, int]:
    """``"K/N"`` as (K, N) with 1 <= K <= N; a usage error otherwise."""
    m = re.fullmatch(r"\s*(\d+)\s*/\s*(\d+)\s*", value)
    if not m or not 1 <= int(m.group(1)) <= int(m.group(2)):
        raise pytest.UsageError(f"--lb-shard takes K/N with 1 <= K <= N, got {value!r}")
    return int(m.group(1)), int(m.group(2))


def read_shard_weights(path: Path = SHARD_WEIGHTS) -> dict[str, float]:
    """Recorded seconds per test file, keyed by the path from the repository
    root (``tests/spark/test_x.py``); written by scripts/spark_shard_weights.py
    from the CI Spark jobs' JUnit reports. Missing file: no weights."""
    if not path.is_file():
        return {}
    data = json.loads(path.read_text())
    return {str(k): float(v) for k, v in data.get("seconds", {}).items()}


def shard_files(files: Iterable[str], n: int, weights: Mapping[str, float]) -> dict[str, int]:
    """Each file's shard (1..n), a function of the file set and the weights
    only, so every shard process computes the same partition and every file
    lands in exactly one shard. Heaviest file first, each to the shard with
    the least recorded time so far (the lower number on a tie); a file with
    no recorded time weighs the median of the recorded ones (1 s if none)."""
    names = sorted(set(files))
    known = sorted(weights[f] for f in names if f in weights)
    default = known[len(known) // 2] if known else 1.0
    load = [0.0] * n
    out: dict[str, int] = {}
    for name in sorted(names, key=lambda f: (-weights.get(f, default), f)):
        target = min(range(n), key=lambda i: (load[i], i))
        load[target] += weights.get(name, default)
        out[name] = target + 1
    return out


def _shard_key(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


# ---------------------------------------------------------------------------
# pytest hooks
# ---------------------------------------------------------------------------


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--lb-reverse",
        action="store_true",
        default=False,
        help="run the collected tests in reverse order (QA-2 order check)",
    )
    parser.addoption(
        "--lb-shard",
        default=None,
        metavar="K/N",
        help="run only the test files in shard K of N (1-based; tests/spark/conftest.py "
        "shard_files). Every file is in exactly one shard; combines with --lb-reverse",
    )


_CONFIG: pytest.Config | None = None


def pytest_configure(config: pytest.Config) -> None:
    global _CONFIG
    _CONFIG = config
    shard = config.getoption("--lb-shard", default=None)
    if shard is not None:
        parse_shard(shard)
    config.addinivalue_line(
        "markers", "requires_jars(*kinds): needs the 'iceberg' and/or 'delta' test jars"
    )
    config.addinivalue_line(
        "markers",
        "spark_static_conf(conf): extra static Spark conf for the module's spark_session "
        "(module-level pytestmark only)",
    )
    config.addinivalue_line(
        "markers",
        "known_bug(id, match=regex, legs=('4.0', '4.1'), reason=''): a known product or "
        "test bug on these Spark lines; the test is a strict xfail there when it fails with "
        "a message matching match, any other failure stays a failure, and a fix turns the run "
        "red until the marker goes",
    )
    if os.environ.get("LB_REQUIRE_JARS") == "1" and pyspark_leg() is None:
        # Every jar module importorskips pyspark, so without it the run would
        # skip them all at collection and still exit 0.
        raise pytest.UsageError("LB_REQUIRE_JARS=1 but pyspark is not importable")
    # Spark's scratch (spark.local.dir) follows TMPDIR instead of /tmp.
    if os.environ.get("TMPDIR"):
        os.environ.setdefault("SPARK_LOCAL_DIRS", os.environ["TMPDIR"])
    # Put the jars on the first JVM's classpath (and every child's). An
    # invalid LB_SPARK_TEST_JARS is reported by the tests that need jars.
    try:
        jars = resolve_jars()
    except JarError:
        return
    preset = os.environ.get("PYSPARK_SUBMIT_ARGS")
    if preset is None:
        os.environ["PYSPARK_SUBMIT_ARGS"] = jars.submit_args()
    elif not all(str(p) in preset for p in jars.paths):
        raise pytest.UsageError(
            "PYSPARK_SUBMIT_ARGS is set without the LB_SPARK_TEST_JARS jars; unset it "
            "(the harness sets it) or add them"
        )


def read_skip_allowance(path: Path = SKIP_ALLOWANCE) -> dict[str, str]:
    """``<nodeid>  <reason>`` per line (blank lines and ``#`` comments
    ignored): the skips a ``LB_REQUIRE_JARS=1`` run may have. The target is
    an empty list."""
    allowed: dict[str, str] = {}
    if not path.is_file():
        return allowed
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        nodeid, _, reason = line.partition(" ")
        allowed[nodeid] = reason.strip()
    return allowed


def _skip_reason(report: pytest.TestReport | pytest.CollectReport) -> str:
    longrepr = report.longrepr
    reason = (
        str(longrepr[2]) if isinstance(longrepr, tuple) and len(longrepr) == 3 else str(longrepr)
    )
    return reason.removeprefix("Skipped: ")


_SKIPS = pytest.StashKey[list[tuple[str, str]]]()
_SKIP_PROBLEMS = pytest.StashKey[list[str]]()


def _record_skip(config: pytest.Config, report: pytest.TestReport | pytest.CollectReport) -> None:
    # An xfail is reported as skipped too. It is not a skip, unless its
    # reason is a jar: an xfail cannot stand in for a missing jar.
    xfail = getattr(report, "wasxfail", None)
    if xfail is not None:
        if _JAR_XFAIL_RE.search(str(xfail)):
            config.stash.setdefault(_SKIPS, []).append((report.nodeid, f"xfail: {xfail}"))
    elif report.skipped:
        config.stash.setdefault(_SKIPS, []).append((report.nodeid, _skip_reason(report)))


@pytest.hookimpl
def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if _CONFIG is not None:
        _record_skip(_CONFIG, report)


@pytest.hookimpl
def pytest_collectreport(report: pytest.CollectReport) -> None:
    if _CONFIG is not None:
        _record_skip(_CONFIG, report)


def skip_problems(skips: list[tuple[str, str]], allowed: dict[str, str]) -> list[str]:
    """The skips a ``LB_REQUIRE_JARS=1`` run must not have: every skip whose
    reason mentions jars (allowed or not), and every skip not in the
    allowance."""
    out = []
    for nodeid, reason in skips:
        if _JAR_SKIP_RE.search(reason):
            out.append(f"{nodeid}: jar skip: {reason}")
        elif nodeid not in allowed:
            out.append(f"{nodeid}: skip not in {SKIP_ALLOWANCE.name}: {reason}")
    return out


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Skip guard, the backstop for tests not on ``requires_jars``: with
    ``LB_REQUIRE_JARS=1`` a skip that mentions jars, or any skip not listed
    in skip_allowance.txt, fails the run."""
    if os.environ.get("LB_REQUIRE_JARS") != "1":
        return
    problems = skip_problems(session.config.stash.get(_SKIPS, []), read_skip_allowance())
    session.config.stash[_SKIP_PROBLEMS] = problems
    if problems and session.exitstatus in (pytest.ExitCode.OK, pytest.ExitCode.NO_TESTS_COLLECTED):
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_terminal_summary(terminalreporter: Any, exitstatus: int, config: pytest.Config) -> None:
    problems = config.stash.get(_SKIP_PROBLEMS, [])
    if problems:
        terminalreporter.section("LB_REQUIRE_JARS=1: skips that fail the run", sep="=", red=True)
        for line in problems:
            terminalreporter.line(line)


_KNOWN_BUG_ID = re.compile(r"^(LB|QR)-\d+$")
_LEGS = ("4.0", "4.1")


_KNOWN_BUG = pytest.StashKey[tuple[str, str]]()


def known_bug_xfail(mark: pytest.Mark, leg: str | None) -> pytest.MarkDecorator | None:
    """The strict xfail a ``known_bug`` mark means on the Spark line *leg*,
    or None when the bug is not known on that line. The id is a BUGS.md
    ``LB-NNN``, or the ``QR-NN`` work item that fixes a stale test; *match*
    is a regex the failure must show (see pytest_runtest_makereport)."""
    if len(mark.args) != 1 or not _KNOWN_BUG_ID.match(str(mark.args[0])):
        raise pytest.UsageError(f"known_bug takes one LB-NNN or QR-NN id, got {mark.args}")
    legs = tuple(mark.kwargs.get("legs", _LEGS))
    unknown = set(legs) - set(_LEGS)
    if unknown or set(mark.kwargs) - {"legs", "reason", "match"} or not mark.kwargs.get("match"):
        raise pytest.UsageError(
            f"known_bug {mark.args[0]}: needs match= and takes legs= and reason=, got {mark.kwargs}"
        )
    try:
        re.compile(mark.kwargs["match"])
    except re.error as err:
        raise pytest.UsageError(f"known_bug {mark.args[0]}: bad match regex: {err}") from err
    if leg not in legs:
        return None
    why = mark.kwargs.get("reason", "")
    reason = f"{mark.args[0]} on Spark {leg}" + (f": {why}" if why else "")
    return pytest.mark.xfail(strict=True, reason=reason)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    leg = pyspark_leg()
    for item in items:
        for mark in item.iter_markers("known_bug"):
            xfail = known_bug_xfail(mark, leg)
            if xfail is not None:
                item.add_marker(xfail)
                item.stash[_KNOWN_BUG] = (mark.args[0], mark.kwargs["match"])
    shard = config.getoption("--lb-shard", default=None)
    if shard is not None:
        k, n = parse_shard(shard)
        root = config.rootpath
        keys = {id(item): _shard_key(item.path, root) for item in items}
        # The files on disk join the collected ones, so the partition is the
        # same whichever files this process was asked to collect.
        on_disk = {_shard_key(p, root) for p in HERE.rglob("test_*.py")}
        assignment = shard_files(on_disk | set(keys.values()), n, read_shard_weights())
        dropped = [item for item in items if assignment[keys[id(item)]] != k]
        if dropped:
            config.hook.pytest_deselected(items=dropped)
            items[:] = [item for item in items if assignment[keys[id(item)]] == k]
    if config.getoption("--lb-reverse", default=False):
        items.reverse()


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Any:
    """A known_bug xfail only covers the failure it names: a test that
    fails with a message that does not match the marker's *match* (another
    bug, a missing jar, a harness break) is reported as a failure."""
    rep = yield
    bug = item.stash.get(_KNOWN_BUG, None)
    if bug is None or not hasattr(rep, "wasxfail") or call.excinfo is None:
        return rep
    # The exception only: the report's traceback shows the test's source,
    # decorators included, so it would always contain the match text.
    text = call.excinfo.exconly()
    if call.excinfo.errisinstance(pytest.fail.Exception) and SKIP_PREFIX in text:
        why = "it needs a jar that is missing"
    elif re.search(bug[1], text):
        return rep
    else:
        why = f"not with /{bug[1]}/"
    rep.outcome = "failed"
    del rep.wasxfail
    rep.longrepr = f"known_bug {bug[0]} does not cover this failure ({why}):\n{rep.longrepr}"
    return rep


def _jar_kinds(marks: Iterator[pytest.Mark]) -> set[str]:
    kinds: set[str] = set()
    for mark in marks:
        bad = [k for k in mark.args if k not in JAR_KINDS]
        if bad or not mark.args:
            pytest.fail(f"requires_jars takes {JAR_KINDS}, got {mark.args}", pytrace=False)
        kinds.update(mark.args)
    return kinds


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    # tryfirst: decide before any fixture (a module-scoped one may start a
    # Spark child) is set up.
    for node, _mark in item.iter_markers_with_node("spark_static_conf"):
        if not isinstance(node, pytest.Module):
            pytest.fail(
                "spark_static_conf applies to the module's spark_session: put it in the "
                f"module's pytestmark, not on {node.nodeid}",
                pytrace=False,
            )
    kinds = _jar_kinds(item.iter_markers("requires_jars"))
    if kinds:
        require_jars(*sorted(kinds))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark_jars() -> SparkJars:
    """The resolved test jars for this pyspark line (possibly none)."""
    return _jars_or_fail()


def _session_conf(jars: SparkJars, kinds: set[str], warehouse: Path) -> dict[str, str]:
    """The product's session shape for the formats *kinds*: one SQL
    extension per format (job.py), and DeltaCatalog as spark_catalog only
    for Delta."""
    conf = {
        "spark.master": "local[2]",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "2",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.warehouse.dir": f"file://{warehouse}",
    }
    if jars.paths:
        conf["spark.jars"] = jars.classpath
    if jars.python_files:
        conf["spark.submit.pyFiles"] = ",".join(str(p) for p in jars.python_files)
    exts = [ICEBERG_EXTENSION] if "iceberg" in kinds else []
    exts += [DELTA_EXTENSION] if "delta" in kinds else []
    if exts:
        conf["spark.sql.extensions"] = ",".join(exts)
    if "delta" in kinds:
        conf["spark.sql.catalog.spark_catalog"] = DELTA_CATALOG
    return conf


_CATALOGS: dict[str, str] = {}


_MODULE_KINDS = pytest.StashKey[dict[str, set[str]]]()


def pytest_itemcollected(item: pytest.Item) -> None:
    # Every collected test, before -k or -m deselect any, so a module's
    # session shape does not depend on -k or -m. (A test named by node id
    # alone is all pytest collects of its module.)
    module = item.getparent(pytest.Module)
    if module is not None:
        kinds = item.config.stash.setdefault(_MODULE_KINDS, {})
        kinds.setdefault(module.nodeid, set()).update(
            _jar_kinds(item.iter_markers("requires_jars"))
        )


def _module_jar_kinds(request: pytest.FixtureRequest) -> set[str]:
    """Every jar kind a test of this module declares (module pytestmark and
    test markers alike), over all its collected tests."""
    module = request.node
    kinds = _jar_kinds(module.iter_markers("requires_jars"))
    return kinds | request.config.stash.get(_MODULE_KINDS, {}).get(module.nodeid, set())


def static_conf_mismatch(
    have: Mapping[str, str], want: Mapping[str, str], extra: Iterable[str] = ()
) -> dict[str, tuple[str | None, str | None]]:
    """The keys where a started session's SparkConf (*have*) differs from
    the conf spark_session asked for (*want*): the master, the extensions,
    every ``spark.sql.catalog.*`` key on either side (so a catalog left in
    the JVM by an earlier session shows), and *extra*."""
    keys = {"spark.master", "spark.sql.extensions", "spark.sql.catalog.spark_catalog", *extra}
    keys |= {k for k in (*have, *want) if k.startswith("spark.sql.catalog.")}
    return {k: (have.get(k), want.get(k)) for k in sorted(keys) if have.get(k) != want.get(k)}


@pytest.fixture(scope="session", autouse=True)
def _bare_spark_gateway() -> None:
    """Start the JVM once with only the jars (PYSPARK_SUBMIT_ARGS) and no
    session conf. pyspark passes the first builder's conf to spark-submit,
    where it becomes JVM system properties that every later SparkConf in
    the process reads back; a gateway started bare keeps one module's
    session shape (DeltaCatalog as spark_catalog, say) out of the next."""
    try:
        from pyspark import SparkContext
    except ImportError:
        return
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    SparkContext._ensure_initialized()


@pytest.fixture(scope="module")
def spark_session(
    request: pytest.FixtureRequest, spark_jars: SparkJars, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Any]:
    """A Spark session for one test module, stopped at the module's end, so
    the next module (in either order, see ``--lb-reverse``) never inherits
    it. It has the jars, and the product's session shape for the formats
    the module's tests declare with ``requires_jars``: the Iceberg or Delta
    SQL extension, and DeltaCatalog as spark_catalog for Delta. UTC,
    ``local[2]``. A module that needs more static conf (another master, say)
    adds ``pytestmark = pytest.mark.spark_static_conf({...})``; add Iceberg
    catalogs with the ``iceberg_catalog`` fixture."""
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    active = SparkSession.getActiveSession()
    running = SparkContext._active_spark_context
    if (active is not None and active.sparkContext._jsc is not None) or running is not None:
        pytest.fail(
            "a Spark session from an earlier fixture is still running; stop it in that "
            "fixture's teardown so spark_session starts with this module's conf",
            pytrace=False,
        )
    warehouse = tmp_path_factory.mktemp("spark-session-warehouse")
    conf = _session_conf(spark_jars, _module_jar_kinds(request), warehouse)
    for mark in request.node.iter_markers("spark_static_conf"):
        conf.update(mark.args[0] if mark.args else mark.kwargs)
    builder = SparkSession.builder
    for k, v in conf.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    _CATALOGS.clear()
    try:
        # A context left running elsewhere, or conf an earlier session left
        # in the JVM, would hand back other static conf; check the keys that
        # decide what the tests see: the master, the extensions and every
        # catalog, whichever side has them.
        extra: set[str] = set()
        for mark in request.node.iter_markers("spark_static_conf"):
            extra |= set(mark.args[0] if mark.args else mark.kwargs)
        bad = static_conf_mismatch(dict(spark.sparkContext.getConf().getAll()), conf, extra)
        if bad:
            pytest.fail(f"spark_session did not take its conf: {bad}", pytrace=False)
        yield spark
    finally:
        spark.stop()


def _register_iceberg_catalog(
    spark: Any, name: str, warehouse: Path, *, cache_enabled: bool | None = None
) -> str:
    """Register a Hadoop Iceberg catalog *name* at *warehouse* on the
    module's spark_session. Spark caches a catalog by name once it is used,
    so registering one name twice in a module with another warehouse fails
    instead of silently reading the first warehouse. *cache_enabled* sets
    the catalog's ``cache-enabled`` property; None leaves Iceberg's default."""
    where = f"file://{warehouse}"
    if _CATALOGS.get(name, where) != where:
        pytest.fail(
            f"Iceberg catalog {name!r} is already registered at {_CATALOGS[name]}",
            pytrace=False,
        )
    _CATALOGS[name] = where
    spark.conf.set(f"spark.sql.catalog.{name}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{name}.type", "hadoop")
    if cache_enabled is not None:
        spark.conf.set(f"spark.sql.catalog.{name}.cache-enabled", str(cache_enabled).lower())
    spark.conf.set(f"spark.sql.catalog.{name}.warehouse", where)
    return name


@pytest.fixture(scope="session")
def iceberg_catalog() -> Callable[..., str]:
    """``iceberg_catalog(spark, name, warehouse, cache_enabled=None)``
    registers a Hadoop Iceberg catalog on a spark_session."""
    return _register_iceberg_catalog


def _failure_text(stdout: str, stderr: str, lines: int = 60) -> str:
    text = (stdout or "").rstrip("\n") + "\n" + (stderr or "")
    tail = text.splitlines()[-lines:]
    caused = [ln.strip() for ln in text.splitlines() if ln.lstrip().startswith("Caused by:")]
    out = "\n".join(tail)
    if caused:
        out += "\n--- Caused by ---\n" + "\n".join(dict.fromkeys(caused))
    return out


def _child_log(factory: pytest.TempPathFactory, argv: list[str], body: str) -> Path:
    test = os.environ.get("PYTEST_CURRENT_TEST", "spark-child").split(" ")[0]
    name = re.sub(r"[^A-Za-z0-9_.-]+", "_", test)[-80:]
    log = factory.mktemp(name, numbered=True) / "spark-subprocess.log"
    log.write_text(f"$ {' '.join(argv)}\n{body}")
    return log


@pytest.fixture(scope="session")
def spark_subprocess(
    spark_jars: SparkJars, tmp_path_factory: pytest.TempPathFactory
) -> Callable[..., subprocess.CompletedProcess[str]]:
    """Run a Spark child: ``spark_subprocess(script, *args, env=None,
    timeout=900, check=True)``. *script* is a path, or ``"-c"`` followed by
    the code in *args*. The child gets ``PYSPARK_PYTHON``, the test jars
    (``LB_SPARK_TEST_JARS`` and ``PYSPARK_SUBMIT_ARGS``, so its JVM has
    every jar whatever its argv names) and a PYTHONPATH of the Spark
    scripts, tests/spark, src and the delta-spark jar (its ``delta``
    package), followed by any PYTHONPATH in *env* or the environment, so
    the child and its executors import the scripts by plain name. Other
    keys in *env* override the harness. When the child exits non-zero or
    times out, its whole output goes to ``spark-subprocess.log`` in a temp
    directory named after the test, and the failure shows the last 60
    lines plus every ``Caused by:`` line. Session-scoped, so module-scoped
    fixtures can use it."""

    def run(
        script: str | os.PathLike[str],
        *args: str | os.PathLike[str],
        env: Mapping[str, str] | None = None,
        timeout: float = 900,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        extra = dict(env or {})
        child = dict(os.environ)
        child["PYSPARK_PYTHON"] = sys.executable
        child["PYSPARK_DRIVER_PYTHON"] = sys.executable
        if spark_jars.paths:
            child["LB_SPARK_TEST_JARS"] = spark_jars.classpath
            child.setdefault("PYSPARK_SUBMIT_ARGS", spark_jars.submit_args())
        path = [str(SCRIPTS), str(HERE), str(ROOT / "src")]
        path += [str(p) for p in spark_jars.python_files]
        tail = extra.pop("PYTHONPATH", None) or child.get("PYTHONPATH")
        if tail:
            path.append(tail)
        child.update(extra)
        child["PYTHONPATH"] = os.pathsep.join(path)
        argv = [sys.executable, str(script), *(str(a) for a in args)]
        try:
            proc = subprocess.run(argv, capture_output=True, text=True, env=child, timeout=timeout)
        except subprocess.TimeoutExpired as e:
            out = (
                e.stdout.decode(errors="replace")
                if isinstance(e.stdout, bytes)
                else (e.stdout or "")
            )
            err = (
                e.stderr.decode(errors="replace")
                if isinstance(e.stderr, bytes)
                else (e.stderr or "")
            )
            log = _child_log(
                tmp_path_factory,
                argv,
                f"timed out after {timeout}s\n--- stdout ---\n{out}\n--- stderr ---\n{err}",
            )
            pytest.fail(
                f"Spark child timed out after {timeout}s (full log: {log})\n"
                + _failure_text(out, err),
                pytrace=False,
            )
        if check and proc.returncode != 0:
            log = _child_log(
                tmp_path_factory,
                argv,
                f"exit {proc.returncode}\n--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}",
            )
            pytest.fail(
                f"Spark child exited {proc.returncode} (full log: {log})\n"
                + _failure_text(proc.stdout, proc.stderr),
                pytrace=False,
            )
        return proc

    return run
