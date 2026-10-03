"""The Spark-tier harness in tests/spark/conftest.py (QA-2): jar resolution
against the product's own rules, the one jar skip (or failure), the
subprocess failure text and the reverse-order option. Unit tier: no pyspark
or JVM needed."""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

pytest_plugins = ["pytester"]

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "tests" / "spark" / "conftest.py"


@pytest.fixture(scope="module")
def harness():
    spec = importlib.util.spec_from_file_location("lb_spark_harness", HARNESS)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["lb_spark_harness"] = mod  # dataclasses resolve the module by name
    spec.loader.exec_module(mod)
    return mod


def _jars(tmp_path: Path, *names: str) -> str:
    for n in names:
        (tmp_path / n).write_bytes(b"")
    return ",".join(str(tmp_path / n) for n in names)


LEG_40 = (
    "iceberg-spark-runtime-4.0_2.13-1.11.0.jar",
    "delta-spark_2.13-4.0.0.jar",
    "delta-storage-4.0.0.jar",
)
LEG_41 = (
    "iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
    "delta-spark_4.1_2.13-4.1.0.jar",
    "delta-storage-4.1.0.jar",
)


@pytest.mark.parametrize(("leg", "names"), [("4.0", LEG_40), ("4.1", LEG_41)])
def test_product_default_jars_resolve_on_their_line(harness, tmp_path, leg, names):
    jars = harness.resolve_jars({"LB_SPARK_TEST_JARS": _jars(tmp_path, *names)}, leg=leg)
    assert jars.has("iceberg") and jars.has("delta")
    assert jars.iceberg is not None and jars.iceberg.name == names[0]
    assert [p.name for p in jars.python_files] == [names[1]]
    assert jars.classpath.split(",") == [str(tmp_path / n) for n in names]
    assert harness.ICEBERG_EXTENSION in jars.extensions
    assert harness.DELTA_EXTENSION in jars.extensions


def test_a_4_0_iceberg_runtime_on_the_4_1_line_is_refused(harness, tmp_path):
    """Iceberg 1.11.0 has a native 4.1 runtime, so the product never uses the
    4.0 jar on 4.1 (job.py iceberg_runtime_suffix_for)."""
    env = {"LB_SPARK_TEST_JARS": _jars(tmp_path, LEG_40[0])}
    with pytest.raises(harness.JarError, match="built for Spark 4.0"):
        harness.resolve_jars(env, leg="4.1")


def test_borrowed_4_0_runtime_is_accepted_where_the_product_borrows_it(harness, tmp_path):
    """Iceberg 1.10.1 has no 4.1 runtime; the product borrows the 4.0 jar."""
    env = {"LB_SPARK_TEST_JARS": _jars(tmp_path, "iceberg-spark-runtime-4.0_2.13-1.10.1.jar")}
    assert harness.resolve_jars(env, leg="4.1").has("iceberg")


@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("delta-spark_2.13-4.1.0.jar", "published as delta-spark_4.1_2.13"),
        ("delta-spark_2.13-4.0.0.jar", "does not run on the Spark 4.1 line"),
    ],
)
def test_wrong_delta_jar_is_refused(harness, tmp_path, name, message):
    env = {"LB_SPARK_TEST_JARS": _jars(tmp_path, name)}
    with pytest.raises(harness.JarError, match=message):
        harness.resolve_jars(env, leg="4.1")


@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("iceberg-spark-runtime-4.0_2.12-1.11.0.jar", "Scala 2.12"),
        ("delta-spark_2.12-4.0.0.jar", "Scala 2.12"),
        ("iceberg-spark-runtime-4.0_2.13-1.9.1.jar", "Iceberg 1.9.1 does not run"),
    ],
)
def test_jars_the_product_would_not_run_are_refused(harness, tmp_path, name, message):
    env = {"LB_SPARK_TEST_JARS": _jars(tmp_path, name)}
    with pytest.raises(harness.JarError, match=message):
        harness.resolve_jars(env, leg="4.0")


def test_paths_are_made_absolute_and_whitespace_is_refused(harness, tmp_path, monkeypatch):
    _jars(tmp_path, LEG_40[0])
    monkeypatch.chdir(tmp_path)
    jars = harness.resolve_jars({"LB_SPARK_TEST_JARS": f"./{LEG_40[0]}"}, leg="4.0")
    assert jars.iceberg == (tmp_path / LEG_40[0]).resolve()
    spaced = tmp_path / "a b"
    spaced.mkdir()
    with pytest.raises(harness.JarError, match="whitespace"):
        harness.resolve_jars({"LB_SPARK_TEST_JARS": _jars(spaced, LEG_40[0])}, leg="4.0")


def test_mismatched_delta_storage_is_refused(harness, tmp_path):
    env = {"LB_SPARK_TEST_JARS": _jars(tmp_path, LEG_41[1], "delta-storage-4.0.0.jar")}
    with pytest.raises(harness.JarError, match="versions differ"):
        harness.resolve_jars(env, leg="4.1")


def test_missing_path_and_directory_are_refused(harness, tmp_path):
    with pytest.raises(harness.JarError, match="is not a jar file"):
        harness.resolve_jars({"LB_SPARK_TEST_JARS": str(tmp_path / "gone.jar")}, leg="4.0")
    with pytest.raises(harness.JarError, match="is not a jar file"):
        harness.resolve_jars({"LB_SPARK_TEST_JARS": str(tmp_path)}, leg="4.0")


def test_two_iceberg_runtimes_are_refused(harness, tmp_path):
    names = (LEG_40[0], "iceberg-spark-runtime-4.0_2.13-1.10.1.jar")
    with pytest.raises(harness.JarError, match="two Iceberg runtime jars"):
        harness.resolve_jars({"LB_SPARK_TEST_JARS": _jars(tmp_path, *names)}, leg="4.0")


def test_delta_needs_both_jars(harness, tmp_path):
    env = {"LB_SPARK_TEST_JARS": _jars(tmp_path, LEG_40[1])}
    assert not harness.resolve_jars(env, leg="4.0").has("delta")


def test_legacy_iceberg_variable_is_merged_once(harness, tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(harness, "_DEPRECATION_SHOWN", False)
    jar = _jars(tmp_path, LEG_40[0])
    env = {"LB_SPARK_TEST_JARS": jar, "LB_TEST_ICEBERG_JAR": jar}
    assert harness.resolve_jars(env, leg="4.0").paths == (Path(jar),)
    harness.resolve_jars({"LB_TEST_ICEBERG_JAR": jar}, leg="4.0")
    assert capsys.readouterr().err.count("LB_TEST_ICEBERG_JAR is deprecated") == 1


def test_no_jars_resolve_to_nothing(harness):
    jars = harness.resolve_jars({}, leg="4.0")
    assert jars.paths == () and not jars.has("iceberg") and jars.extensions == ""


def test_submit_args_put_the_delta_python_package_on_the_path(harness, tmp_path):
    jars = harness.resolve_jars({"LB_SPARK_TEST_JARS": _jars(tmp_path, *LEG_40)}, leg="4.0")
    args = jars.submit_args()
    assert args.startswith(f"--driver-memory 3g --jars {jars.classpath} ")
    assert f"--py-files {tmp_path / LEG_40[1]}" in args and args.endswith(" pyspark-shell")


def test_failure_text_keeps_every_caused_by_line(harness):
    out = "\n".join(f"line {i}" for i in range(200))
    err = "Caused by: java.lang.ClassNotFoundException: X\n" + "noise\n" * 100
    text = harness._failure_text(out, err)
    # The last 60 lines are stderr noise; the root cause above them is kept.
    assert "line 199" not in text and text.count("noise") == 60
    assert text.endswith("Caused by: java.lang.ClassNotFoundException: X")
    short = harness._failure_text("only line", "")
    assert short.strip() == "only line"


# --- the one jar skip, through a real pytest session ------------------------


def _session(pytester, monkeypatch, body: str, *args: str, **env: str):
    pytester.makeconftest(HARNESS.read_text())
    pytester.makepyfile(body)
    monkeypatch.delenv("LB_SPARK_TEST_JARS", raising=False)
    monkeypatch.delenv("LB_TEST_ICEBERG_JAR", raising=False)
    monkeypatch.delenv("LB_REQUIRE_JARS", raising=False)
    # Set by an outer Spark-tier run (pytest_configure); the inner session
    # sets its own.
    monkeypatch.delenv("PYSPARK_SUBMIT_ARGS", raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    path = [str(ROOT), str(ROOT / "src")]
    if importlib.util.find_spec("pyspark") is None:
        # The unit tier has no pyspark; the harness only reads its version.
        fake = pytester.path / "fake-pyspark" / "pyspark"
        fake.mkdir(parents=True, exist_ok=True)
        (fake / "__init__.py").write_text('__version__ = "4.0.1"\n')
        path.insert(0, str(fake.parent))
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(path))
    return pytester.runpytest_subprocess("-p", "no:cacheprovider", "-rs", *args)


_NEEDS_ICEBERG = """
import pytest

@pytest.mark.requires_jars("iceberg")
def test_needs_iceberg():
    pass
"""


def test_missing_jar_skips_with_the_one_prefix(pytester, monkeypatch):
    res = _session(pytester, monkeypatch, _NEEDS_ICEBERG)
    res.assert_outcomes(skipped=1)
    assert "LB-JARS missing: iceberg" in res.stdout.str()
    assert res.ret == 0


def test_missing_jar_fails_under_require(pytester, monkeypatch):
    """The in-tree form of "a removed jar turns the job red"."""
    res = _session(pytester, monkeypatch, _NEEDS_ICEBERG, LB_REQUIRE_JARS="1")
    assert res.ret == 1
    assert "LB-JARS missing: iceberg" in res.stdout.str()
    assert res.parseoutcomes().get("skipped", 0) == 0


def test_present_jar_runs_the_test(harness, pytester, monkeypatch, tmp_path):
    names = LEG_41 if harness.pyspark_leg() == "4.1" else LEG_40  # fake pyspark is 4.0
    body = _NEEDS_ICEBERG.replace(
        "def test_needs_iceberg():\n    pass",
        "def test_needs_iceberg(spark_jars):\n    assert spark_jars.has('iceberg')",
    )
    res = _session(
        pytester,
        monkeypatch,
        body,
        LB_SPARK_TEST_JARS=_jars(tmp_path, names[0]),
        LB_REQUIRE_JARS="1",
    )
    res.assert_outcomes(passed=1)


def test_bad_jar_variable_fails_instead_of_skipping(pytester, monkeypatch, tmp_path):
    res = _session(
        pytester, monkeypatch, _NEEDS_ICEBERG, LB_SPARK_TEST_JARS=str(tmp_path / "missing.jar")
    )
    assert res.ret == 1
    assert "is not a jar file" in res.stdout.str()


def test_lb_reverse_runs_the_tests_backwards(pytester, monkeypatch):
    body = "def test_1():\n    pass\n\ndef test_2():\n    pass\n"
    forward = _session(pytester, monkeypatch, body, "-v")
    backward = _session(pytester, monkeypatch, body, "-v", "--lb-reverse")
    fwd, bwd = forward.stdout.str(), backward.stdout.str()
    assert fwd.index("::test_1") < fwd.index("::test_2")
    assert bwd.index("::test_2") < bwd.index("::test_1")


def test_require_without_pyspark_refuses_to_start(pytester, monkeypatch):
    pytester.makeconftest(HARNESS.read_text())
    pytester.makepyfile(_NEEDS_ICEBERG)
    # A pyspark that cannot be imported, whatever this interpreter has.
    broken = pytester.path / "broken" / "pyspark"
    broken.mkdir(parents=True)
    (broken / "__init__.py").write_text("raise ImportError('no pyspark here')\n")
    path = [str(broken.parent), str(ROOT), str(ROOT / "src")]
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(path))
    monkeypatch.setenv("LB_REQUIRE_JARS", "1")
    monkeypatch.delenv("LB_SPARK_TEST_JARS", raising=False)
    res = pytester.runpytest_subprocess("-p", "no:cacheprovider")
    assert res.ret == pytest.ExitCode.USAGE_ERROR
    assert "pyspark is not importable" in res.stderr.str()


def test_spark_static_conf_on_a_test_function_fails(pytester, monkeypatch):
    body = (
        "import pytest\n\n"
        "@pytest.mark.spark_static_conf({'spark.master': 'local[1]'})\n"
        "def test_x():\n    pass\n"
    )
    res = _session(pytester, monkeypatch, body)
    assert res.ret == 1
    assert "put it in the module's pytestmark" in res.stdout.str()


def test_child_timeout_keeps_its_output(pytester, monkeypatch):
    body = (
        "def test_slow(spark_subprocess):\n"
        "    spark_subprocess('-c', 'import time; print(\"started\", flush=True); "
        "time.sleep(30)', timeout=2)\n"
    )
    res = _session(pytester, monkeypatch, body)
    out = res.stdout.str()
    assert res.ret == 1 and "timed out after 2s" in out and "started" in out
    logs = list(pytester.path.parent.rglob("spark-subprocess.log")) + list(
        Path(out.split("full log: ")[1].split(")")[0]).parent.glob("*.log")
    )
    assert logs and "started" in logs[-1].read_text()


def test_child_failure_shows_the_cause(pytester, monkeypatch):
    body = (
        "def test_fails(spark_subprocess):\n"
        "    spark_subprocess('-c', 'import sys; print(\"Caused by: boom\"); sys.exit(3)')\n"
    )
    res = _session(pytester, monkeypatch, body)
    out = res.stdout.str()
    assert res.ret == 1 and "Spark child exited 3" in out and "Caused by: boom" in out


# --- the skip guard (LB_REQUIRE_JARS=1) --------------------------------------


_FREE_TEXT_SKIP = """
import pytest

def test_old_style():
    pytest.skip("no iceberg-spark-runtime jar (set LB_TEST_ICEBERG_JAR)")
"""


def test_free_text_jar_skip_fails_under_require(pytester, monkeypatch):
    """A test not yet on requires_jars still cannot skip for a jar."""
    res = _session(pytester, monkeypatch, _FREE_TEXT_SKIP, LB_REQUIRE_JARS="1")
    assert res.ret == 1
    out = res.stdout.str()
    assert "skips that fail the run" in out and "test_old_style: jar skip" in out


def test_free_text_jar_skip_only_skips_without_require(pytester, monkeypatch):
    res = _session(pytester, monkeypatch, _FREE_TEXT_SKIP)
    res.assert_outcomes(skipped=1)
    assert res.ret == 0


def test_module_level_jar_skip_fails_under_require(pytester, monkeypatch):
    body = 'import pytest\npytest.skip("Iceberg jars not set", allow_module_level=True)\n'
    res = _session(pytester, monkeypatch, body, LB_REQUIRE_JARS="1")
    assert res.ret == 1
    assert "jar skip: Iceberg jars not set" in res.stdout.str()


_OTHER_SKIP = """
import pytest

def test_other():
    pytest.skip("needs a GPU")
"""


def test_skip_outside_the_allowance_fails_under_require(pytester, monkeypatch):
    res = _session(pytester, monkeypatch, _OTHER_SKIP, LB_REQUIRE_JARS="1")
    assert res.ret == 1
    assert "skip not in skip_allowance.txt: needs a GPU" in res.stdout.str()


def test_skip_in_the_allowance_passes_under_require(pytester, monkeypatch):
    pytester.path.joinpath("skip_allowance.txt").write_text(
        "# comment\n\ntest_skip_in_the_allowance_passes_under_require.py::test_other  GPU\n"
    )
    res = _session(pytester, monkeypatch, _OTHER_SKIP, LB_REQUIRE_JARS="1")
    res.assert_outcomes(skipped=1)
    assert res.ret == 0


def test_jar_skip_in_the_allowance_still_fails(pytester, monkeypatch):
    pytester.path.joinpath("skip_allowance.txt").write_text(
        "test_jar_skip_in_the_allowance_still_fails.py::test_old_style  jar\n"
    )
    res = _session(pytester, monkeypatch, _FREE_TEXT_SKIP, LB_REQUIRE_JARS="1")
    assert res.ret == 1


def test_xfail_is_not_a_skip(pytester, monkeypatch):
    body = "import pytest\n\n@pytest.mark.xfail(strict=True)\ndef test_x():\n    assert False\n"
    res = _session(pytester, monkeypatch, body, LB_REQUIRE_JARS="1")
    res.assert_outcomes(xfailed=1)
    assert res.ret == 0


def test_skip_allowance_in_the_tree_is_empty(harness):
    """The target is no allowed skip at all (QA-2: 0 jar skips on both legs)."""
    assert harness.read_skip_allowance() == {}


def test_xfail_with_a_jar_reason_fails_under_require(pytester, monkeypatch):
    body = (
        "import pytest\n\n"
        "def test_imperative():\n    pytest.xfail('no iceberg-spark-runtime jar')\n\n"
        "@pytest.mark.xfail(True, reason='no Delta jar', run=False)\n"
        "def test_not_run():\n    pass\n"
    )
    res = _session(pytester, monkeypatch, body, LB_REQUIRE_JARS="1")
    assert res.ret == 1
    out = res.stdout.str()
    assert "test_imperative: jar skip: xfail:" in out and "test_not_run: jar skip: xfail:" in out


def test_spark_static_conf_on_a_class_fails(pytester, monkeypatch):
    body = (
        "import pytest\n\n"
        "@pytest.mark.spark_static_conf({'spark.master': 'local[1]'})\n"
        "class TestX:\n    def test_x(self):\n        pass\n"
    )
    res = _session(pytester, monkeypatch, body)
    assert res.ret == 1
    assert "not on test_spark_static_conf_on_a_class_fails.py::TestX" in res.stdout.str()


def test_submit_args_carry_the_jars_and_a_foreign_preset_is_refused(
    harness, pytester, monkeypatch, tmp_path
):
    names = LEG_41 if harness.pyspark_leg() == "4.1" else LEG_40
    jars = _jars(tmp_path, *names)
    body = (
        "import os\n\n"
        "def test_args():\n"
        "    args = os.environ['PYSPARK_SUBMIT_ARGS']\n"
        "    assert args.startswith('--driver-memory 3g --jars ')\n"
        "    assert args.endswith(' pyspark-shell')\n"
        "    assert '--py-files ' in args and 'delta-spark' in args.split('--py-files ')[1]\n"
    )
    res = _session(pytester, monkeypatch, body, LB_SPARK_TEST_JARS=jars)
    res.assert_outcomes(passed=1)
    res = _session(
        pytester, monkeypatch, body, LB_SPARK_TEST_JARS=jars, PYSPARK_SUBMIT_ARGS="pyspark-shell"
    )
    assert res.ret == pytest.ExitCode.USAGE_ERROR
    assert "PYSPARK_SUBMIT_ARGS is set without the LB_SPARK_TEST_JARS jars" in res.stderr.str()


def test_session_conf_is_the_product_shape_per_format(harness, tmp_path):
    jars = harness.SparkJars(leg="4.0")
    ice = harness._session_conf(jars, {"iceberg"}, tmp_path)
    assert ice["spark.sql.extensions"] == harness.ICEBERG_EXTENSION
    assert "spark.sql.catalog.spark_catalog" not in ice
    both = harness._session_conf(jars, {"iceberg", "delta"}, tmp_path)
    assert both["spark.sql.extensions"].split(",") == [
        harness.ICEBERG_EXTENSION,
        harness.DELTA_EXTENSION,
    ]
    assert both["spark.sql.catalog.spark_catalog"] == harness.DELTA_CATALOG
    plain = harness._session_conf(jars, set(), tmp_path)
    assert "spark.sql.extensions" not in plain and plain["spark.sql.session.timeZone"] == "UTC"


def test_child_env_and_pythonpath_precedence(pytester, monkeypatch):
    body = (
        "import os, sys\n\n"
        "def test_env(spark_subprocess):\n"
        '    code = \'import os; print(os.environ["PYTHONPATH"]); print(os.environ["X"])\'\n'
        "    r = spark_subprocess('-c', code, env={'PYTHONPATH': '/extra', 'X': 'mine'})\n"
        "    path, x = r.stdout.splitlines()[:2]\n"
        "    parts = path.split(os.pathsep)\n"
        "    assert parts[0].endswith('spark/scripts') and parts[-1] == '/extra'\n"
        "    assert x == 'mine'\n"
    )
    res = _session(pytester, monkeypatch, body)
    res.assert_outcomes(passed=1)


def test_known_bug_xfail_naming_a_format_is_not_a_jar_excuse(pytester, monkeypatch):
    body = (
        "import pytest\n\n"
        "@pytest.mark.xfail(strict=True, reason='LB-034: Delta Q2 ClassCastException upstream')\n"
        "def test_x():\n    assert False\n"
    )
    res = _session(pytester, monkeypatch, body, LB_REQUIRE_JARS="1")
    res.assert_outcomes(xfailed=1)
    assert res.ret == 0


def test_static_conf_mismatch_sees_a_catalog_left_in_the_jvm(harness):
    want = {"spark.master": "local[2]", "spark.sql.extensions": harness.ICEBERG_EXTENSION}
    have = dict(want, **{"spark.sql.catalog.lh": "org.apache.iceberg.spark.SparkCatalog"})
    assert harness.static_conf_mismatch(have, want) == {
        "spark.sql.catalog.lh": ("org.apache.iceberg.spark.SparkCatalog", None)
    }
    have = dict(want, **{"spark.sql.catalog.spark_catalog": harness.DELTA_CATALOG})
    assert list(harness.static_conf_mismatch(have, want)) == ["spark.sql.catalog.spark_catalog"]
    assert harness.static_conf_mismatch(want, dict(want)) == {}
    extra = {"spark.default.parallelism"}
    assert harness.static_conf_mismatch(
        want, dict(want, **{"spark.default.parallelism": "1"}), extra
    )


# --- known_bug ---------------------------------------------------------------


def _inner_leg(harness) -> str:
    """The Spark line the inner session sees: the fake pyspark is 4.0."""
    return (harness.pyspark_leg() or "4.0") if importlib.util.find_spec("pyspark") else "4.0"


def _other_leg(leg: str) -> str:
    return "4.1" if leg == "4.0" else "4.0"


def test_known_bug_on_this_leg_is_a_strict_xfail(harness, pytester, monkeypatch):
    leg = _inner_leg(harness)
    body = (
        "import pytest\n\n"
        f"@pytest.mark.known_bug('LB-193', match='No plan', legs=('{leg}',), reason='temp view')\n"
        "def test_still_broken():\n    raise RuntimeError('No plan for TableReference')\n\n"
        f"@pytest.mark.known_bug('LB-193', match='No plan', legs=('{leg}',))\n"
        "def test_fixed_upstream():\n    pass\n"
    )
    res = _session(pytester, monkeypatch, body, "-rxX", LB_REQUIRE_JARS="1")
    res.assert_outcomes(xfailed=1, failed=1)
    out = res.stdout.str()
    assert f"LB-193 on Spark {leg}: temp view" in out
    assert "XPASS(strict)" in out


def test_known_bug_on_another_leg_runs_the_test(harness, pytester, monkeypatch):
    other = _other_leg(_inner_leg(harness))
    body = (
        "import pytest\n\n"
        f"@pytest.mark.known_bug('LB-193', match='.', legs=('{other}',))\n"
        "def test_broken_here_too():\n    assert False\n"
    )
    res = _session(pytester, monkeypatch, body)
    res.assert_outcomes(failed=1)


def test_known_bug_on_both_legs_by_default(pytester, monkeypatch):
    body = (
        "import pytest\n\n"
        "@pytest.mark.known_bug('QR-6', match='TABLE_OR_VIEW', reason='stale names')\n"
        "def test_x():\n    raise ValueError('[TABLE_OR_VIEW_NOT_FOUND] lh.x')\n"
    )
    res = _session(pytester, monkeypatch, body)
    res.assert_outcomes(xfailed=1)


@pytest.mark.parametrize(
    "args",
    [
        "'see BUGS', match='x'",
        "'LB-193', match='x', legs=('3.5',)",
        "'LB-193', match='x', leg='4.1'",
        "'LB-193'",
        "'LB-193', match='('",
        "",
    ],
)
def test_known_bug_with_bad_arguments_refuses_to_run(pytester, monkeypatch, args):
    body = f"import pytest\n\n@pytest.mark.known_bug({args})\ndef test_x():\n    pass\n"
    res = _session(pytester, monkeypatch, body)
    assert res.ret == pytest.ExitCode.USAGE_ERROR
    assert "known_bug" in res.stderr.str()


def test_known_bug_does_not_cover_another_failure(pytester, monkeypatch):
    """A test marked for one bug that fails some other way is a failure."""
    body = (
        "import pytest\n\n"
        "@pytest.mark.known_bug('LB-195', match='queryId is not set')\n"
        "def test_other_cause():\n    raise KeyError('W5_sanctions_match')\n\n"
        "@pytest.fixture\n"
        "def broken():\n    raise RuntimeError('ClassNotFoundException: org.apache.iceberg')\n\n"
        "@pytest.mark.known_bug('LB-195', match='queryId is not set')\n"
        "def test_setup_error(broken):\n    pass\n"
    )
    res = _session(pytester, monkeypatch, body)
    res.assert_outcomes(failed=1, errors=1)
    assert res.stdout.str().count("known_bug LB-195 does not cover this failure") == 2


def test_known_bug_does_not_cover_a_missing_jar(pytester, monkeypatch):
    body = (
        "import pytest\n\n"
        "@pytest.mark.known_bug('LB-195', match='.')\n"
        "@pytest.mark.requires_jars('iceberg')\n"
        "def test_x():\n    pass\n"
    )
    res = _session(pytester, monkeypatch, body, LB_REQUIRE_JARS="1")
    assert res.ret == 1
    res.assert_outcomes(errors=1)
    assert "LB-JARS missing: iceberg" in res.stdout.str()


def test_known_bug_does_not_cover_a_jar_failure_inside_the_test(pytester, monkeypatch):
    body = (
        "import pytest\n\n"
        "@pytest.mark.known_bug('LB-195', match='.')\n"
        "def test_x():\n"
        "    pytest.fail('LB-JARS missing: delta (set LB_SPARK_TEST_JARS)')\n"
    )
    res = _session(pytester, monkeypatch, body)
    res.assert_outcomes(failed=1)
    assert "it needs a jar that is missing" in res.stdout.str()


# -- --lb-shard: CI splits each Spark leg and order into shards by file ------

SPARK_DIR = ROOT / "tests" / "spark"


def _spark_files() -> set[str]:
    return {p.relative_to(ROOT).as_posix() for p in SPARK_DIR.rglob("test_*.py")}


@pytest.mark.parametrize("n", [1, 2, 3, 4])
def test_shards_put_every_spark_file_in_exactly_one_shard(harness, n):
    files = _spark_files()
    weights = harness.read_shard_weights()
    assignment = harness.shard_files(files, n, weights)
    assert set(assignment) == files
    assert set(assignment.values()) == set(range(1, n + 1))
    # A function of the file set and the weights only, not their order.
    assert harness.shard_files(sorted(files, reverse=True), n, weights) == assignment


def test_shards_balance_on_the_recorded_seconds(harness):
    weights = {"a.py": 100.0, "b.py": 60.0, "c.py": 50.0, "d.py": 10.0}
    got = harness.shard_files(weights, 2, weights)
    assert got == {"a.py": 1, "b.py": 2, "c.py": 2, "d.py": 1}


def test_a_file_without_a_recorded_time_weighs_the_median(harness):
    weights = {"a.py": 10.0, "b.py": 30.0, "c.py": 50.0}
    # new.py weighs 30: after c(50) -> 1 and b(30) -> 2, new(30, "n" > "b") -> 2,
    # then a(10) -> 1.
    got = harness.shard_files([*weights, "new.py"], 2, weights)
    assert got == {"c.py": 1, "b.py": 2, "new.py": 2, "a.py": 1}


@pytest.mark.parametrize("value", ["0/2", "3/2", "1", "a/b", "1/0", "-1/2"])
def test_bad_shard_values_are_usage_errors(harness, value):
    with pytest.raises(pytest.UsageError, match="--lb-shard"):
        harness.parse_shard(value)


def test_the_recorded_weights_name_spark_test_files(harness):
    """Keyed the way the hook keys a collected file, or balancing is off."""
    weights = harness.read_shard_weights()
    files = _spark_files()
    assert weights, "tests/spark/shard_weights.json has no seconds"
    assert len(set(weights) & files) >= len(files) // 2, sorted(set(weights) - files)[:5]


_SHARD_BODY = "def test_a():\n    pass\n\ndef test_b():\n    pass\n"


def _collected(pytester, monkeypatch, *args: str) -> tuple[list[str], str]:
    res = _session(pytester, monkeypatch, "", "--collect-only", "-q", *args)
    assert res.ret == 0, res.stdout.str()
    return [ln for ln in res.stdout.lines if "::" in ln], res.stdout.str()


def test_lb_shard_partitions_the_files_and_keeps_the_reverse_order(pytester, monkeypatch):
    pytester.makepyfile(**{f"test_s{i}": _SHARD_BODY for i in range(5)})
    everything, _ = _collected(pytester, monkeypatch)
    assert len(everything) == 10
    shards = []
    for k in (1, 2):
        forward, out = _collected(pytester, monkeypatch, "--lb-shard", f"{k}/2")
        backward, _ = _collected(pytester, monkeypatch, "--lb-shard", f"{k}/2", "--lb-reverse")
        assert forward and backward == forward[::-1]
        assert forward == [t for t in everything if t in forward]
        assert f"{10 - len(forward)} deselected" in out
        shards.append(forward)
    assert sorted(shards[0] + shards[1]) == sorted(everything)
    files = [{t.split("::")[0] for t in s} for s in shards]
    assert not files[0] & files[1]


def test_lb_shard_reads_the_weights_beside_the_harness(pytester, monkeypatch):
    pytester.makepyfile(**{f"test_s{i}": _SHARD_BODY for i in range(4)})
    (pytester.path / "shard_weights.json").write_text(
        '{"seconds": {"test_s2.py": 1000, "test_s0.py": 1, "test_s1.py": 1, "test_s3.py": 1}}'
    )
    first, _ = _collected(pytester, monkeypatch, "--lb-shard", "1/2")
    assert {t.split("::")[0] for t in first} == {"test_s2.py"}


def test_lb_shard_rejects_a_bad_value(pytester, monkeypatch):
    res = _session(pytester, monkeypatch, _SHARD_BODY, "--lb-shard", "3/2")
    assert res.ret == pytest.ExitCode.USAGE_ERROR
    assert "--lb-shard takes K/N" in res.stderr.str()
