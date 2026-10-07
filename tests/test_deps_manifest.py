"""deps.manifest: the CLI's check of a served set and the ConfigMap it writes
(DEP-2, ch01 s2.5 steps 4-5), plus the ``platform.deps`` keys (s2.7).

The contract tests at the end run the real ``lb_deps.py`` resolve, show,
serve and fetch (with the fake tools of ``tests/test_lb_deps.py``), so the
ConfigMap the deployer writes is proven readable by the consumer that reads
it.
"""

from __future__ import annotations

import hashlib
import json

import pytest
from pydantic import ValidationError

from lakebench.deploy.deps_tools import lb_deps
from lakebench.deps import manifest as m
from lakebench.deps import request as req
from tests.conftest import make_config
from tests.fixtures.deps_manifest_helpers import _h as _h
from tests.fixtures.deps_manifest_helpers import fake_shown as fake_shown
from tests.fixtures.lb_deps_helpers import ext_repo  # noqa: F401 -- the fixture

H = "0" * 64


@pytest.fixture(autouse=True)
def _restore_signal_handlers():
    """The contract tests run lb_deps.py resolve in this process, which
    installs a SIGTERM handler; restore it for the tests after them."""
    import signal

    saved = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    yield
    for s, handler in saved.items():
        signal.signal(s, handler)


def _request(**cfg_over) -> req.DepsRequest:
    return req.select_request(make_config(**cfg_over), tools_digest="t" * 64)


C360 = {"recipe": "hive-iceberg-spark-trino"}
AML_DUCK = {"recipe": "polaris-iceberg-spark-duckdb", "workload": {"schema": "financial"}}
DELTA = {"recipe": "hive-delta-spark-thrift"}


@pytest.mark.parametrize("over", [C360, AML_DUCK, DELTA])
def test_a_matching_set_passes(over):
    r = _request(**over)
    assert m.check_manifest(r, fake_shown(r)) == []


def _rehash(shown: dict) -> dict:
    shown["pinset_sha256"] = req.pinset_sha256(shown["groups"], shown["jar_order"])
    return shown


def test_printed_pinset_is_never_trusted():
    r = _request(**C360)
    shown = fake_shown(r)
    shown["groups"]["jars"][0]["sha256"] = "1" * 64  # entries changed, field kept
    assert any("printed pinset" in p for p in m.check_manifest(r, shown))


def test_reordered_jars_are_another_set():
    r = _request(**C360)
    shown = fake_shown(r)
    shown["jar_order"] = list(reversed(shown["jar_order"]))
    assert any("printed pinset" in p for p in m.check_manifest(r, shown))


def test_jar_order_must_name_the_jars_group():
    r = _request(**C360)
    shown = fake_shown(r)
    shown["jar_order"] = shown["jar_order"][1:]
    assert any("do not hash" in p for p in m.check_manifest(r, shown))


@pytest.mark.parametrize(
    "field,value,needle",
    [
        ("request_sha256", "f" * 64, "request_sha256"),
        ("tools_sha256", "f" * 64, "resolver"),
    ],
)
def test_another_request_or_resolver_is_refused(field, value, needle):
    r = _request(**C360)
    assert any(needle in p for p in m.check_manifest(r, fake_shown(r, **{field: value})))


def test_a_missing_or_extra_group_is_refused():
    r = _request(**AML_DUCK)
    shown = fake_shown(r)
    del shown["groups"]["py-reference"]
    assert any("groups" in p for p in m.check_manifest(r, _rehash(shown)))
    c360 = _request(**C360)
    extra = fake_shown(c360)
    extra["groups"]["py-reference"] = [{"file": "x-1-py3-none-any-x.whl", "sha256": H, "size": 1}]
    assert any("groups" in p for p in m.check_manifest(c360, _rehash(extra)))


def test_an_empty_group_is_refused():
    r = _request(**AML_DUCK)
    shown = fake_shown(r)
    shown["groups"]["duckdb-ext"] = []
    assert any("empty" in p for p in m.check_manifest(r, _rehash(shown)))


def test_unsafe_file_names_are_refused():
    for group, name in [
        ("jars", "../evil.jar"),
        ("jars", "sub/evil.jar"),
        ("jars", "evil jar.jar"),
        ("jars", "evil\n.jar"),
        ("duckdb-ext", "v1.5.5/../x.duckdb_extension"),
        ("duckdb-ext", "/v1.5.5/linux_amd64/x.duckdb_extension"),
    ]:
        r = _request(**AML_DUCK)
        shown = fake_shown(r)
        shown["groups"][group][0]["file"] = name
        if group == "jars":
            shown["jar_order"][0] = name
        assert any("unsafe" in p for p in m.check_manifest(r, _rehash(shown)))


def test_a_missing_direct_coordinate_is_refused():
    r = _request(**C360)
    shown = fake_shown(r)
    gone = m.ivy_jar_name(r.jar_coordinates[-1])
    shown["groups"]["jars"] = [e for e in shown["groups"]["jars"] if e["file"] != gone]
    shown["jar_order"] = [f for f in shown["jar_order"] if f != gone]
    assert any("is not in the set" in p for p in m.check_manifest(r, _rehash(shown)))


def test_ux_d2_one_runtime_of_the_requested_suffix():
    """Spark 4.1 with Iceberg 1.11 must hold the 4.1 runtime, not 4.0."""
    r = _request(
        images={"spark": "apache/spark:4.1.1-python3"},
        architecture={
            "catalog": {"type": "hive"},
            "table_format": {"type": "iceberg", "iceberg": {"version": "1.11.0"}},
        },
    )
    runtime = m.ivy_jar_name(next(c for c in r.jar_coordinates if "spark-runtime" in c))
    assert "runtime-4.1_" in runtime
    shown = fake_shown(r)
    other = runtime.replace("runtime-4.1_", "runtime-4.0_")
    shown["groups"]["jars"].append({"file": other, "sha256": H, "size": 1})
    shown["jar_order"].append(other)
    assert any("runtime jars" in p for p in m.check_manifest(r, _rehash(shown)))
    delta = _request(**DELTA)
    mixed = fake_shown(delta)
    mixed["groups"]["jars"].append({"file": runtime, "sha256": H, "size": 1})
    mixed["jar_order"].append(runtime)
    assert any("runtime jars" in p for p in m.check_manifest(delta, _rehash(mixed)))


def test_unknown_overlaps_are_refused_and_known_ones_pass():
    r = _request(**C360)
    known = next(iter(req.KNOWN_OVERLAPS))
    ok = {"artifact": known[0], "jar": "x.jar", "version": known[1]}
    ok |= {"image_jar": "y.jar", "image_version": known[2]}
    assert m.check_manifest(r, fake_shown(r, overlaps=[ok])) == []
    bad = dict(ok, version="9.9.9")
    problems = m.check_manifest(r, fake_shown(r, overlaps=[bad]))
    assert any("KNOWN_OVERLAPS" in p for p in problems)
    assert any(
        "overlaps are malformed" in p for p in m.check_manifest(r, fake_shown(r, overlaps=[{}]))
    )


def test_wheels_match_pins_one_to_one():
    r = _request(**AML_DUCK)
    shown = fake_shown(r)
    shown["groups"]["py-reference"][0]["file"] = "numpy-9.9.9-cp310-cp310-x.whl"
    assert any("pin numpy==" in p for p in m.check_manifest(r, _rehash(shown)))
    extra = fake_shown(r)
    extra["groups"]["py-reference"].append(
        {"file": "evil-1.0-py3-none-any.whl", "sha256": H, "size": 1}
    )
    assert any("files for" in p for p in m.check_manifest(r, _rehash(extra)))


def test_duckdb_files_are_the_requested_version_and_extensions():
    r = _request(**AML_DUCK)
    wrong_version = fake_shown(r)
    e = wrong_version["groups"]["duckdb-ext"][0]
    e["file"] = e["file"].replace(f"v{r.duckdb_version}/", "v0.0.1/")
    assert any("duckdb-ext" in p for p in m.check_manifest(r, _rehash(wrong_version)))
    missing = fake_shown(r)
    missing["groups"]["duckdb-ext"].pop()
    assert any("duckdb-ext" in p for p in m.check_manifest(r, _rehash(missing)))
    two_platforms = fake_shown(r)
    e = two_platforms["groups"]["duckdb-ext"][0]
    e["file"] = e["file"].replace("linux_amd64", "linux_arm64")
    assert any("platforms" in p for p in m.check_manifest(r, _rehash(two_platforms)))


def test_base_url_is_built_from_the_namespace_only():
    assert m.base_url("ns-a", "p" * 64) == (
        "http://lb-deps.ns-a.svc.cluster.local:8080/sets/" + "p" * 64
    )


# --- the pod's reservation and the co-resident sum ----------------------------------


def _deployment(over: dict) -> dict:
    from unittest.mock import MagicMock

    from lakebench.deploy.deps import DependencyServerDeployer
    from lakebench.deploy.engine import TemplateRenderer

    cfg = make_config(**over)
    engine = MagicMock(config=cfg, renderer=TemplateRenderer(), dry_run=False)
    d = DependencyServerDeployer(engine)
    docs = d.render(req.select_request(cfg, tools_digest="t" * 64))
    return next(doc for doc in docs if doc["kind"] == "Deployment")


def test_lb_deps_reservation_can_lower_datagen_on_a_binding_cluster():
    """The documented auto-sizing shift (CHANGELOG): scale 100 batch on Trino
    with 180 allocatable cores gets 14 datagen pods, where it got 16 before
    lb-deps was counted."""
    from types import SimpleNamespace

    from lakebench.config.autosizer import resolve_auto_sizing

    cap = SimpleNamespace(
        total_cpu_millicores=180_000,
        total_memory_bytes=1800 * 1024**3,
        largest_node_cpu_millicores=40_000,
        largest_node_memory_bytes=402 * 1024**3,
        node_count=4,
    )
    cfg = make_config(**C360)
    cfg.architecture.workload.datagen.scale = 100
    resolve_auto_sizing(cfg, cap)
    assert cfg.architecture.workload.datagen.parallelism == 14


# --- platform.deps ------------------------------------------------------------------


def _deps(**keys):
    s3 = {"endpoint": "http://minio:9000", "access_key": "k", "secret_key": "s"}
    return make_config(platform={"storage": {"s3": s3}, "deps": keys}).platform.deps


@pytest.mark.parametrize(
    "value,needle",
    [
        ("ftp://nexus/m2/", "http://"),
        ("nexus/m2/", "http://"),
        ("http://user:pw@nexus/m2/", "credentials"),
        ("http://token@nexus/m2/", "credentials"),
        ("http://nexus/m2/?x=1", "query"),
        ("http://nexus/m2/#frag", "query"),
        ("http://nexus/m 2/", "query"),
    ],
)
def test_bad_mirror_urls_are_refused(value, needle):
    with pytest.raises(ValidationError, match=needle):
        _deps(maven_repository=value)


# --- contract with the real lb_deps.py ------------------------------------------------


def test_the_configmap_is_what_fetch_and_pip_consume(tmp_path, monkeypatch, capsys):
    """resolve -> show -> check_manifest -> ConfigMap data -> serve -> fetch:
    the deployer's manifest.json is accepted by ``lb_deps.py fetch`` for
    every group, and each requirement line's hash is the served wheel's."""
    from tests.fixtures import lb_deps_helpers as t

    env = t.Env(tmp_path / "pod", monkeypatch)
    request = req.DepsRequest(
        groups=("jars", "py-reference"),
        jar_coordinates=tuple(t.COORDS),
        repositories=(req.MAVEN_CENTRAL,),
        spark_image="apache/spark:4.0.2-python3",
        tools_sha256=hashlib.sha256(t.TOOL.read_bytes()).hexdigest(),
        py_reference=tuple(t.PINS),
        pypi_index="http://pypi.mirror.example/simple/",
    )
    (env.tools / "request.json").write_text(request.canonical_json())
    monkeypatch.setenv("LB_DEPS_REQUEST_SHA256", request.request_sha256)
    assert env.run("resolve", "spark") == 0
    capsys.readouterr()
    assert lb_deps.main(["show", "--request", request.request_sha256]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert m.check_manifest(request, shown) == []
    data = m.manifest_configmap_data(request, shown, "lb-deps-x", "uid-x")
    mount = tmp_path / "manifest-mount"
    mount.mkdir()
    for k, v in data.items():
        (mount / k).write_text(v)

    proc = t._serve(tmp_path / "port")
    try:
        port = t._wait_port(proc, tmp_path / "port")
        url = f"http://127.0.0.1:{port}/sets/{shown['pinset_sha256']}"
        for group in ("jars", "py-reference"):
            dest = tmp_path / "fetched" / group
            rc = lb_deps.main(
                ["fetch", "--group", group, "--dest", str(dest), "--manifest",
                 str(mount / "manifest.json"), "--url", url]
            )  # fmt: skip
            assert rc == 0
        jars = tmp_path / "fetched" / "jars"
        for line in (mount / "jars.sha256").read_text().splitlines():
            sha, name = line.split("  ", 1)
            assert hashlib.sha256((jars / name).read_bytes()).hexdigest() == sha
        wheels = {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (tmp_path / "fetched" / "py-reference").iterdir()
        }
        lines = (mount / "requirements-py-reference.txt").read_text().splitlines()
        assert len(lines) == len(t.PINS)
        for line in lines:
            pin, hash_part = line.split(" --hash=sha256:")
            name, ver = pin.split("==")
            (wheel,) = [w for w in wheels if w.startswith(f"{name.replace('-', '_')}-{ver}-")]
            assert wheels[wheel] == hash_part
    finally:
        proc.kill()
        proc.wait()


def test_rendered_objects_select_only_the_server():
    """Deployment and Service select on the server role too, so a later
    object labelled component=deps is never adopted or sent traffic."""
    from unittest.mock import MagicMock

    from lakebench.deploy.deps import DependencyServerDeployer
    from lakebench.deploy.engine import TemplateRenderer

    cfg = make_config(**AML_DUCK, platform={"storage": {"s3": {"endpoint": "http://m:9000",
        "access_key": "k", "secret_key": "s"}}, "deps": {"storage_class": "true"}})  # fmt: skip
    eng = MagicMock(config=cfg, renderer=TemplateRenderer(), dry_run=False)
    docs = {d["kind"]: d for d in DependencyServerDeployer(eng).render(req.select_request(cfg))}
    ns = cfg.get_namespace()
    assert {d["metadata"]["namespace"] for d in docs.values()} == {ns}
    # The templates spell the names out (the Category-1 registry scan reads
    # them); they must be the constants the deployer and destroy use.
    assert docs["Deployment"]["metadata"]["name"] == m.SERVER_NAME
    assert docs["Service"]["metadata"]["name"] == m.SERVER_NAME
    assert docs["PersistentVolumeClaim"]["metadata"]["name"] == m.PVC_NAME
    claim = docs["Deployment"]["spec"]["template"]["spec"]["volumes"][1]
    assert claim["persistentVolumeClaim"]["claimName"] == m.PVC_NAME
    assert docs["Deployment"]["spec"]["selector"]["matchLabels"] == m.SELECTOR_LABELS
    assert docs["Service"]["spec"]["selector"] == m.SELECTOR_LABELS
    tmpl = docs["Deployment"]["spec"]["template"]["metadata"]["labels"]
    assert m.SELECTOR_LABELS.items() <= tmpl.items()
    # A YAML-ambiguous class name stays a string.
    assert docs["PersistentVolumeClaim"]["spec"]["storageClassName"] == "true"


def test_overlaps_must_be_a_list():
    r = _request(**C360)
    for bad in (None, {}, "x"):
        assert any("overlaps" in p for p in m.check_manifest(r, fake_shown(r, overlaps=bad)))
    missing = fake_shown(r)
    del missing["overlaps"]
    assert any("overlaps" in p for p in m.check_manifest(r, missing))


def test_the_duckdb_groups_are_what_fetch_and_pip_consume(tmp_path, monkeypatch, capsys, request):
    """The DuckDB half of the contract: the real resolve's duckdb-ext names
    pass check_manifest, fetch recreates the extension paths, and the
    requirements line hashes the served wheel."""
    from tests.fixtures import lb_deps_helpers as t

    repo = request.getfixturevalue("ext_repo")
    env = t.Env(tmp_path / "pod", monkeypatch)
    deps_request = req.DepsRequest(
        groups=("jars", "duckdb"),
        jar_coordinates=tuple(t.COORDS),
        repositories=(req.MAVEN_CENTRAL,),
        spark_image="apache/spark:4.0.2-python3",
        tools_sha256=hashlib.sha256(t.TOOL.read_bytes()).hexdigest(),
        pypi_index="http://pypi.mirror.example/simple/",
        duckdb_version="1.5.5",
        duckdb_extensions=req.DUCKDB_EXTENSIONS,
        duckdb_image="python:3.11-slim",
        duckdb_extension_repository=repo,
    )
    (env.tools / "request.json").write_text(deps_request.canonical_json())
    monkeypatch.setenv("LB_DEPS_REQUEST_SHA256", deps_request.request_sha256)
    assert env.run("resolve", "duckdb") == 0
    assert env.run("resolve", "spark") == 0
    capsys.readouterr()
    assert lb_deps.main(["show", "--request", deps_request.request_sha256]) == 0
    shown = json.loads(capsys.readouterr().out)
    assert m.check_manifest(deps_request, shown) == []
    data = m.manifest_configmap_data(deps_request, shown, "p", "u")
    mount = tmp_path / "mount"
    mount.mkdir()
    for k, v in data.items():
        (mount / k).write_text(v)
    proc = t._serve(tmp_path / "port")
    try:
        port = t._wait_port(proc, tmp_path / "port")
        url = f"http://127.0.0.1:{port}/sets/{shown['pinset_sha256']}"
        for group in ("duckdb-wheels", "duckdb-ext"):
            rc = lb_deps.main(["fetch", "--group", group, "--dest", str(tmp_path / group),
                               "--manifest", str(mount / "manifest.json"), "--url", url])  # fmt: skip
            assert rc == 0
        for line in (mount / "duckdb-ext.sha256").read_text().splitlines():
            sha, rel = line.split("  ", 1)
            got = hashlib.sha256((tmp_path / "duckdb-ext" / rel).read_bytes()).hexdigest()
            assert got == sha
        (line,) = (mount / "requirements-duckdb.txt").read_text().splitlines()
        pin, h = line.split(" --hash=sha256:")
        assert pin == "duckdb==1.5.5"
        (wheel,) = (tmp_path / "duckdb-wheels").iterdir()
        assert hashlib.sha256(wheel.read_bytes()).hexdigest() == h
    finally:
        proc.kill()
        proc.wait()
