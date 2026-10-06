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
import re

import pytest
from pydantic import ValidationError

from lakebench.deploy.deps_tools import lb_deps
from lakebench.deps import manifest as m
from lakebench.deps import request as req
from tests.conftest import make_config
from tests.test_lb_deps import ext_repo  # noqa: F401 -- the fixture

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


def _h(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def fake_shown(request: req.DepsRequest, **over) -> dict:
    """What ``lb_deps.py show`` prints for a set that serves ``request``."""
    jars = [m.ivy_jar_name(c) for c in request.jar_coordinates]
    jars.append("software.amazon.awssdk_bundle-2.24.6.jar")
    groups: dict[str, list[dict]] = {
        "jars": [{"file": f, "sha256": _h(f), "size": 10} for f in jars]
    }
    if req.GROUP_PY_REFERENCE in request.groups:
        groups["py-reference"] = []
        for pin in request.py_reference:
            n, v = pin.split("==")
            f = f"{n.replace('-', '_')}-{v}-cp310-cp310-manylinux_2_17_x86_64.whl"
            groups["py-reference"].append({"file": f, "sha256": _h(f), "size": 5})
    if req.GROUP_DUCKDB in request.groups:
        v = request.duckdb_version
        w = f"duckdb-{v}-cp311-cp311-manylinux_2_27_x86_64.whl"
        groups["duckdb-wheels"] = [{"file": w, "sha256": _h(w), "size": 7}]
        groups["duckdb-ext"] = [
            {"file": f"v{v}/linux_amd64/{n}.duckdb_extension", "sha256": _h(n), "size": 3}
            for n in sorted(request.duckdb_extensions)
        ]
    order = list(jars)
    shown = {
        "request_sha256": request.request_sha256,
        "tools_sha256": request.tools_sha256,
        "groups": groups,
        "jar_order": order,
        "overlaps": [],
        "repositories": list(request.repositories),
        "python": {"spark": "3.10.12"},
        "resolved_at": "2026-10-01T00:00:00Z",
    }
    shown["pinset_sha256"] = req.pinset_sha256(groups, order)
    shown.update(over)
    return shown


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


@pytest.mark.parametrize(
    "group,name",
    [
        ("jars", "../evil.jar"),
        ("jars", "sub/evil.jar"),
        ("jars", "evil jar.jar"),
        ("jars", "evil\n.jar"),
        ("duckdb-ext", "v1.5.5/../x.duckdb_extension"),
        ("duckdb-ext", "/v1.5.5/linux_amd64/x.duckdb_extension"),
    ],
)
def test_unsafe_file_names_are_refused(group, name):
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


def test_configmap_data_files():
    r = _request(**AML_DUCK)
    shown = fake_shown(r)
    data = m.manifest_configmap_data(r, shown, "lb-deps-abc", "uid-1")
    assert json.loads(data["manifest.json"]) == shown
    lines = data["jars.sha256"].splitlines()
    assert [ln.split("  ", 1)[1] for ln in lines] == shown["jar_order"]
    reqs = data["requirements-py-reference.txt"].splitlines()
    assert [ln.split(" ")[0] for ln in reqs] == list(r.py_reference)
    by_file = {e["file"]: e["sha256"] for e in shown["groups"]["py-reference"]}
    for ln in reqs:
        assert re.fullmatch(r"\S+==\S+ --hash=sha256:[0-9a-f]{64}", ln)
        assert ln.rsplit(":", 1)[1] in by_file.values()
    assert data["requirements-duckdb.txt"].startswith(f"duckdb=={r.duckdb_version} --hash=sha256:")
    assert len(data["duckdb-ext.sha256"].splitlines()) == len(r.duckdb_extensions)
    assert (data["server-pod"], data["server-pod-uid"]) == ("lb-deps-abc", "uid-1")
    c360 = _request(**C360)
    plain = m.manifest_configmap_data(c360, fake_shown(c360), "p", "u")
    assert set(plain) == {"manifest.json", "jars.sha256", "server-pod", "server-pod-uid"}


def test_configmap_budget():
    r = _request(**C360)
    shown = fake_shown(r, overlaps=[{"pad": "x" * (m.MANIFEST_CONFIGMAP_BUDGET + 1)}])
    with pytest.raises(ValueError, match="budget"):
        m.manifest_configmap_data(r, shown, "p", "u")


def test_ivy_jar_name_matches_the_resolver():
    for c in (
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0",
        "io.delta:delta-spark_2.13:4.0.0",
    ):
        assert m.ivy_jar_name(c) == lb_deps.coordinate_jar(c)


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


def _cpu_m(q: str) -> int:
    return int(q[:-1]) if q.endswith("m") else int(float(q) * 1000)


def _mi(q: str) -> int:
    return int(q[:-2]) * (1024 if q.endswith("Gi") else 1)


@pytest.mark.parametrize("over", [C360, AML_DUCK])
def test_pod_reservation_is_the_rendered_effective_request(over):
    """max(largest init request, sum of container requests), as the
    scheduler reserves it for the pod's whole life."""
    spec = _deployment(over)["spec"]["template"]["spec"]
    inits = [c["resources"]["requests"] for c in spec["initContainers"]]
    apps = [c["resources"]["requests"] for c in spec["containers"]]
    cpu = max(max(_cpu_m(r["cpu"]) for r in inits), sum(_cpu_m(r["cpu"]) for r in apps))
    mem = max(max(_mi(r["memory"]) for r in inits), sum(_mi(r["memory"]) for r in apps))
    assert (m.POD_REQUEST_CPU_M, m.POD_REQUEST_MEMORY_MI) == (cpu, mem) == (1000, 2048)


def test_co_resident_sum_counts_lb_deps():
    from lakebench.config import sizing
    from lakebench.config.autosizer import _co_resident_cpu_m, _co_resident_label

    cfg = make_config(**C360)
    trino = cfg.architecture.query_engine.trino
    coord = _cpu_m(str(trino.coordinator.cpu))
    workers = trino.worker.replicas * _cpu_m(str(trino.worker.cpu))
    assert _co_resident_cpu_m(cfg) == coord + workers + 1000 + m.POD_REQUEST_CPU_M
    co = sizing.co_resident_request(cfg, False)
    assert "lb-deps" in co.label and "lb-deps" in _co_resident_label(cfg)
    # Memory: the engine pods, the catalog and Postgres, and lb-deps once.
    others = sum(mem for _, _, mem in sizing._engine_pods(cfg)) + sizing._catalog_memory_gi(cfg)
    assert co.memory_gb == sizing._ceil(others + m.POD_REQUEST_MEMORY_MI / 1024)
    assert co.cpu_cores == -(-(coord + workers + 1000 + m.POD_REQUEST_CPU_M) // 1000)


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


def test_deps_keys_default_empty():
    d = make_config().platform.deps
    assert (d.maven_repository, d.pypi_index, d.duckdb_extension_repository, d.storage_class) == (
        "",
        "",
        "",
        "",
    )


def test_mirror_urls_are_normalised():
    d = _deps(
        maven_repository=" http://nexus:8081/repository/maven ",
        pypi_index="https://nexus/repository/pypi/simple//",
        duckdb_extension_repository="http://ext.lab/",
    )
    assert d.maven_repository == "http://nexus:8081/repository/maven/"
    assert d.pypi_index == "https://nexus/repository/pypi/simple/"
    assert d.duckdb_extension_repository == "http://ext.lab"


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


def test_storage_class_name():
    assert _deps(storage_class=" px-csi-db ").storage_class == "px-csi-db"
    with pytest.raises(ValidationError, match="StorageClass name"):
        _deps(storage_class="PX_CSI")


def test_unknown_deps_key_is_refused():
    with pytest.raises(ValidationError, match="mirror"):
        _deps(mirror="http://x/")


# --- contract with the real lb_deps.py ------------------------------------------------


def test_the_configmap_is_what_fetch_and_pip_consume(tmp_path, monkeypatch, capsys):
    """resolve -> show -> check_manifest -> ConfigMap data -> serve -> fetch:
    the deployer's manifest.json is accepted by ``lb_deps.py fetch`` for
    every group, and each requirement line's hash is the served wheel's."""
    from tests import test_lb_deps as t

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


@pytest.mark.parametrize(
    "value",
    ["http://host:99999/m2/", "http://host:abc/m2/", "http://ho\x00st/m2/", "http://h\u00f6st/m2/"],
)
def test_bad_ports_and_characters_are_refused(value):
    with pytest.raises(ValidationError, match="platform.deps.maven_repository"):
        _deps(maven_repository=value)


def test_scheme_and_host_case_is_normalised():
    assert _deps(maven_repository="HTTP://Nexus.Lab:8081/M2").maven_repository == (
        "http://nexus.lab:8081/M2/"
    )


@pytest.mark.parametrize("name", ["px..db", "-px", "px-", "a" * 64])
def test_bad_storage_class_names_are_refused(name):
    with pytest.raises(ValidationError, match="StorageClass name"):
        _deps(storage_class=name)


def test_the_duckdb_groups_are_what_fetch_and_pip_consume(tmp_path, monkeypatch, capsys, request):
    """The DuckDB half of the contract: the real resolve's duckdb-ext names
    pass check_manifest, fetch recreates the extension paths, and the
    requirements line hashes the served wheel."""
    from tests import test_lb_deps as t

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
