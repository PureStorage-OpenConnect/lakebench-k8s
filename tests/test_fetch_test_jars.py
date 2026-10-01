"""scripts/fetch_test_jars.py (QA-2): pinned jars, the mirror fallback, the
cache re-hash and the lock rebuild, with a fake HTTP layer (no network)."""

from __future__ import annotations

import hashlib
import http.client
import json
import urllib.error
from pathlib import Path

import pytest

from tests.conftest import exec_repo_script

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def fj():
    return exec_repo_script(ROOT / "scripts" / "fetch_test_jars.py", "fetch_test_jars")


COORD = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"
JAR = b"iceberg runtime bytes"


def _entry(data: bytes = JAR, coord: str = COORD) -> dict:
    return {"coord": coord, "sha256": hashlib.sha256(data).hexdigest(), "kind": "iceberg"}


class FakeMaven:
    """Serves paths from a dict; *fail* maps a host prefix to an exception."""

    def __init__(self, files: dict[str, bytes], fail: dict[str, Exception] | None = None):
        self.files = files
        self.fail = fail or {}
        self.calls: list[str] = []

    def __call__(self, url: str) -> bytes:
        self.calls.append(url)
        for prefix, err in self.fail.items():
            if url.startswith(prefix):
                raise err
        path = url.split("/maven2/", 1)[1]
        if path not in self.files:
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)  # type: ignore[arg-type]
        return self.files[path]


def _maven(fj, data: bytes = JAR, **fail: Exception) -> FakeMaven:
    return FakeMaven({fj.artifact_path(COORD): data}, fail)


def test_paths_and_names(fj):
    assert fj.jar_name(COORD) == "iceberg-spark-runtime-4.0_2.13-1.11.0.jar"
    assert fj.artifact_path("io.delta:delta-storage:4.1.0", "pom") == (
        "io/delta/delta-storage/4.1.0/delta-storage-4.1.0.pom"
    )


def test_download_is_checked_and_kept_under_its_maven_name(fj, tmp_path):
    get = _maven(fj)
    path = fj.ensure_jar(_entry(), tmp_path, get)
    assert path == tmp_path / "iceberg-spark-runtime-4.0_2.13-1.11.0.jar"
    assert path.read_bytes() == JAR
    assert not list(tmp_path.glob("*.part"))


def test_sha_mismatch_deletes_the_download_and_names_both_hashes(fj, tmp_path):
    get = _maven(fj, data=b"replaced upstream")
    with pytest.raises(fj.FetchError) as err:
        fj.ensure_jar(_entry(), tmp_path, get)
    msg = str(err.value)
    assert COORD in msg
    assert hashlib.sha256(b"replaced upstream").hexdigest() in msg
    assert _entry()["sha256"] in msg
    assert not list(tmp_path.iterdir())


def test_cached_jar_is_hashed_again_not_trusted(fj, tmp_path):
    get = _maven(fj)
    cached = tmp_path / fj.jar_name(COORD)
    cached.write_bytes(b"corrupt")
    assert fj.ensure_jar(_entry(), tmp_path, get).read_bytes() == JAR
    assert len(get.calls) == 1
    # A good cached copy is not downloaded again.
    fj.ensure_jar(_entry(), tmp_path, get)
    assert len(get.calls) == 1


@pytest.mark.parametrize(
    "err",
    [
        urllib.error.HTTPError("u", 429, "Too Many Requests", {}, None),  # type: ignore[arg-type]
        urllib.error.HTTPError("u", 503, "Unavailable", {}, None),  # type: ignore[arg-type]
        urllib.error.URLError("connection refused"),
        http.client.IncompleteRead(b"partial"),
        TimeoutError("read timed out"),
    ],
)
def test_central_429_5xx_or_down_falls_back_to_the_mirror(fj, tmp_path, err):
    get = _maven(fj, **{fj.CENTRAL: err})
    assert fj.ensure_jar(_entry(), tmp_path, get).read_bytes() == JAR
    assert get.calls[0].startswith(fj.CENTRAL) and get.calls[1].startswith(fj.MIRROR)


def test_central_404_is_not_retried_on_the_mirror(fj, tmp_path):
    get = FakeMaven({})
    with pytest.raises(fj.FetchError, match="404"):
        fj.ensure_jar(_entry(), tmp_path, get)
    assert len(get.calls) == 1


def test_both_hosts_failing_names_the_coordinate_path(fj, tmp_path):
    down = urllib.error.HTTPError("u", 502, "Bad Gateway", {}, None)  # type: ignore[arg-type]
    get = _maven(fj, **{fj.CENTRAL: down, fj.MIRROR: down})
    with pytest.raises(fj.FetchError, match="iceberg-spark-runtime-4.0_2.13/1.11.0"):
        fj.ensure_jar(_entry(), tmp_path, get)


def test_print_env_lists_the_legs_jars_in_lock_order(fj, tmp_path, monkeypatch, capsys):
    other = b"delta bytes"
    dcoord = "io.delta:delta-spark_2.13:4.0.0"
    lock = {
        "schema": 1,
        "legs": {"4.0": {"pyspark": "4.0.1", "jars": [_entry(), _entry(other, dcoord)]}},
    }
    (tmp_path / "lock.json").write_text(json.dumps(lock))
    files = {fj.artifact_path(COORD): JAR, fj.artifact_path(dcoord): other}
    monkeypatch.setattr(fj, "_http_get", FakeMaven(files))
    cache = tmp_path / "cache"
    rc = fj.main(
        [
            "--leg",
            "4.0",
            "--lock",
            str(tmp_path / "lock.json"),
            "--cache",
            str(cache),
            "--print-env",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out.strip()
    assert out == "LB_SPARK_TEST_JARS=" + ",".join(
        str(cache / n)
        for n in ("iceberg-spark-runtime-4.0_2.13-1.11.0.jar", "delta-spark_2.13-4.0.0.jar")
    )


def test_unknown_leg_exits_1(fj, tmp_path, capsys):
    (tmp_path / "lock.json").write_text(json.dumps({"schema": 1, "legs": {}}))
    rc = fj.main(["--leg", "3.5", "--lock", str(tmp_path / "lock.json"), "--cache", str(tmp_path)])
    assert rc == 1
    assert "no leg '3.5'" in capsys.readouterr().err


_DELTA_POM = b"""<?xml version="1.0"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <dependencies>
    <dependency><groupId>io.delta</groupId><artifactId>delta-storage</artifactId>
      <version>4.1.0</version></dependency>
    <dependency><groupId>org.antlr</groupId><artifactId>antlr4-runtime</artifactId>
      <version>4.13.1</version></dependency>
    <dependency><groupId>io.delta</groupId><artifactId>delta-test</artifactId>
      <version>4.1.0</version><scope>test</scope></dependency>
  </dependencies>
</project>
"""


def test_compile_dependencies_reads_the_delta_pom(fj):
    assert fj.compile_dependencies(_DELTA_POM, "io.delta") == ["io.delta:delta-storage:4.1.0"]


def test_update_lock_pins_what_central_serves(fj, tmp_path, monkeypatch):
    """The rebuilt lock carries the product coordinates, delta-storage from
    the POM, and sha256 values of bytes that match Central's .sha1."""
    files: dict[str, bytes] = {}
    for leg in fj.LEG_PYSPARK:
        coords = fj.product_coordinates(leg)
        dver = coords["delta"].split(":")[2]
        storage = f"io.delta:delta-storage:{dver}"
        for c in (coords["iceberg"], coords["delta"], storage):
            data = c.encode()
            files[fj.artifact_path(c)] = data
            files[fj.artifact_path(c, "pom")] = b"<project/>"
            files[fj.artifact_path(c, "jar.sha1")] = hashlib.sha1(data).hexdigest().encode()  # noqa: S324
        files[fj.artifact_path(coords["delta"], "pom")] = _DELTA_POM.replace(
            b"4.1.0", dver.encode()
        )
    lock = fj.build_lock(tmp_path, FakeMaven(files))
    assert set(lock["legs"]) == {"4.0", "4.1"}
    for leg, body in lock["legs"].items():
        coords = fj.product_coordinates(leg)
        assert [j["coord"] for j in body["jars"]][:2] == [coords["iceberg"], coords["delta"]]
        assert body["jars"][2]["transitive_of"] == coords["delta"]
        for j in body["jars"]:
            assert j["sha256"] == hashlib.sha256(j["coord"].encode()).hexdigest()


def test_update_lock_refuses_bytes_that_do_not_match_central_sha1(fj, tmp_path):
    coords = fj.product_coordinates("4.0")
    files = {
        fj.artifact_path(coords["iceberg"]): b"tampered",
        fj.artifact_path(coords["iceberg"], "pom"): b"<project/>",
        fj.artifact_path(coords["iceberg"], "jar.sha1"): b"0" * 40,
        fj.artifact_path(coords["delta"], "pom"): _DELTA_POM.replace(b"4.1.0", b"4.0.0"),
    }
    with pytest.raises(fj.FetchError, match="do not match Central's .sha1"):
        fj.build_lock(tmp_path, FakeMaven(files))
