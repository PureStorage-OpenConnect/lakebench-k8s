"""SAF-8: per-deployment secrets.

A new deployment gets its own Hive metastore DB password, Polaris DB password
and Polaris client secret; an existing (v1.6) deployment keeps its own. No
test reaches a cluster: the Kubernetes API is a fake keyed by namespace.
"""

from __future__ import annotations

import base64
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from lakebench.config import LakebenchConfig
from lakebench.config.schema import PolarisClientSecretMissing
from lakebench.deploy import deployment_secrets as ds


class FakeCore:
    """CoreV1Api subset: Secrets and PVCs per namespace."""

    def __init__(self, secrets: dict | None = None, pvcs: set | None = None):
        # {(ns, name): {key: plaintext}}
        self.secrets: dict[tuple[str, str], dict[str, str]] = dict(secrets or {})
        self.pvcs: set[tuple[str, str]] = set(pvcs or set())
        self.creates: list[dict] = []
        self.replaces: list[dict] = []

    def read_namespaced_secret(self, name, ns):
        if (ns, name) not in self.secrets:
            raise ApiException(status=404)
        data = {
            k: base64.b64encode(v.encode()).decode() for k, v in self.secrets[(ns, name)].items()
        }
        return SimpleNamespace(data=data)

    def create_namespaced_secret(self, ns, body):
        name = body["metadata"]["name"]
        if (ns, name) in self.secrets:
            raise ApiException(status=409)
        self.creates.append(body)
        self.secrets[(ns, name)] = dict(body["stringData"])

    def replace_namespaced_secret(self, name, ns, body):
        self.replaces.append(body)
        self.secrets[(ns, name)] = dict(body["stringData"])

    def connect_get_namespaced_pod_exec(self, *a, **k):  # only passed to stream()
        raise AssertionError("exec goes through the patched stream")

    def read_namespaced_persistent_volume_claim(self, name, ns):
        if (ns, name) not in self.pvcs:
            raise ApiException(status=404)
        return SimpleNamespace(metadata=SimpleNamespace(deletion_timestamp=None))


def _cfg(name="a", client_secret="") -> LakebenchConfig:
    return LakebenchConfig(
        name=name,
        platform={"kubernetes": {"namespace": name}},
        architecture={
            "catalog": {"type": "polaris", "polaris": {"client_secret": client_secret}},
            "table_format": {"type": "iceberg"},
            "query_engine": {"type": "trino"},
        },
    )


# -- the three secrets -----------------------------------------------------------


def test_two_new_deployments_get_distinct_secrets():
    core = FakeCore()
    got = {}
    for ns in ("dep-a", "dep-b"):
        cfg = _cfg(ns)
        got[ns] = (
            ds.hive_db_password(core, ns),
            ds.ensure_polaris_db_password(core, cfg, ns, role_exists=False),
            ds.ensure_polaris_client_secret(core, cfg, ns, fresh=True),
        )
    a, b = got["dep-a"], got["dep-b"]
    for i, what in enumerate(("hive db", "polaris db", "client secret")):
        assert a[i] != b[i], what
        assert a[i] not in (ds.LEGACY_HIVE_DB_PASSWORD, ds.LEGACY_POLARIS_DB_PASSWORD), what
        assert len(a[i]) >= 32, what


def test_v16_polaris_redeploy_keeps_password():
    """A v1.6 Polaris (role exists, no Secret) keeps the v1.6 DB password; a
    generator that ignored the role would lock Polaris out of its metastore."""
    core = FakeCore()
    pw = ds.ensure_polaris_db_password(core, _cfg(), "a", role_exists=True)
    assert pw == ds.LEGACY_POLARIS_DB_PASSWORD
    assert core.secrets[("a", ds.POLARIS_DB_SECRET)]["password"] == pw


def test_stored_polaris_password_is_reused():
    core = FakeCore({("a", ds.POLARIS_DB_SECRET): {"password": "stored-pw"}})
    assert ds.ensure_polaris_db_password(core, _cfg(), "a", role_exists=True) == "stored-pw"
    assert core.creates == []


def test_v16_hive_redeploy_keeps_password():
    core = FakeCore({("a", ds.HIVE_DB_SECRET): {"password": ds.LEGACY_HIVE_DB_PASSWORD}})
    assert ds.hive_db_password(core, "a") == ds.LEGACY_HIVE_DB_PASSWORD


def test_hive_pvc_without_secret_uses_legacy():
    """LB-187: the Postgres PVC outlived the Secret; its data has the v1.6 default."""
    core = FakeCore(pvcs={("a", ds.POSTGRES_PVC)})
    assert ds.hive_db_password(core, "a") == ds.LEGACY_HIVE_DB_PASSWORD


def test_terminating_pvc_is_not_v16_data():
    """Right after a destroy the old claim is still Terminating: the next
    Postgres initdbs a new volume, so it must not get the public default."""

    class Terminating(FakeCore):
        def read_namespaced_persistent_volume_claim(self, name, ns):
            return SimpleNamespace(
                metadata=SimpleNamespace(deletion_timestamp="2026-10-01T00:00:00Z")
            )

    assert ds.hive_db_password(Terminating(), "a") != ds.LEGACY_HIVE_DB_PASSWORD


def test_fresh_hive_password_is_generated():
    pw = ds.hive_db_password(FakeCore(), "a")
    assert pw != ds.LEGACY_HIVE_DB_PASSWORD and len(pw) >= 32


def test_secret_without_key_is_an_error_not_a_guess():
    core = FakeCore({("a", ds.HIVE_DB_SECRET): {"username": "hive"}})
    with pytest.raises(ds.DeploymentSecretError, match="no key 'password'"):
        ds.hive_db_password(core, "a")


def test_racing_create_returns_the_winners_value():
    core = FakeCore({("a", ds.POLARIS_CLIENT_SECRET): {"clientSecret": "winner"}})

    class Racy(FakeCore):
        def read_namespaced_secret(self, name, ns):
            if not getattr(self, "_raced", False):
                self._raced = True
                raise ApiException(status=404)
            return core.read_namespaced_secret(name, ns)

        def create_namespaced_secret(self, ns, body):
            raise ApiException(status=409)

    got = ds.ensure_polaris_client_secret(Racy(), _cfg(), "a", fresh=True)
    assert got == "winner"


# -- client secret ---------------------------------------------------------------


def test_config_client_secret_is_stored_for_a_fresh_polaris():
    core = FakeCore({("a", ds.POLARIS_CLIENT_SECRET): {"clientSecret": "old"}})
    assert (
        ds.ensure_polaris_client_secret(core, _cfg(client_secret="cfg"), "a", fresh=True) == "cfg"
    )
    assert core.secrets[("a", ds.POLARIS_CLIENT_SECRET)]["clientSecret"] == "cfg"


def test_config_cannot_change_a_bootstrapped_client_secret():
    """Polaris keeps only a hash of the bootstrap secret: replacing the
    Secret would lose the only copy and lock every client out."""
    core = FakeCore({("a", ds.POLARIS_CLIENT_SECRET): {"clientSecret": "bootstrapped"}})
    with pytest.raises(ds.DeploymentSecretError, match="differs from the secret Polaris"):
        ds.ensure_polaris_client_secret(core, _cfg(client_secret="other"), "a", fresh=False)
    assert core.secrets[("a", ds.POLARIS_CLIENT_SECRET)]["clientSecret"] == "bootstrapped"
    assert core.replaces == []


def test_matching_config_on_a_bootstrapped_polaris_is_fine():
    core = FakeCore({("a", ds.POLARIS_CLIENT_SECRET): {"clientSecret": "same"}})
    assert (
        ds.ensure_polaris_client_secret(core, _cfg(client_secret="same"), "a", fresh=False)
        == "same"
    )


def test_bootstrapped_polaris_without_any_secret_refuses():
    """A realm accepts only the secret it was bootstrapped with: never invent one."""
    with pytest.raises(ds.DeploymentSecretError, match="was bootstrapped with a client secret"):
        ds.ensure_polaris_client_secret(FakeCore(), _cfg(), "a", fresh=False)


def test_run_reads_client_secret_from_namespace():
    core = FakeCore({("a", ds.POLARIS_CLIENT_SECRET): {"clientSecret": "stored"}})
    assert ds.polaris_client_secret(_cfg(), core) == "stored"
    with pytest.raises(PolarisClientSecretMissing, match="run lakebench deploy first"):
        ds.polaris_client_secret(_cfg(), FakeCore())


def test_spark_job_uses_the_stored_client_secret():
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    core = FakeCore({("a", ds.POLARIS_CLIENT_SECRET): {"clientSecret": "stored-xyz"}})
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    mgr = SparkJobManager(_cfg(), k8s)
    with patch("kubernetes.client.CoreV1Api", return_value=core):
        conf = mgr._build_manifest(JobType.BRONZE_VERIFY)["spec"]["sparkConf"]
    creds = [v for k, v in conf.items() if k.endswith(".credential")]
    assert creds == ["lakebench:stored-xyz"]


# -- polaris role probe ------------------------------------------------------------


@pytest.mark.parametrize("out,expected", [(" lbrole:1\n", True), ("lbrole:0", False)])
def test_role_probe(out, expected):
    assert ds.polaris_role_exists(lambda db, sql: out) is expected


@pytest.mark.parametrize("out", ["", "psql: error: connection refused", "ERROR: 'lbrole:' x"])
def test_role_probe_refuses_to_guess(out):
    with pytest.raises(ds.DeploymentSecretError, match="rather than guess"):
        ds.polaris_role_exists(lambda db, sql: out)


# -- consumers render no literal --------------------------------------------------------


def _render(template: str, recipe: str) -> str:
    from tests.test_functional_templates import _enrich_context, _make_engine

    engine = _make_engine(
        recipe=recipe,
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://10.0.1.50:80",
                    "access_key": "AKIA-LITERAL-ACCESS",
                    "secret_key": "LITERAL-SECRET-KEY",
                }
            }
        },
    )
    ctx = _enrich_context(engine)
    return engine.renderer.render(template, ctx)


def test_thrift_spec_has_no_literal_keys():
    text = _render("spark-thrift/sparkapplication.yaml.j2", "polaris-iceberg-spark-thrift")
    assert "AKIA-LITERAL-ACCESS" not in text and "LITERAL-SECRET-KEY" not in text
    assert 'fs.s3a.access.key="$AWS_ACCESS_KEY_ID"' in text
    assert 'credential="lakebench:$POLARIS_CLIENT_SECRET"' in text
    assert "name: lakebench-polaris-client" in text


def test_trino_reads_client_secret_from_env():
    cm = _render("trino/configmap.yaml.j2", "polaris-iceberg-spark-trino")
    assert "oauth2.credential=lakebench:${ENV:POLARIS_CLIENT_SECRET}" in cm
    for t in ("trino/coordinator.yaml.j2", "trino/worker.yaml.j2"):
        assert "name: lakebench-polaris-client" in _render(t, "polaris-iceberg-spark-trino")


def test_polaris_templates_read_passwords_from_secrets():
    for t in ("polaris/deployment.yaml.j2", "polaris/bootstrap-job.yaml.j2"):
        text = _render(t, "polaris-iceberg-spark-trino")
        assert "lakebench-polaris-2024" not in text
        assert "name: lakebench-polaris-db" in text
    boot = _render("polaris/bootstrap-job.yaml.j2", "polaris-iceberg-spark-trino")
    assert '"POLARIS,lakebench,$CLIENT_SECRET"' in boot


def test_no_fixed_secret_left_in_templates_or_code():
    from pathlib import Path

    root = Path(ds.__file__).resolve().parents[1]
    hits = []
    for p in list(root.rglob("*.j2")) + list(root.rglob("*.py")):
        if p.name == "deployment_secrets.py":
            continue
        text = p.read_text(encoding="utf-8")
        for fixed in ("lakebench-polaris-2024", "lakebench-hive-2024", '"grafana.adminPassword"'):
            if fixed in text:
                hits.append(f"{p.relative_to(root)}: {fixed}")
    assert hits == []


# -- secrets step and destroy --------------------------------------------------------


def test_secrets_step_keeps_the_stored_hive_password():
    from tests.test_functional_templates import _make_engine

    engine = _make_engine()
    engine.dry_run = False
    ns = engine.config.get_namespace()
    core = FakeCore({(ns, ds.HIVE_DB_SECRET): {"password": "kept-pw"}})
    with patch("kubernetes.client.CoreV1Api", return_value=core):
        result = engine._deploy_secrets()
    assert result.status.name == "SUCCESS"
    applied = [c.args[0] for c in engine.k8s.apply_manifest.call_args_list]
    pg = next(m for m in applied if m["metadata"]["name"] == ds.HIVE_DB_SECRET)
    assert pg["stringData"]["password"] == "kept-pw"


def _destroy_secret_deletes(pvc_present: bool, pvc=None, pvc_error=None) -> list[str]:
    from lakebench.deploy.destroy import destroy_all
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    engine = MagicMock()
    engine.config.name = "u02"
    engine.config.get_namespace.return_value = "u02"
    engine.config.platform.kubernetes.create_namespace = False
    core = MagicMock()
    if pvc_error is not None:
        core.read_namespaced_persistent_volume_claim.side_effect = pvc_error
    elif pvc is not None:
        core.read_namespaced_persistent_volume_claim.return_value = pvc
    elif not pvc_present:
        core.read_namespaced_persistent_volume_claim.side_effect = ApiException(status=404)
    else:
        core.read_namespaced_persistent_volume_claim.return_value = SimpleNamespace(
            metadata=SimpleNamespace(deletion_timestamp=None)
        )
    match = IdentityReport(
        verdict=IdentityVerdict.MATCH,
        resource_name="u02",
        expected_deployment="u02",
        found_deployment="u02",
    )
    with (
        patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=match),
        patch("lakebench.deploy.ownership.verify_bucket_ownership", return_value=match),
        patch("kubernetes.client.CoreV1Api", return_value=core),
        patch("kubernetes.client.RbacAuthorizationV1Api", return_value=MagicMock()),
        patch("kubernetes.client.CustomObjectsApi", return_value=MagicMock()),
        patch("lakebench.spark.SparkOperatorManager", MagicMock()),
        patch("lakebench.deploy.destroy.logger"),
    ):
        destroy_all(engine, clean_buckets=False)
    return [c.args[0] for c in core.delete_namespaced_secret.call_args_list]


def test_destroy_removes_the_new_secrets_with_the_data():
    deleted = _destroy_secret_deletes(pvc_present=False)
    for name in (ds.HIVE_DB_SECRET, ds.POLARIS_DB_SECRET, ds.POLARIS_CLIENT_SECRET):
        assert name in deleted


def test_destroy_keeps_db_secrets_while_the_postgres_pvc_survives():
    """create_namespace=false with a surviving PVC (LB-187): a redeploy must
    find the passwords its data was initialised with."""
    deleted = _destroy_secret_deletes(pvc_present=True)
    assert "lakebench-s3-credentials" in deleted
    for name in (ds.HIVE_DB_SECRET, ds.POLARIS_DB_SECRET, ds.POLARIS_CLIENT_SECRET):
        assert name not in deleted


def _polaris_db(role_out: str, core: FakeCore, cfg: LakebenchConfig | None = None):
    from lakebench.modules.catalogs.polaris.deployer import PolarisDeployer

    engine = SimpleNamespace(
        config=cfg or _cfg(), k8s=MagicMock(), renderer=MagicMock(), context={}, dry_run=False
    )
    sqls: list[str] = []

    def fake_stream(fn, pod, ns, command, **kw):
        sql = command[-1]
        sqls.append(sql)
        if "lbrole:" in sql:
            return role_out
        if "pg_database" in sql:
            return "polaris"
        if sql.startswith("CREATE USER"):
            return "CREATE ROLE"
        if sql.startswith("ALTER ROLE"):
            return "ALTER ROLE"
        return "GRANT"

    with (
        patch("kubernetes.client.CoreV1Api", return_value=core),
        patch("kubernetes.stream.stream", fake_stream),
    ):
        PolarisDeployer(engine)._create_polaris_db("a")
    return sqls


def test_fresh_polaris_creates_user_with_its_own_password():
    core = FakeCore()
    sqls = _polaris_db("lbrole:0", core)
    pw = core.secrets[("a", ds.POLARIS_DB_SECRET)]["password"]
    create = next(s for s in sqls if s.startswith("CREATE USER"))
    assert "SCRAM-SHA-256$4096:" in create
    assert all(pw not in s for s in sqls), "the plaintext must not cross the exec request"
    assert core.secrets[("a", ds.POLARIS_CLIENT_SECRET)]["clientSecret"]


def test_v16_polaris_through_the_deployer_keeps_its_metastore():
    """Role exists, no Secret, config carries the 1.6 client secret: the
    legacy DB password is stored, the role is synced to it, no CREATE."""
    core = FakeCore()
    sqls = _polaris_db("lbrole:1", core, _cfg(client_secret="v16-secret"))
    assert core.secrets[("a", ds.POLARIS_DB_SECRET)]["password"] == ds.LEGACY_POLARIS_DB_PASSWORD
    assert core.secrets[("a", ds.POLARIS_CLIENT_SECRET)]["clientSecret"] == "v16-secret"
    assert not any(s.startswith("CREATE USER") for s in sqls)
    alter = [s for s in sqls if s.startswith("ALTER ROLE polaris")]
    assert alter and ds.LEGACY_POLARIS_DB_PASSWORD not in alter[0]


def test_v16_polaris_without_its_client_secret_stops():
    with pytest.raises(ds.DeploymentSecretError, match="was bootstrapped with a client secret"):
        _polaris_db("lbrole:1", FakeCore())


def test_unreadable_role_probe_stops_before_any_user_change():
    core = FakeCore()
    with pytest.raises(ds.DeploymentSecretError):
        _polaris_db("psql: error: could not connect", core)
    assert core.creates == []


def test_scram_verifier_matches_rfc7677():
    """RFC 7677 section 3 example: user "user", password "pencil"."""
    import hashlib
    import hmac

    salt = base64.b64decode("W22ZaJ0SNY7soEsUEjb6gQ==")
    v = ds.scram_sha256_verifier("pencil", salt=salt)
    head, keys = v.rsplit("$", 1)
    assert head == "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ=="
    stored, server = (base64.b64decode(k) for k in keys.split(":"))
    nonce = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
    auth = (
        f"n=user,r=rOprNGfwEbeRWgbNEkqO,r={nonce},s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,"
        f"c=biws,r={nonce}"
    ).encode()
    server_sig = base64.b64encode(hmac.new(server, auth, hashlib.sha256).digest()).decode()
    assert server_sig == "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
    proof = base64.b64decode("dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=")
    client_sig = hmac.new(stored, auth, hashlib.sha256).digest()
    client_key = bytes(a ^ b for a, b in zip(proof, client_sig, strict=True))
    assert hashlib.sha256(client_key).digest() == stored


def test_role_sync_failure_does_not_echo_psql_output():
    with pytest.raises(ds.DeploymentSecretError) as ei:
        ds.sync_role_password(
            lambda db, sql: f"ERROR:  syntax error\nLINE 1: {sql}", "hive", "pw-xyz"
        )
    assert "SCRAM" not in str(ei.value) and "pw-xyz" not in str(ei.value)


def test_postgres_step_syncs_the_hive_role_to_its_secret(monkeypatch):
    from lakebench.deploy import postgres as pg

    sent: list[str] = []
    monkeypatch.setattr(
        pg, "postgres_psql", lambda core, ns: lambda db, sql: sent.append(sql) or "ALTER ROLE"
    )
    d = pg.PostgresDeployer.__new__(pg.PostgresDeployer)
    d.context = {"postgres_password": "hive-pw"}
    with patch("kubernetes.client.CoreV1Api", return_value=FakeCore()):
        d._sync_hive_role("a")
    assert len(sent) == 1 and sent[0].startswith("ALTER ROLE hive WITH PASSWORD 'SCRAM-SHA-256$")
    assert "hive-pw" not in sent[0]


def test_secrets_step_creates_a_new_hive_secret_before_rendering():
    """409-safe: an overlapping deploy that won the create keeps its value."""
    from tests.test_functional_templates import _make_engine

    engine = _make_engine()
    engine.dry_run = False

    class Raced(FakeCore):
        def create_namespaced_secret(self, ns_, body):
            self.secrets[(ns_, body["metadata"]["name"])] = {"password": "winner-pw"}
            raise ApiException(status=409)

    with patch("kubernetes.client.CoreV1Api", return_value=Raced()):
        engine._deploy_secrets()
    applied = [c.args[0] for c in engine.k8s.apply_manifest.call_args_list]
    pg_secret = next(m for m in applied if m["metadata"]["name"] == ds.HIVE_DB_SECRET)
    assert pg_secret["stringData"]["password"] == "winner-pw"


def test_thrift_container_expands_env_through_bash():
    """The env references only work under a shell: pin it (SD-5b reworks
    this template)."""
    import yaml

    text = _render("spark-thrift/sparkapplication.yaml.j2", "polaris-iceberg-spark-thrift")
    docs = [d for d in yaml.safe_load_all(text) if d]
    containers = [
        c
        for d in docs
        for c in ((d.get("spec") or {}).get("template", {}).get("spec", {}).get("containers") or [])
        if c.get("name") == "spark-thrift"
    ]
    assert containers and containers[0]["command"][:2] == ["/bin/bash", "-c"]


def test_destroy_deletes_secrets_when_the_pvc_is_terminating():
    """The usual path: step 8 just deleted the claim, so it is Terminating."""
    pvc = SimpleNamespace(metadata=SimpleNamespace(deletion_timestamp="2026-10-01T00:00:00Z"))
    deleted = _destroy_secret_deletes(True, pvc=pvc)
    assert ds.POLARIS_CLIENT_SECRET in deleted and ds.HIVE_DB_SECRET in deleted


def test_destroy_keeps_db_secrets_when_the_pvc_cannot_be_read():
    deleted = _destroy_secret_deletes(True, pvc_error=ApiException(status=500))
    assert ds.HIVE_DB_SECRET not in deleted and ds.POLARIS_DB_SECRET not in deleted


def test_create_race_falls_through_to_sync():
    from lakebench.modules.catalogs.polaris.deployer import PolarisDeployer

    engine = SimpleNamespace(
        config=_cfg(), k8s=MagicMock(), renderer=MagicMock(), context={}, dry_run=False
    )
    probes = iter(["lbrole:0", "lbrole:1"])
    sqls: list[str] = []

    def fake_stream(fn, pod, ns, command, **kw):
        sql = command[-1]
        sqls.append(sql)
        if "lbrole:" in sql:
            return next(probes)
        if sql.startswith("CREATE USER"):
            return 'ERROR:  role "polaris" already exists'
        if sql.startswith("ALTER ROLE"):
            return "ALTER ROLE"
        return "polaris" if "pg_database" in sql else "GRANT"

    with (
        patch("kubernetes.client.CoreV1Api", return_value=FakeCore()),
        patch("kubernetes.stream.stream", fake_stream),
    ):
        PolarisDeployer(engine)._create_polaris_db("a")
    assert any(s.startswith("ALTER ROLE polaris") for s in sqls)
