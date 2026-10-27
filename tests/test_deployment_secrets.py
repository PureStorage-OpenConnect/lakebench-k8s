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
from lakebench.deploy import deployment_secrets as ds
from tests.fixtures.deployment_secrets_helpers import FakeCore as FakeCore


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


def test_stored_polaris_password_is_reused():
    core = FakeCore({("a", ds.POLARIS_DB_SECRET): {"password": "stored-pw"}})
    assert ds.ensure_polaris_db_password(core, _cfg(), "a", role_exists=True) == "stored-pw"
    assert core.creates == []


class _TerminatingPvcCore(FakeCore):
    def read_namespaced_persistent_volume_claim(self, name, ns):
        return SimpleNamespace(metadata=SimpleNamespace(deletion_timestamp="2026-10-01T00:00:00Z"))


@pytest.mark.parametrize(
    ("core", "legacy"),
    [
        # A v1.6 deployment keeps its Secret.
        (FakeCore({("a", ds.HIVE_DB_SECRET): {"password": ds.LEGACY_HIVE_DB_PASSWORD}}), True),
        # The Postgres PVC outlived the Secret: its data has the v1.6 default.
        (FakeCore(pvcs={("a", ds.POSTGRES_PVC)}), True),
        # Right after a destroy the old claim is still Terminating: the next
        # Postgres initdbs a new volume, so it must not get the public default.
        (_TerminatingPvcCore(), False),
    ],
    ids=["v16-secret", "pvc-without-secret", "terminating-pvc"],
)
def test_hive_password_follows_the_existing_state(core, legacy):
    assert (ds.hive_db_password(core, "a") == ds.LEGACY_HIVE_DB_PASSWORD) is legacy


def test_secret_without_key_is_an_error_not_a_guess():
    core = FakeCore({("a", ds.HIVE_DB_SECRET): {"username": "hive"}})
    with pytest.raises(ds.DeploymentSecretError, match="no key 'password'"):
        ds.hive_db_password(core, "a")


# -- client secret ---------------------------------------------------------------


def test_config_client_secret_is_stored_for_a_fresh_polaris():
    core = FakeCore({("a", ds.POLARIS_CLIENT_SECRET): {"clientSecret": "old"}})
    assert (
        ds.ensure_polaris_client_secret(core, _cfg(client_secret="cfg"), "a", fresh=True) == "cfg"
    )
    assert core.secrets[("a", ds.POLARIS_CLIENT_SECRET)]["clientSecret"] == "cfg"


def test_config_cannot_change_a_bootstrapped_client_secret():
    """Polaris keeps only a hash of the bootstrap secret: a config value that
    differs is refused (replacing the Secret would lose the only copy and lock
    every client out); a config value that matches is accepted unchanged."""
    # A different value is refused and the stored secret is untouched.
    core = FakeCore({("a", ds.POLARIS_CLIENT_SECRET): {"clientSecret": "bootstrapped"}})
    with pytest.raises(ds.DeploymentSecretError, match="differs from the secret Polaris"):
        ds.ensure_polaris_client_secret(core, _cfg(client_secret="other"), "a", fresh=False)
    assert core.secrets[("a", ds.POLARIS_CLIENT_SECRET)]["clientSecret"] == "bootstrapped"
    assert core.replaces == []
    # The same value is accepted.
    same = FakeCore({("a", ds.POLARIS_CLIENT_SECRET): {"clientSecret": "same"}})
    assert (
        ds.ensure_polaris_client_secret(same, _cfg(client_secret="same"), "a", fresh=False)
        == "same"
    )


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


# -- consumers render no literal --------------------------------------------------------


def _render(template: str, recipe: str) -> str:
    from tests.fixtures.functional_templates_helpers import _enrich_context, _make_engine

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


@pytest.mark.parametrize(
    ("template", "recipe"),
    [
        ("spark-thrift/sparkapplication.yaml.j2", "polaris-iceberg-spark-thrift"),
        ("trino/configmap.yaml.j2", "polaris-iceberg-spark-trino"),
        ("trino/coordinator.yaml.j2", "polaris-iceberg-spark-trino"),
        ("trino/worker.yaml.j2", "polaris-iceberg-spark-trino"),
        ("polaris/deployment.yaml.j2", "polaris-iceberg-spark-trino"),
        ("polaris/bootstrap-job.yaml.j2", "polaris-iceberg-spark-trino"),
    ],
)
def test_rendered_consumers_carry_no_literal_secret(template, recipe):
    text = _render(template, recipe)
    for literal in ("AKIA-LITERAL-ACCESS", "LITERAL-SECRET-KEY", "lakebench-polaris-2024"):
        assert literal not in text


# -- secrets step and destroy --------------------------------------------------------


def test_secrets_step_keeps_the_stored_hive_password():
    from tests.fixtures.functional_templates_helpers import _make_engine

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
        patch("kubernetes.client.AppsV1Api", return_value=MagicMock()),
        patch("kubernetes.client.CustomObjectsApi", return_value=MagicMock()),
        patch("lakebench.spark.SparkOperatorManager", MagicMock()),
        patch("lakebench.deploy.destroy.logger"),
    ):
        destroy_all(engine, clean_buckets=False)
    return [c.args[0] for c in core.delete_namespaced_secret.call_args_list]


_ALWAYS_DELETED = {"lakebench-ca-certificate", "lakebench-s3-credentials", "lakebench-grafana"}
_PVC_BOUND = {ds.HIVE_DB_SECRET, ds.POLARIS_DB_SECRET, ds.POLARIS_CLIENT_SECRET}
_TERMINATING = SimpleNamespace(metadata=SimpleNamespace(deletion_timestamp="2026-10-01T00:00:00Z"))


@pytest.mark.parametrize(
    ("state", "kept_with_the_data"),
    [
        # Namespace and claim gone: the secrets go with the data.
        ({"pvc_present": False}, False),
        # create_namespace=false with a surviving PVC: a redeploy must find
        # the passwords its data was initialised with.
        ({"pvc_present": True}, True),
        # The usual path: step 8 just deleted the claim, so it is Terminating.
        ({"pvc_present": True, "pvc": _TERMINATING}, False),
        # An unreadable claim is treated as surviving.
        ({"pvc_present": True, "pvc_error": ApiException(status=500)}, True),
    ],
    ids=["no-pvc", "pvc-survives", "pvc-terminating", "pvc-unreadable"],
)
def test_destroy_deletes_db_secrets_only_when_their_data_goes(state, kept_with_the_data):
    deleted = set(_destroy_secret_deletes(**state))
    assert deleted == _ALWAYS_DELETED | (set() if kept_with_the_data else _PVC_BOUND)


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
        if sql.startswith("CREATE SCHEMA"):
            return "CREATE SCHEMA"
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
    """RFC 7677 section 3 example (password "pencil", salt W22Z...): the
    StoredKey and ServerKey are derived from that vector."""
    salt = base64.b64decode("W22ZaJ0SNY7soEsUEjb6gQ==")
    assert ds.scram_sha256_verifier("pencil", salt=salt) == (
        "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$"
        "WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU="
    )


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


def test_secrets_step_renders_the_hive_secret_that_won_a_racing_create():
    """409-safe: an overlapping deploy that won the create keeps its value."""
    from tests.fixtures.functional_templates_helpers import _make_engine

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
        if sql.startswith("CREATE SCHEMA"):
            return "CREATE SCHEMA"
        return "polaris" if "pg_database" in sql else "GRANT"

    with (
        patch("kubernetes.client.CoreV1Api", return_value=FakeCore()),
        patch("kubernetes.stream.stream", fake_stream),
    ):
        PolarisDeployer(engine)._create_polaris_db("a")
    assert any(s.startswith("ALTER ROLE polaris") for s in sqls)


# -- empty stored values -------------------------------------------------------


def test_an_empty_stored_secret_is_refused_not_used():
    """A tampered Secret holding "" must never bootstrap Polaris, be synced to
    the role, or be used as the DB password, whichever secret is stored
    empty and (for the client secret) whether or not this is a fresh deploy."""

    def client(core, fresh):
        ds.ensure_polaris_client_secret(core, _cfg(), "a", fresh=fresh)

    def db(core, _fresh):
        ds.ensure_polaris_db_password(core, _cfg(), "a", role_exists=True)

    cases = [
        # (secret key, stored value, call, fresh)
        ((ds.POLARIS_CLIENT_SECRET, ds.POLARIS_CLIENT_KEY), client, True),
        ((ds.POLARIS_CLIENT_SECRET, ds.POLARIS_CLIENT_KEY), client, False),
        ((ds.POLARIS_DB_SECRET, ds.POLARIS_DB_KEY), db, False),
    ]
    for (secret_name, key), call, fresh in cases:
        core = FakeCore({("a", secret_name): {key: ""}})
        with pytest.raises(ds.DeploymentSecretError, match="empty"):
            call(core, fresh)
        assert core.creates == [] and core.replaces == [], (secret_name, fresh)


@pytest.mark.parametrize(
    "user",
    [None, "(?i)secret|password|mytoken"],
)
def test_job_redaction_regex_always_hides_the_catalog_credential(user):
    """A user's spark.redaction.regex is kept, with Lakebench's terms in front:
    the Polaris client secret sits in sparkConf as ...catalog.<name>.credential."""
    import re

    from lakebench.modules.pipeline_engines.spark.job import (
        SPARK_REDACTION_REGEX,
        redaction_regex,
    )

    regex = redaction_regex(user)
    if not user:
        assert regex == SPARK_REDACTION_REGEX
        return
    # Lakebench's own group leads, ahead of whatever the user wrote.
    ours = re.match(r"\(\?i:[^)]*\)", regex)
    assert ours and re.search(ours.group(0), "spark.sql.catalog.lakehouse.credential")
    assert user in regex


def test_polaris_db_creates_the_schema_polaris_1_8_bootstrap_needs():
    sqls = _polaris_db("lbrole:0", FakeCore())
    schema = [s for s in sqls if s.startswith("CREATE SCHEMA")]
    assert schema == ["CREATE SCHEMA IF NOT EXISTS polaris_schema AUTHORIZATION polaris;"]
    # After the database and its grants, before anything bootstraps.
    assert sqls.index(schema[0]) > max(
        i for i, s in enumerate(sqls) if s.startswith("GRANT ALL ON SCHEMA")
    )


def test_bootstrap_job_polls_health_on_the_management_port():
    """Polaris serves /q/health on 8182; polling the REST port 8181 never
    succeeds and made every deploy wait out the whole loop."""
    text = _render("polaris/bootstrap-job.yaml.j2", "polaris-iceberg-spark-trino")
    assert ":8182" in text.split("POLARIS_HEALTH_URI")[1].splitlines()[1]
    assert '"$POLARIS_URI/q/health' not in text


def _run_setup_catalog(tmp_path, grant_code: str, namespace_code: str = "200"):
    """Run the rendered setup-catalog script against a fake curl.

    The fake answers the token call, reports `grant_code` for the grant PUTs,
    `namespace_code` for the namespace POSTs and 200 for every other call that
    asks for a status, and logs each call.
    """
    import os
    import subprocess

    import yaml

    text = _render("polaris/bootstrap-job.yaml.j2", "polaris-iceberg-spark-trino")
    job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
    container = next(
        c for c in job["spec"]["template"]["spec"]["containers"] if c["name"] == "setup-catalog"
    )
    bindir = tmp_path / "bin"
    bindir.mkdir()
    calls = tmp_path / "calls.log"
    (bindir / "curl").write_text(
        "#!/bin/sh\n"
        'echo "$*" >> "$CALLS"\n'
        'case "$*" in *oauth/tokens*) echo \'{"access_token":"tok"}\'; exit 0;; esac\n'
        'case "$*" in *"-w"*) ;; *) exit 0;; esac\n'
        'case "$*" in *"-X PUT"*) printf "%s" "$GRANT_CODE";;'
        ' */namespaces*) printf "%s" "$NAMESPACE_CODE";; *) printf 200;; esac\n'
    )
    (bindir / "curl").chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
        "CALLS": str(calls),
        "GRANT_CODE": grant_code,
        "NAMESPACE_CODE": namespace_code,
        **{e["name"]: e.get("value", "x") for e in container["env"]},
    }
    done = subprocess.run(
        ["sh", "-c", container["args"][0]], env=env, capture_output=True, text=True, timeout=60
    )
    return done, calls.read_text().splitlines()


def test_bootstrap_job_grants_the_data_privileges_that_authorize_metrics_reports(tmp_path):
    """Polaris refuses the Iceberg client's scan-metrics report (REPORT_READ_METRICS)
    without TABLE_READ_DATA, and every Spark and Trino scan logged the 403."""
    done, calls = _run_setup_catalog(tmp_path, "201")
    assert done.returncode == 0, done.stderr
    grants = [c for c in calls if "catalog-roles/catalog_admin/grants" in c]
    assert len(grants) == 2
    assert all("-X PUT" in c and '"type": "catalog"' in c for c in grants)
    assert '"privilege": "TABLE_READ_DATA"' in grants[0]
    assert '"privilege": "TABLE_WRITE_DATA"' in grants[1]
    # After the catalog and its namespaces exist, and before the job ends.
    last_namespace = max(i for i, c in enumerate(calls) if "/namespaces" in c)
    assert calls.index(grants[0]) > last_namespace


def test_a_refused_namespace_location_fails_the_bootstrap(tmp_path):
    """Polaris refuses a namespace location outside the catalog's default
    base; the namespace would then root in the bronze bucket, so the job
    fails instead of reporting success."""
    done, _ = _run_setup_catalog(tmp_path, "201", namespace_code="400")
    assert done.returncode != 0
    assert "ERROR: Namespace 'default' creation returned HTTP 400" in done.stdout
    assert "Polaris bootstrap complete" not in done.stdout


def test_a_refused_grant_warns_and_does_not_fail_the_deploy(tmp_path):
    done, _ = _run_setup_catalog(tmp_path, "403")
    assert done.returncode == 0, done.stderr
    assert "WARNING: Granting TABLE_READ_DATA returned HTTP 403" in done.stdout
    assert "Polaris bootstrap complete" in done.stdout
