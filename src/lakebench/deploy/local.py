"""Local (non-Kubernetes) deployment orchestrator.

Runs the whole lakehouse on one host with podman or docker. Nine of the
fourteen Kubernetes deploy steps do not apply, which is what makes this
tractable:

    namespace       -> container name prefix
    secrets         -> env vars on the container
    s3-buckets      -> same boto3 code, pointed at Garage
    scratch-sc      -> host directory
    postgres        -> not needed (Iceberg hadoop catalog)
    hive / polaris  -> not needed (Iceberg hadoop catalog)
    rbac            -> not needed (no operator)
    spark-operator  -> not needed (spark local[*])
    trino           -> not needed (DuckDB reads Iceberg directly)
    object store    -> Garage container (the one addition)

The catalog is the key simplification. Iceberg's ``hadoop`` catalog type keeps
table metadata in the object store beside the data, so there is no catalog
service, no PostgreSQL, and no bootstrap.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from lakebench.deploy.garage import GarageCredentials, GarageDeployer
from lakebench.runtime.container import ContainerRuntime, detect_container_cli

logger = logging.getLogger(__name__)

# Components a Kubernetes deploy runs that have no local equivalent. Listed
# explicitly so `lakebench deploy --local` can report them as skipped rather
# than silently omitting them.
SKIPPED_COMPONENTS: tuple[tuple[str, str], ...] = (
    ("postgres", "not needed: Iceberg hadoop catalog stores metadata in S3"),
    ("hive", "not needed: Iceberg hadoop catalog stores metadata in S3"),
    ("polaris", "not needed: Iceberg hadoop catalog stores metadata in S3"),
    ("rbac", "not needed: no Spark Operator"),
    ("spark-operator", "not needed: Spark runs in local[*] mode"),
    ("trino", "not needed: DuckDB reads Iceberg directly"),
    ("observability", "not supported in local mode"),
)


@dataclass
class LocalDeployment:
    """Result of a local deploy: what runs, and how to reach it."""

    credentials: GarageCredentials
    runtime_cli: str
    workdir: Path
    buckets: tuple[str, ...]
    skipped: tuple[tuple[str, str], ...] = field(default=SKIPPED_COMPONENTS)

    @property
    def endpoint(self) -> str:
        return self.credentials.endpoint


class LocalDeployer:
    """Deploys and tears down the local stack.

    Mirrors the Kubernetes engine's deploy/destroy shape so the CLI can treat
    both paths the same way.
    """

    def __init__(
        self,
        workdir: str | Path,
        namespace: str = "lakebench",
        s3_port: int = 3900,
        region: str = "us-east-1",
        buckets: tuple[str, ...] = ("lb-bronze", "lb-silver", "lb-gold"),
        cli: str = "",
    ) -> None:
        """
        Args:
            workdir: Host directory for Garage config, metadata, and data.
            namespace: Container name prefix and label, so concurrent local
                runs on one host do not collide.
            s3_port: Host port for the Garage S3 API.
            region: S3 region. Garage validates the sigv4 scope, so this must
                match what Spark signs with (LB-052).
            buckets: Medallion buckets to create.
            cli: Container CLI. Auto-detected when empty.
        """
        self.workdir = Path(workdir).expanduser()
        self.namespace = namespace
        self.s3_port = s3_port
        self.region = region
        self.buckets = buckets
        self.runtime = ContainerRuntime(cli=cli or detect_container_cli(), namespace=namespace)

    def deploy(self, timeout: int = 180) -> LocalDeployment:
        """Bring up the local stack and return connection details."""
        self.workdir.mkdir(parents=True, exist_ok=True)

        garage = GarageDeployer(
            self.runtime,
            config_dir=str(self.workdir / "garage"),
            port=self.s3_port,
            region=self.region,
            buckets=self.buckets,
        )
        creds = garage.deploy(timeout=timeout)

        return LocalDeployment(
            credentials=creds,
            runtime_cli=self.runtime.cli,
            workdir=self.workdir,
            buckets=self.buckets,
        )

    def destroy(self, remove_data: bool = False) -> int:
        """Tear down every managed container.

        Args:
            remove_data: Also delete the host working directory. Off by
                default: an accidental destroy should not discard data that
                took hours to generate.

        Returns:
            Number of containers removed.
        """
        removed = self.runtime.delete_all()
        if remove_data and self.workdir.exists():
            import shutil

            shutil.rmtree(self.workdir, ignore_errors=True)
        return removed

    def status(self) -> list[str]:
        """Return names of running managed components."""
        return self.runtime.list_managed()
