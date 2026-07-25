"""Garage object store deployment for local mode.

Garage is the bundled S3 backend when no cluster or external object store is
available. It was selected over MinIO (community edition is maintenance-only
since 2025) and SeaweedFS (bucket enumeration is silently broken, so destroy
cannot clean up). Garage passes the full conformance suite at a 21.7 MB image
and roughly 4 MB idle RSS.

Garage is not usable straight from ``run``. It needs a five-step bootstrap
before the S3 API will answer:

    1. read the node id
    2. assign a layout to that node
    3. apply the layout
    4. create an access key
    5. create buckets and grant the key access to them

This deployer encodes that sequence. It is idempotent: re-running against an
already-bootstrapped instance is a no-op rather than an error.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass

from lakebench.runtime.container import ContainerRuntime
from lakebench.runtime.protocol import ComponentSpec, ContainerPort, Mount

logger = logging.getLogger(__name__)

DEFAULT_IMAGE = "docker.io/dxflrs/garage:v1.0.1"
DEFAULT_S3_PORT = 3900
COMPONENT = "garage"

# Garage refuses to start without an rpc_secret. For a single-node local
# instance reachable only on loopback this is not a security boundary, so a
# fixed value keeps the deployment reproducible.
_LOCAL_RPC_SECRET = "0123456789abcdef" * 4

CONFIG_TEMPLATE = """\
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "{rpc_secret}"

[s3_api]
s3_region = "{region}"
api_bind_addr = "[::]:{port}"
root_domain = ".s3.garage"
"""


@dataclass
class GarageCredentials:
    """Credentials minted during bootstrap."""

    access_key: str
    secret_key: str
    endpoint: str
    region: str


class GarageDeployError(RuntimeError):
    """Garage could not be deployed or bootstrapped."""


class GarageDeployer:
    """Deploys Garage as a container and bootstraps it for lakebench.

    Mirrors the Kubernetes deployer contract (``deploy``, ``destroy``) but runs
    against a ``ContainerRuntime`` rather than a Kubernetes client.
    """

    def __init__(
        self,
        runtime: ContainerRuntime,
        config_dir: str,
        image: str = DEFAULT_IMAGE,
        port: int = DEFAULT_S3_PORT,
        region: str = "us-east-1",
        buckets: tuple[str, ...] = ("lb-bronze", "lb-silver", "lb-gold"),
        key_name: str = "lakebench",
    ) -> None:
        """
        Args:
            runtime: Container runtime to deploy onto.
            config_dir: Host directory for the rendered garage.toml. Must be
                writable and readable by the container.
            image: Garage image reference.
            port: Host port for the S3 API.
            region: S3 region. Garage validates the sigv4 region scope, so
                this must match what clients sign with (see LB-052).
            buckets: Buckets to create and grant access to.
            key_name: Name of the access key to create.
        """
        self.runtime = runtime
        self.config_dir = config_dir.rstrip("/")
        self.image = image
        self.port = port
        self.region = region
        self.buckets = buckets
        self.key_name = key_name

    # -- lifecycle ----------------------------------------------------------

    def deploy(self, timeout: int = 180) -> GarageCredentials:
        """Start Garage, bootstrap it, and return usable credentials.

        Raises:
            GarageDeployError: if the container does not become ready or the
                bootstrap sequence fails.
        """
        self._write_config()

        spec = ComponentSpec(
            name=COMPONENT,
            image=self.image,
            ports=[ContainerPort(container_port=self.port, host_port=self.port)],
            mounts=[
                Mount(
                    source=f"{self.config_dir}/garage.toml",
                    target="/etc/garage.toml",
                    read_only=True,
                ),
                # Metadata (including access keys) and data live on a host
                # directory so they survive a container recreate. Without
                # this, redeploying silently mints new credentials and orphans
                # every existing bucket.
                Mount(source=f"{self.config_dir}/meta", target="/var/lib/garage/meta"),
                Mount(source=f"{self.config_dir}/data", target="/var/lib/garage/data"),
            ],
            labels={"lakebench.component": COMPONENT},
        )
        self.runtime.register_readiness(COMPONENT, ["/garage", "status"])
        self.runtime.apply(spec)

        if not self.runtime.wait_ready(COMPONENT, timeout=timeout):
            logs = self.runtime.logs(COMPONENT)
            raise GarageDeployError(f"Garage did not become ready within {timeout}s.\n{logs}")

        self._bootstrap_layout()
        creds = self._create_key()
        self._create_buckets(creds)
        return creds

    def destroy(self) -> None:
        """Remove the Garage container. Data in the container is discarded."""
        self.runtime.delete(COMPONENT)

    # -- bootstrap ----------------------------------------------------------

    def _write_config(self) -> None:
        import pathlib

        path = pathlib.Path(self.config_dir)
        path.mkdir(parents=True, exist_ok=True)
        # Bind-mount targets must exist on the host first.
        (path / "meta").mkdir(exist_ok=True)
        (path / "data").mkdir(exist_ok=True)
        (path / "garage.toml").write_text(
            CONFIG_TEMPLATE.format(rpc_secret=_LOCAL_RPC_SECRET, region=self.region, port=self.port)
        )

    def _garage(self, *args: str, check: bool = True) -> str:
        code, out = self.runtime.exec(COMPONENT, ["/garage", *args])
        if check and code != 0:
            raise GarageDeployError(f"garage {' '.join(args)} failed: {out.strip()}")
        return out

    def _node_id(self) -> str:
        """Read this node's id, retrying while the RPC layer settles."""
        for _ in range(15):
            code, out = self.runtime.exec(COMPONENT, ["/garage", "node", "id", "-q"])
            if code == 0 and out.strip():
                # Format is <id>@<addr>; the layout command wants just the id.
                return out.strip().split("@")[0]
            time.sleep(2)
        raise GarageDeployError("Could not read Garage node id")

    def _bootstrap_layout(self) -> None:
        """Assign and apply the cluster layout.

        Idempotent: an already-configured node reports no changes, which is a
        success rather than an error.
        """
        node_id = self._node_id()
        self._garage("layout", "assign", "-z", "dc1", "-c", "1G", node_id, check=False)

        out = self._garage("layout", "apply", "--version", "1", check=False)
        lowered = out.lower()
        if "applied" in lowered or "no changes" in lowered or "already" in lowered:
            return
        # Layout may already be at a later version from a previous run.
        status = self._garage("layout", "show", check=False)
        if "no role" in status.lower():
            raise GarageDeployError(f"Garage layout not applied: {out.strip()}")

    def _existing_key_ids(self) -> list[str]:
        """Return key IDs already registered under ``key_name``.

        ``garage key create`` is NOT idempotent: it succeeds every time and
        creates a duplicate key with the same name. After two runs,
        ``garage key info <name>`` fails with "2 matching keys". So existence
        must be checked before creating.
        """
        out = self._garage("key", "list", check=False)
        ids = []
        for line in out.splitlines():
            match = re.match(r"\s*(GK\w+)\s+(\S+)\s*$", line)
            if match and match.group(2) == self.key_name:
                ids.append(match.group(1))
        return ids

    def _create_key(self) -> GarageCredentials:
        """Create the access key, or reuse it when it already exists."""
        existing = self._existing_key_ids()
        if existing:
            # Address by ID, not name: duplicates make the name ambiguous.
            out = self._garage("key", "info", existing[0], "--show-secret", check=False)
        else:
            out = self._garage("key", "create", self.key_name, check=False)

        access = self._extract(out, r"Key ID:\s*(\S+)")
        secret = self._extract(out, r"Secret key:\s*(\S+)")
        if not (access and secret):
            raise GarageDeployError(f"Could not parse Garage credentials from:\n{out}")

        # Needed so lakebench can create its own buckets later. Address by
        # ID rather than name so it stays unambiguous.
        self._garage("key", "allow", "--create-bucket", access, check=False)

        return GarageCredentials(
            access_key=access,
            secret_key=secret,
            endpoint=f"http://localhost:{self.port}",
            region=self.region,
        )

    def _create_buckets(self, creds: GarageCredentials) -> None:
        for bucket in self.buckets:
            self._garage("bucket", "create", bucket, check=False)
            self._garage(
                "bucket",
                "allow",
                "--read",
                "--write",
                "--owner",
                bucket,
                "--key",
                creds.access_key,
                check=False,
            )

    @staticmethod
    def _extract(text: str, pattern: str) -> str:
        match = re.search(pattern, text)
        return match.group(1).strip() if match else ""
