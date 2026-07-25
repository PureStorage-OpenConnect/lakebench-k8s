"""Spark execution in local[*] mode, without the Spark Operator.

Runs the same pipeline scripts the Kubernetes path uses. The scripts are driven
entirely by ``LB_*`` environment variables, so nothing about them changes; only
the submission mechanism differs.

Two workarounds are mandatory here and are not needed on the operator path:

- **Ivy cache (LB-053).** The Spark image user has no home directory, so
  ``--packages`` fails with ``FileNotFoundException:
  /nonexistent/.ivy2.5.2/cache/...``. ``spark.jars.ivy`` and ``HOME`` must
  point somewhere writable.
- **S3A region (LB-052).** Without ``fs.s3a.endpoint.region``, S3A signs with
  a default region. Garage validates the sigv4 scope and rejects it with an
  opaque 400.
"""

from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from lakebench.engine.protocol import JobResult

logger = logging.getLogger(__name__)

DEFAULT_SPARK_IMAGE = "docker.io/apache/spark:4.0.2-python3"

# Scripts, relative to the packaged scripts directory.
_SCRIPTS = {
    "bronze-verify": "bronze_verify.py",
    "silver-build": "silver_build.py",
    "gold-finalize": "gold_finalize.py",
}

# Iceberg runtime per Spark major.minor. Mirrors _ICEBERG_RUNTIME_SUFFIX on the
# Kubernetes path; kept separate so the local default can move independently.
_ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1"
_HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.4.1"


@dataclass
class LocalSparkConfig:
    """Everything a local Spark submission needs."""

    endpoint: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    bronze_bucket: str = "lb-bronze"
    silver_bucket: str = "lb-silver"
    gold_bucket: str = "lb-gold"
    image: str = DEFAULT_SPARK_IMAGE
    driver_memory: str = "3g"
    cores: int = 2
    catalog: str = "lb"


class LocalSparkRunner:
    """Submits pipeline jobs to a containerised Spark in local[*] mode."""

    def __init__(
        self,
        config: LocalSparkConfig,
        workdir: str | Path,
        cli: str = "podman",
        scripts_dir: str | Path | None = None,
    ) -> None:
        """
        Args:
            config: Connection and sizing settings.
            workdir: Host directory for the Ivy cache and mounted scripts.
                The Ivy cache is roughly 1.2 GB, so it is deliberately
                persistent: re-downloading it dominates a cold run.
            cli: Container CLI (podman or docker).
            scripts_dir: Override the packaged pipeline scripts.
        """
        self.config = config
        self.workdir = Path(workdir).expanduser()
        self.cli = cli
        self.scripts_dir = (
            Path(scripts_dir)
            if scripts_dir
            else Path(__file__).resolve().parents[3] / "spark" / "scripts"
        )

    # -- submission ---------------------------------------------------------

    def _spark_conf(self) -> dict[str, str]:
        cfg = self.config
        warehouse = f"s3a://{cfg.bronze_bucket}/warehouse"
        return {
            "spark.driver.memory": cfg.driver_memory,
            # LB-053: the image user has no home, so Ivy cannot write its cache.
            "spark.jars.ivy": "/work/ivy",
            f"spark.sql.catalog.{cfg.catalog}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{cfg.catalog}.type": "hadoop",
            f"spark.sql.catalog.{cfg.catalog}.warehouse": warehouse,
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
            "spark.hadoop.fs.s3a.endpoint": cfg.endpoint,
            # LB-052: Garage validates the sigv4 region scope.
            "spark.hadoop.fs.s3a.endpoint.region": cfg.region,
            "spark.hadoop.fs.s3a.access.key": cfg.access_key,
            "spark.hadoop.fs.s3a.secret.key": cfg.secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(
                cfg.endpoint.startswith("https")
            ).lower(),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        }

    def _env(self) -> dict[str, str]:
        cfg = self.config
        return {
            "HOME": "/work",  # LB-053
            "LB_BRONZE_URI": f"s3a://{cfg.bronze_bucket}/",
            "LB_SILVER_URI": f"s3a://{cfg.silver_bucket}/",
            "LB_GOLD_URI": f"s3a://{cfg.gold_bucket}/",
            "LB_ICEBERG_CATALOG": cfg.catalog,
            "LB_CATALOG_TYPE": "hadoop",
            "LB_BRONZE_TABLE": "default.bronze_raw",
            "LB_SILVER_TABLE": "silver.customer_interactions_enriched",
            "LB_GOLD_TABLE": "gold.customer_executive_dashboard",
        }

    def build_command(self, job_type: str) -> list[str]:
        """Build the container command for a job. Exposed for testing."""
        script = _SCRIPTS.get(job_type)
        if not script:
            raise ValueError(f"Unknown job type {job_type!r}. Expected one of {sorted(_SCRIPTS)}.")

        cmd = [self.cli, "run", "--rm", "--network", "host"]
        for key, value in self._env().items():
            cmd += ["-e", f"{key}={value}"]
        cmd += ["-v", f"{self.workdir}:/work:Z"]
        cmd += ["-v", f"{self.scripts_dir}:/scripts:ro,Z"]
        cmd += [self.config.image, "/opt/spark/bin/spark-submit"]
        cmd += ["--master", f"local[{self.config.cores}]"]
        cmd += ["--packages", f"{_ICEBERG_RUNTIME},{_HADOOP_AWS}"]
        # common.py sits beside the job scripts and is imported by them.
        cmd += ["--py-files", "/scripts/common.py"]
        for key, value in self._spark_conf().items():
            cmd += ["--conf", f"{key}={value}"]
        cmd.append(f"/scripts/{script}")
        return cmd

    def run_job(self, job_type: str, timeout: int = 3600) -> JobResult:
        """Run one pipeline job and return its result."""
        (self.workdir / "ivy").mkdir(parents=True, exist_ok=True)
        cmd = self.build_command(job_type)

        logger.debug("Submitting %s: %s", job_type, " ".join(cmd))
        start = time.monotonic()
        try:
            proc = subprocess.run(  # noqa: S603
                cmd, capture_output=True, text=True, timeout=timeout, check=False
            )
        except subprocess.TimeoutExpired:
            return JobResult(
                job_id=job_type,
                job_type=job_type,
                success=False,
                elapsed_seconds=time.monotonic() - start,
                error_message=f"Job exceeded {timeout}s",
            )

        elapsed = time.monotonic() - start
        output = (proc.stdout or "") + (proc.stderr or "")
        if proc.returncode != 0:
            return JobResult(
                job_id=job_type,
                job_type=job_type,
                success=False,
                elapsed_seconds=elapsed,
                error_message=_summarise_failure(output),
                details={"output": output},
            )

        return JobResult(
            job_id=job_type,
            job_type=job_type,
            success=True,
            elapsed_seconds=elapsed,
            details={"output": output},
        )


def _summarise_failure(output: str) -> str:
    """Pull the useful line out of Spark's very long stack traces."""
    markers = ("Exception:", "Error:", "ERROR", "Caused by:")
    for line in output.splitlines():
        if any(m in line for m in markers):
            return line.strip()[:300]
    return output.strip()[-300:] if output.strip() else "Job failed with no output"
