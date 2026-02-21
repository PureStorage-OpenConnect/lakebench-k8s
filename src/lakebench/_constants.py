"""Shared constants for lakebench."""

# Kubernetes ServiceAccount/Role/RoleBinding name for Spark driver/executor pods
SPARK_SERVICE_ACCOUNT = "lakebench-spark-runner"

# Polaris OAuth2 client credentials used by the bootstrap job, Trino, and Spark.
POLARIS_CLIENT_ID = "lakebench"
POLARIS_CLIENT_SECRET = "lakebench-polaris-secret-2024"

# Unified output directory -- single top-level directory for all lakebench outputs.
# Contains:
#   journal/   -- session-scoped JSONL provenance logs
#   runs/      -- per-run subdirectories with metrics.json and report.html
DEFAULT_OUTPUT_DIR = "./lakebench-output"
