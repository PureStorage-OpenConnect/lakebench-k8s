"""Shared constants for lakebench."""

# Kubernetes ServiceAccount/Role/RoleBinding name for Spark driver/executor pods
SPARK_SERVICE_ACCOUNT = "lakebench-spark-runner"

# Polaris OAuth2 client_id used by the bootstrap job, Trino, and Spark.
# The matching client_secret comes from user config
# (`architecture.catalog.polaris.client_secret`); see LB-090 for why no
# default is provided.
POLARIS_CLIENT_ID = "lakebench"

# Unified output directory -- single top-level directory for all lakebench outputs.
# Contains:
#   journal/   -- session-scoped JSONL provenance logs
#   runs/      -- per-run subdirectories with metrics.json and report.html
DEFAULT_OUTPUT_DIR = "./lakebench-output"
