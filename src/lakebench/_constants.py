"""Shared constants for lakebench."""

# Kubernetes ServiceAccount/Role/RoleBinding name for Spark driver/executor pods
SPARK_SERVICE_ACCOUNT = "lakebench-spark-runner"

# Unified output directory -- single top-level directory for all lakebench outputs.
# Contains:
#   journal/   -- session-scoped JSONL provenance logs
#   runs/      -- per-run subdirectories with metrics.json and report.html
DEFAULT_OUTPUT_DIR = "./lakebench-output"
