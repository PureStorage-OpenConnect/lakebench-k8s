"""Storage-byte counters from the Spark executors, for the deployment's Prometheus.

Each executor JVM runs the Prometheus JMX exporter agent next to Spark's JMX
metrics sink. The agent exposes the executor source (task output bytes, S3A
read bytes) on :data:`PORT`; the deployment's Prometheus scrapes the pod
(``templates/prometheus/prometheus.yaml.j2``) and the run's end reads the
series back (``observability/spark_io.py``). Nothing is added inside a timed
stage: the agent is passive and the read happens after the run.

What the counters cover and what they do not is in ``metrics/io.py``: writes
are the task output bytes; reads are the S3A client's read bytes, which is
raw Parquet and Delta, not Iceberg table reads (S3FileIO has no client
counter); streaming checkpoint and state bytes are in neither.

The agent jar comes from ``images.jmx_exporter`` by an init container, the
way the Trino pods get theirs. Only when ``observability.enabled``.
"""

from __future__ import annotations

from typing import Any

PORT = 9404
DIR = "/opt/jmx-exporter"
JAR = f"{DIR}/jmx_prometheus_javaagent.jar"
CONFIG = f"{DIR}/config.yaml"
VOLUME = "lb-jmx-exporter"

# The exporter rules take every bean with no whitelist: a whitelist on
# ``metrics:name=*executor*`` matched nothing in the probe. Prometheus keeps
# only the series the report reads (metric_relabel_configs).
_RULES = (
    'lowercaseOutputName: true\\nlowercaseOutputLabelNames: true\\nrules:\\n  - pattern: ".*"\\n'
)

# Spark keys the wiring sets (conf_keys.LAKEBENCH_OWNED_SPARK_KEYS lists them).
SPARK_CONF: dict[str, str] = {
    "spark.metrics.namespace": "lakebench",
    "spark.metrics.conf.*.sink.jmx.class": "org.apache.spark.metrics.sink.JmxSink",
    "spark.executor.metrics.fileSystemSchemes": "file,s3a,s3",
}


def agent_option() -> str:
    return f"-javaagent:{JAR}={PORT}:{CONFIG}"


def init_container(image: str, pull_policy: str) -> dict[str, Any]:
    """Copies the exporter agent into the shared emptyDir and writes its rules."""
    copy = (
        f"cp /opt/bitnami/jmx-exporter/jmx_prometheus_javaagent.jar {DIR}/ 2>/dev/null "
        f"|| cp /opt/jmx_exporter/jmx_prometheus_javaagent*.jar {JAR} 2>/dev/null "
        "|| { echo 'could not find the JMX exporter jar in the image' >&2; exit 1; }"
    )
    return {
        "name": "lb-jmx-exporter",
        "image": image,
        "imagePullPolicy": pull_policy,
        "command": ["sh", "-c", f"{copy}; printf '{_RULES}' > {CONFIG}"],
        "volumeMounts": [{"name": VOLUME, "mountPath": DIR}],
    }


def wire(
    spark_conf: dict[str, str],
    executor_pod_template: dict[str, Any],
    *,
    image: str,
    pull_policy: str,
) -> None:
    """Add the agent, its init container and the Spark keys to an executor.

    The agent option is appended to ``spark.executor.extraJavaOptions`` as it
    stands, so the G1 flags and the truststore options stay.
    """
    spark_conf.update(SPARK_CONF)
    base = spark_conf.get("spark.executor.extraJavaOptions", "")
    spark_conf["spark.executor.extraJavaOptions"] = f"{base} {agent_option()}".strip()
    spec = executor_pod_template["spec"]
    spec["volumes"] = [*spec["volumes"], {"name": VOLUME, "emptyDir": {}}]
    spec["initContainers"] = [
        *spec.get("initContainers", []),
        init_container(image, pull_policy),
    ]
    for container in spec["containers"]:
        container["volumeMounts"] = [
            *container["volumeMounts"],
            {"name": VOLUME, "mountPath": DIR},
        ]
