"""AML gold-finalize keeps 1000 jobs and stages in the driver's status store
(common.rule_stage_profile reads each detection rule's stages from it after
the rule; W3 and W17 run more than the default 100). Every other job, and
gold-finalize of every other workload, keeps the default 100."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
from tests.conftest import make_config


def _conf(cfg, job_type: JobType) -> dict[str, str]:
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    manager = SparkJobManager(cfg, k8s)
    with patch.object(SparkJobManager, "_build_env_vars", return_value=[]):
        return manager._build_manifest(job_type)["spec"]["sparkConf"]


BATCH = (JobType.BRONZE_VERIFY, JobType.SILVER_BUILD, JobType.GOLD_FINALIZE)


@pytest.mark.parametrize("schema", ["financial", "customer360"])
@pytest.mark.parametrize("job_type", BATCH, ids=[j.value for j in BATCH])
def test_retention_reaches_only_aml_gold_finalize(schema, job_type):
    cfg = make_config(architecture={"workload": {"schema": schema}})
    conf = _conf(cfg, job_type)
    want = "1000" if (schema, job_type) == ("financial", JobType.GOLD_FINALIZE) else "100"
    assert conf["spark.ui.retainedJobs"] == want
    assert conf["spark.ui.retainedStages"] == want
    assert conf["spark.ui.retainedTasks"] == "1000"
