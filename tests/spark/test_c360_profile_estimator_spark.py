"""Executed: silver profiling estimates distinct customers from a sample's
frequency profile instead of dividing by the sampling fraction (LB-144).

The old estimate at scale 10 was 14.7M customers where there are 1M.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


@pytest.fixture(scope="module")
def spark():
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.mark.parametrize("zipf", [False, True])
def test_estimate_close_to_truth_where_linear_extrapolation_is_far(spark, zipf):
    from common import sample_key_profile
    from pyspark.sql.functions import col, floor, pow, rand

    customers, rows, fraction = 40_000, 1_000_000, 0.01
    # Two fixed slices: both the seeded rand() and the seeded sample draw
    # per partition, so the input must not depend on whichever session
    # getOrCreate() hands back.
    df = spark.range(0, rows, 1, 2)
    if zipf:
        # Heavy-tailed: low ids far more frequent, every id still present.
        df = df.select(
            (floor(pow(rand(seed=7), 2.0) * customers)).cast("long").alias("customer_id")
        )
    else:
        df = df.select((col("id") % customers).alias("customer_id"))
    truth = df.select("customer_id").distinct().count()
    # fraction= must be a keyword. PySpark 4.0 reads sample(0.01, seed=11)
    # as sample(fraction=0.01) with the seed taken from the (absent) second
    # positional, so seed=11 is silently dropped and every run drew a fresh
    # random sample; the CI failure (44193 vs 40000) was one of those draws.
    sample = df.sample(fraction=fraction, seed=11)
    est, skew = sample_key_profile(sample, "customer_id", rows)
    sample_distinct = sample.select("customer_id").distinct().count()
    linear = sample_distinct / fraction
    assert linear > 5 * truth  # the LB-144 defect, reproduced
    # Seeded, the result is fixed. For the record, over 1,000 random sample
    # seeds on the uniform key the estimate ran +3.3% mean, 3.0% sd, 1.1% of
    # draws beyond 10%, so the tolerance is only safe because the seed now
    # holds. The heavy-tailed key lands 27% low at this seed; over 60 seeds
    # it ran -22% mean, 2.5% sd, worst -28.5%, so a change to the sampler or
    # slice count can push it past 0.3 without any estimator regression.
    # Linear extrapolation is more than 5x high on both.
    tol = 0.3 if zipf else 0.1
    assert abs(est - truth) / truth < tol, (est, truth)
    assert skew >= 1.0


def test_estimator_edge_cases():
    from common import estimate_distinct_from_sample

    # The full population is its own answer.
    assert estimate_distinct_from_sample(100, 40, 10, 5, 100) == 40
    assert estimate_distinct_from_sample(0, 0, 0, 0, 100) == 0
    # Never above the population row count.
    assert estimate_distinct_from_sample(10, 10, 10, 0, 100) == 55
    assert estimate_distinct_from_sample(10, 10, 10, 0, 20) == 20


def test_scale10_shape_gives_about_one_million():
    """Expected frequency profile of a 0.1% sample of 24.77M rows over 1M
    customers (Poisson, lambda 0.0248): about 24.5K distinct, 24.2K seen once,
    300 seen twice."""
    from common import estimate_distinct_from_sample

    est = estimate_distinct_from_sample(24_770, 24_490, 24_190, 300, 24_770_109)
    assert 0.9e6 < est < 1.1e6
    assert 24_490 / 0.001 > 14e6  # what the old code reported
