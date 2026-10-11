"""Tests for metrics collection, storage and pipeline scoring."""

from datetime import datetime, timedelta

import pytest

from lakebench.metrics import (
    BenchmarkMetrics,
    BenchmarkRoundMeta,
    JobMetrics,
    MetricsCollector,
    MetricsStorage,
    PipelineBenchmark,
    PipelineMetrics,
    StageMetrics,
    StreamingJobMetrics,
    aggregate_benchmark_rounds,
    build_pipeline_benchmark,
)


class TestMetricsStorage:
    """Tests for metrics persistence."""

    def test_save_and_load_financial_scoring_and_detection_fields(self, tmp_path):
        """financial_scoring AND the JobMetrics detection dicts must
        survive save -> load. The report is always rendered from the disk
        reload, so a field dropped here shows the scorecard zero alerts /
        no recall even though the live run had them."""
        storage = MetricsStorage(tmp_path / "metrics")
        now = datetime.now()
        metrics = PipelineMetrics(
            run_id="aml-001",
            deployment_name="test",
            start_time=now,
            success=True,
            jobs=[
                JobMetrics(
                    job_name="lakebench-gold-finalize",
                    job_type="gold-finalize",
                    success=True,
                    alerts_by_rule={"W2_structuring": 12, "W3_round_tripping": 5},
                    rules_skipped={"W1_connected_components": "vertex-cap"},
                    rule_errors={"W7_cross_border_high_risk": "boom"},
                ),
            ],
            financial_scoring={
                "typologies": [
                    {
                        "typology_type": "micro_structuring",
                        "recall": 0.83,
                        "instance_count": 6,
                        "detection_status": "scored",
                    }
                ],
                "total_alerts": 17,
                "fp_alerts": 2,
                "fp_rate": 0.1176,
                "run_id": "aml-001",
            },
        )
        storage.save_run(metrics)
        loaded = storage.load_run("aml-001")
        assert loaded is not None
        # financial_scoring round-trips.
        assert loaded.financial_scoring is not None
        assert loaded.financial_scoring["typologies"][0]["recall"] == 0.83
        assert loaded.financial_scoring["total_alerts"] == 17
        # JobMetrics detection dicts round-trip (the HIGH finding: previously
        # serialized but dropped on load).
        job = loaded.jobs[0]
        assert job.alerts_by_rule == {"W2_structuring": 12, "W3_round_tripping": 5}
        assert job.rules_skipped == {"W1_connected_components": "vertex-cap"}
        assert job.rule_errors == {"W7_cross_border_high_risk": "boom"}

    @pytest.mark.parametrize(
        ("logs", "alerts", "skipped", "errors"),
        [
            (
                "[lb] 2026-09-22T10:00:00 - [detection] W2_structuring: alerts=1234 prior=0 elapsed=15.2s\n"
                "[lb] 2026-09-22T10:00:16 - [detection] W3_round_tripping: alerts=42 prior=42 elapsed=8.9s\n"
                "[lb] 2026-09-22T10:00:25 - [detection] W1_connected_components: alerts=17 prior=17 elapsed=120.4s\n"
                "[lb] 2026-09-22T10:02:26 - [detection] total alerts written: 1293\n",
                {"W2_structuring": 1234, "W3_round_tripping": 42, "W1_connected_components": 17},
                None,
                {},
            ),
            (
                "[lb] 2026-09-22T10:00:00 - [detection] W2_structuring: alerts=1234 prior=0 elapsed=15.2s\n"
                "[lb] 2026-09-22T10:00:16 - [detection] W7_cross_border_high_risk: alerts=0 error=AnalysisException: silver.entities not found elapsed=0.4s\n",
                {"W2_structuring": 1234, "W7_cross_border_high_risk": 0},
                None,
                {"W7_cross_border_high_risk": ["AnalysisException", "silver.entities not found"]},
            ),
            # a structural skip is a third state, never read as a 0-alert result
            (
                "[lb] 2026-09-22T10:00:00 - [detection] W2_structuring: alerts=1234 prior=0 elapsed=15.2s\n"
                "[lb] 2026-09-22T10:00:16 - [detection] W1_connected_components: skipped=vertex-cap detail=vertices=50000000 max=8000000 (raise financial.w1_max_vertices to run W1 at this scale) elapsed=0.6s\n"
                "[lb] 2026-09-22T10:00:20 - [detection] total alerts written: 1234\n",
                {"W2_structuring": 1234},
                {"W1_connected_components": "vertex-cap"},
                {},
            ),
            (
                "[lb] 2026-09-22T10:00:00 - [detection] W2_structuring: alerts=10 prior=0 elapsed=1.0s\n"
                "[lb] 2026-09-22T10:00:01 - [detection] W1_connected_components: skipped=vertex-cap detail=vertices=9000000 max=8000000 elapsed=0.2s\n"
                "[lb] 2026-09-22T10:00:02 - [detection] W7_cross_border_high_risk: alerts=0 error=AnalysisException: boom elapsed=0.4s\n",
                {"W2_structuring": 10, "W7_cross_border_high_risk": 0},
                {"W1_connected_components": "vertex-cap"},
                {"W7_cross_border_high_risk": ["AnalysisException"]},
            ),
            # a JVM log line glued to the end of a detection line still counts
            (
                "[detection] W7_cross_border_high_risk: alerts=509768 prior=0 elapsed=12.0s\n"
                "[detection] W8_dormant_reactivation: alerts=58562 prior=0 elapsed=10.5s"
                "26/10/07 07:40:00 INFO BlockManager: Removing RDD 1150\n"
                "[detection] W1_connected_components: skipped=giant-component "
                "detail=x elapsed=0.4s26/10/07 07:40:01 INFO DAGScheduler: Job 9 finished\n",
                {"W7_cross_border_high_risk": 509768, "W8_dormant_reactivation": 58562},
                {"W1_connected_components": "giant-component"},
                None,
            ),
        ],
    )
    def test_parse_detection_lines(self, logs, alerts, skipped, errors):
        metrics = MetricsCollector().parse_driver_logs(logs, "gold-finalize")
        assert metrics.alerts_by_rule == alerts
        if skipped is not None:
            assert metrics.rules_skipped == skipped
        if errors is not None:
            assert set(metrics.rule_errors) == set(errors)
            for rule, needles in errors.items():
                for n in needles:
                    assert n in metrics.rule_errors[rule]
        if "W8_dormant_reactivation" in alerts:
            assert metrics.rule_elapsed_s["W8_dormant_reactivation"] == 10.5


class TestStreamingLogParsing:
    """Tests for streaming driver log parsing."""

    def test_parse_streaming_logs_label_families(self):
        """Labels under the accepted prefix families (silver_, dim_merge_,
        kyc_, data_clock_) land in extra_metrics with no code change; Spark's
        own lines, multi-label lines (a garbage value would be trusted),
        uppercase and hyphenated keys never do."""
        c = MetricsCollector()
        logs = """
[lb] 2026-09-28T15:54:53.883921 - silver_stream_scale_cap: measured_up_to_scale_10
[lb] 2026-09-28T15:54:53.883921 - silver_stream_scale_admission: ok
[lb] 2026-09-28T15:55:13.041566 - [batch 0] 189335 txns
[lb] 2026-09-28T15:56:14.522310 - silver_statements_parity_mode: strict_monotone
[lb] 2026-09-28T15:56:14.522514 - silver_statements_late_arrivals_this_batch: 0
[lb] 2026-09-28T15:56:14.522514 - silver_statements_batch_id: 0
[lb] 2026-09-28T15:55:53.790921 - dim_merge_elapsed_ms_entities: 12345
[lb] 2026-09-28T15:55:53.790921 - dim_merge_elapsed_ms_accounts: 6789
[lb] 2026-09-28T15:55:25.028644 - kyc_refreshed_at: 1717029325
[lb] 2026-09-28T15:55:25.028644 - kyc_refresh_kind: initial
[lb] 2026-09-28T15:00:00.000000 - silver_frobnicate_rows: 42
[lb] 2026-09-28T15:00:00.000000 - silver_stream_backpressure_ms: 137
[lb] 2026-09-28T15:00:00.000000 - dim_merge_conflicts: 3
[lb] 2026-09-28T15:00:00.000000 - kyc_pending_refreshes: 11
[lb] 2026-09-28T15:00:00.000000 - data_clock_skew_ms: -250
26/09/28 15:00:00 INFO SparkContext: unrelated_something: should_not_match
[lb] 2026-09-28T15:00:00 - silver_TxScale: 5
[lb] 2026-09-28T15:00:00 - dim_merge_ok-count: 3
[lb] 2026-09-28T15:00:00 - silver_valid_lowercase: 7
"""
        e = c.parse_streaming_logs(logs, "silver-stream").extra_metrics
        want = {
            "silver_stream_scale_cap": "measured_up_to_scale_10",
            "silver_stream_scale_admission": "ok",
            "silver_statements_parity_mode": "strict_monotone",
            "silver_statements_late_arrivals_this_batch": "0",
            "silver_statements_batch_id": "0",
            "dim_merge_elapsed_ms_entities": "12345",
            "dim_merge_elapsed_ms_accounts": "6789",
            "kyc_refreshed_at": "1717029325",
            "kyc_refresh_kind": "initial",
            "silver_frobnicate_rows": "42",
            "silver_stream_backpressure_ms": "137",
            "dim_merge_conflicts": "3",
            "kyc_pending_refreshes": "11",
            "data_clock_skew_ms": "-250",
            "silver_valid_lowercase": "7",
        }
        assert {k: e.get(k) for k in want} == want
        for refused in ("unrelated_something", "silver_TxScale", "dim_merge_ok-count"):
            assert refused not in e
        multi = c.parse_streaming_logs(
            "[lb] 2026-09-28T15:00:00 - silver_statements_parity_mode: "
            "strict_monotone silver_statements_late_arrivals_this_batch: 0\n",
            "silver-stream",
        ).extra_metrics
        assert "silver_statements_parity_mode" not in multi
        assert "silver_statements_late_arrivals_this_batch" not in multi


class TestStreamingTimingAndFreshness:
    """Tests for new streaming timing and freshness log patterns."""

    @pytest.mark.parametrize(
        ("logs", "batches", "rows", "freshness"),
        [
            # max(30, 60) = 60, not the average 45
            (
                """\
[lb] 2026-02-01T12:00:01.000Z - Cycle 1: aggregating 150,000 Silver records
[lb] 2026-02-01T12:00:07.000Z - Cycle 1: data freshness 30s
[lb] 2026-02-01T12:00:08.000Z - Cycle 1: refreshed ice.gold.customer_executive_dashboard in 8.2s (30 KPI records)
[lb] 2026-02-01T12:05:01.000Z - Cycle 2: aggregating 300,000 Silver records
[lb] 2026-02-01T12:05:11.000Z - Cycle 2: data freshness 60s
[lb] 2026-02-01T12:05:14.000Z - Cycle 2: refreshed ice.gold.customer_executive_dashboard in 13.5s (30 KPI records)
""",
                2,
                450_000,
                60.0,
            ),
            # max(10, 50, 20) = 50: neither the average 26.67 nor the last cycle
            (
                """\
[lb] 2026-02-01T12:00:01.000Z - Cycle 1: aggregating 100,000 Silver records
[lb] 2026-02-01T12:00:07.000Z - Cycle 1: data freshness 10s
[lb] 2026-02-01T12:00:08.000Z - Cycle 1: refreshed tbl in 7.0s (30 KPI records)
[lb] 2026-02-01T12:05:01.000Z - Cycle 2: aggregating 100,000 Silver records
[lb] 2026-02-01T12:05:11.000Z - Cycle 2: data freshness 50s
[lb] 2026-02-01T12:05:14.000Z - Cycle 2: refreshed tbl in 13.0s (30 KPI records)
[lb] 2026-02-01T12:10:01.000Z - Cycle 3: aggregating 100,000 Silver records
[lb] 2026-02-01T12:10:05.000Z - Cycle 3: data freshness 20s
[lb] 2026-02-01T12:10:09.000Z - Cycle 3: refreshed tbl in 8.0s (30 KPI records)
""",
                3,
                300_000,
                50.0,
            ),
        ],
    )
    def test_gold_freshness_is_the_worst_cycle(self, logs, batches, rows, freshness):
        """Gold 'data freshness Xs' populates freshness_seconds as the worst case."""
        metrics = MetricsCollector().parse_streaming_logs(logs, "gold-refresh")
        assert metrics.total_batches == batches
        assert metrics.total_rows_processed == rows
        assert metrics.freshness_seconds == pytest.approx(freshness)


def test_gold_size_leaves_out_the_incremental_detection_state():
    """Continuous AML gold keeps its detection state between passes under
    the gold bucket; the published gold size counts the gold tables only."""
    from lakebench.metrics.collector import GOLD_INCREMENTAL_PREFIX
    from lakebench.s3 import S3Client
    from tests.fixtures.recording_k8s import recording

    with recording("ns") as rec:
        rec.add_bucket("b", {"pacs008/p.parquet": b"x" * 7})
        rec.add_bucket("s", {"silver/t/d.parquet": b"x" * 5})
        rec.add_bucket(
            "g",
            {
                "gold/alerts/data/a.parquet": b"x" * 3,
                f"{GOLD_INCREMENTAL_PREFIX}app/W4/pairs-1/p.parquet": b"y" * 1000,
            },
        )
        s3 = S3Client(endpoint="http://10.0.1.50", access_key="a", secret_key="b")
        c = MetricsCollector()
        c.start_run("r", "d", {})
        c.record_actual_sizes(s3, "b", "s", "g")
        assert c.current_run.gold_size_gb * 1024**3 == 3


class TestPipelineBenchmark:
    """Tests for the pipeline-level benchmark dataclass."""

    def test_throughput_uses_ttv_not_summed_time(self):
        """Throughput divides by wall-clock TTV, not sum of stage durations."""
        now = datetime.now()
        # Two stages that overlap: both run from t=0 to t=100
        # Summed elapsed = 200s, but TTV = 100s
        stages = [
            StageMetrics(
                stage_name="bronze",
                stage_type="batch",
                engine="spark",
                start_time=now,
                end_time=now + timedelta(seconds=100),
                elapsed_seconds=100.0,
                success=True,
                input_size_gb=10.0,
                output_size_gb=10.0,
            ),
            StageMetrics(
                stage_name="silver",
                stage_type="batch",
                engine="spark",
                start_time=now,
                end_time=now + timedelta(seconds=100),
                elapsed_seconds=100.0,
                success=True,
                input_size_gb=10.0,
                output_size_gb=3.5,
            ),
        ]
        pb = PipelineBenchmark(
            run_id="ttv-test",
            deployment_name="test",
            pipeline_mode="batch",
            start_time=now,
            stages=stages,
            success=True,
        )
        pb.compute_aggregates()

        assert pb.total_elapsed_seconds == 200.0  # summed
        assert pb.time_to_value_seconds == 100.0  # wall-clock
        # Throughput uses TTV: 20 GB / 100s = 0.2, not 20/200 = 0.1
        assert pb.pipeline_throughput_gb_per_second == pytest.approx(0.2)


class TestBuildPipelineBenchmark:
    """Tests for the PipelineMetrics → PipelineBenchmark conversion."""

    def _make_run(self) -> PipelineMetrics:
        now = datetime.now()
        return PipelineMetrics(
            run_id="build-test",
            deployment_name="test",
            start_time=now,
            end_time=now + timedelta(seconds=300),
            total_elapsed_seconds=300.0,
            success=True,
            bronze_size_gb=10.0,
            silver_size_gb=3.5,
            gold_size_gb=0.01,
            jobs=[
                JobMetrics(
                    job_name="lakebench-bronze-verify",
                    job_type="bronze-verify",
                    start_time=now,
                    end_time=now + timedelta(seconds=50),
                    elapsed_seconds=50.0,
                    success=True,
                    input_size_gb=10.0,
                    input_rows=5_000_000,
                    output_rows=5_000_000,
                    executor_count=4,
                    executor_cores=2,
                    executor_memory_gb=4.0,
                ),
                JobMetrics(
                    job_name="lakebench-silver-build",
                    job_type="silver-build",
                    start_time=now + timedelta(seconds=50),
                    end_time=now + timedelta(seconds=200),
                    elapsed_seconds=150.0,
                    success=True,
                    input_size_gb=10.0,
                    input_rows=5_000_000,
                    output_rows=4_900_000,
                    executor_count=12,
                    executor_cores=4,
                    executor_memory_gb=48.0,
                ),
                JobMetrics(
                    job_name="lakebench-gold-finalize",
                    job_type="gold-finalize",
                    start_time=now + timedelta(seconds=200),
                    end_time=now + timedelta(seconds=260),
                    elapsed_seconds=60.0,
                    success=True,
                    input_size_gb=3.5,
                    input_rows=4_900_000,
                    output_rows=92,
                    executor_count=4,
                    executor_cores=4,
                    executor_memory_gb=32.0,
                ),
            ],
            benchmark=BenchmarkMetrics(
                mode="power",
                cache="hot",
                scale=100,
                qph=820.0,
                total_seconds=40.0,
                queries=[{"name": "Q1", "success": True}],
            ),
        )

    def test_s3_size_fallback(self):
        """output_size_gb uses S3 layer size when job reports 0."""
        run = self._make_run()
        # Zero out output_size_gb on jobs (simulating no driver log reporting)
        for j in run.jobs:
            j.output_size_gb = 0.0

        pb = build_pipeline_benchmark(run)

        assert pb.stages[0].output_size_gb == pytest.approx(10.0)  # bronze_size_gb
        assert pb.stages[1].output_size_gb == pytest.approx(3.5)  # silver_size_gb
        assert pb.stages[2].output_size_gb == pytest.approx(0.01)  # gold_size_gb

    def test_empty_fleet_dict_does_not_append_zero_stage(self):
        """A fleet with pods_reported=0 (all pods failed to emit) is still
        a truthy dict; the datagen stage must NOT be appended, otherwise
        core-hour aggregation double-counts a zero-length stage."""
        run = self._make_run()
        empty_fleet = {
            "schema": "customer360",
            "pods_expected": 8,
            "pods_reported": 0,
            "pods_missing": 8,
            "data_quality": "empty",
            "total_bytes_written": 0,
            "total_files_written": 0,
            "total_rows_written": 0,
            "aggregate_mbps": 0.0,
            "wall_elapsed_max_s": 0.0,
            "wall_elapsed_min_s": 0.0,
            "cores_total": 0.0,
            "cpu_seconds_total": 0.0,
            "cpu_hr_per_tb": None,
            "phase_pct": {"build_batch": 0, "encode_parquet": 0, "s3_put": 0},
            "phase_p50_s": {"build_batch": 0, "encode_parquet": 0, "s3_put": 0},
            "phase_p95_s": {"build_batch": 0, "encode_parquet": 0, "s3_put": 0},
            "worst_pod_elapsed_s": 0.0,
            "best_pod_elapsed_s": 0.0,
            "per_pod": [],
        }
        pb = build_pipeline_benchmark(run, datagen_elapsed=0.0, datagen_fleet=empty_fleet)
        stage_names = [s.stage_name for s in pb.stages]
        assert "datagen" not in stage_names

    def test_fleet_enriches_datagen_stage_with_executors(self):
        run = self._make_run()
        fleet = {
            "schema": "customer360",
            "pods_expected": 4,
            "pods_reported": 4,
            "pods_missing": 0,
            "data_quality": "complete",
            "total_bytes_written": 100_000_000_000,
            "total_files_written": 200,
            "total_rows_written": 40_000_000,
            "aggregate_mbps": 833.0,
            "wall_elapsed_max_s": 120.0,
            "wall_elapsed_min_s": 118.0,
            "cores_total": 32.0,  # 4 pods * 8 cores
            "cpu_seconds_total": 3840.0,
            "cpu_hr_per_tb": 10.66,
            "phase_pct": {"build_batch": 55, "encode_parquet": 40, "s3_put": 5},
            "phase_p50_s": {"build_batch": 200, "encode_parquet": 150, "s3_put": 15},
            "phase_p95_s": {"build_batch": 220, "encode_parquet": 165, "s3_put": 20},
            "worst_pod_elapsed_s": 120.0,
            "best_pod_elapsed_s": 118.0,
            "per_pod": [],
        }
        pb = build_pipeline_benchmark(run, datagen_elapsed=120.0, datagen_fleet=fleet)
        dg = next(s for s in pb.stages if s.stage_name == "datagen")
        assert dg.executor_count == 4
        assert dg.executor_cores == 8  # 32 / 4


class TestSustainedPipelineScoring:
    """Tests for sustained (streaming) pipeline scoring."""

    def _make_streaming_run(self) -> PipelineMetrics:
        """Helper: build a 3-stage streaming PipelineMetrics."""
        now = datetime.now()
        return PipelineMetrics(
            run_id="cont-test",
            deployment_name="test",
            start_time=now,
            success=True,
            bronze_size_gb=10.0,
            silver_size_gb=5.0,
            gold_size_gb=0.5,
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=100,
                    total_rows_processed=500_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=277.8,
                    micro_batch_duration_ms=200.0,
                    freshness_seconds=5.0,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-silver-stream",
                    job_type="silver-stream",
                    total_batches=95,
                    total_rows_processed=480_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=266.7,
                    micro_batch_duration_ms=350.0,
                    freshness_seconds=8.5,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-gold-refresh",
                    job_type="gold-refresh",
                    total_batches=90,
                    total_rows_processed=450_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=250.0,
                    micro_batch_duration_ms=500.0,
                    freshness_seconds=15.0,
                    batch_size=5000,
                ),
            ],
        )

    def test_sustained_scores_computed(self):
        """Sustained scores are computed from streaming stage data."""
        run = self._make_streaming_run()
        pb = build_pipeline_benchmark(run)

        assert pb.pipeline_mode == "sustained"
        assert len(pb.stages) == 3

        # data_freshness_seconds = max freshness across stages (worst case)
        assert pb.data_freshness_seconds == 15.0

        # sustained_throughput_rps = bronze input_rows / run_duration
        # bronze has 500_000 rows, run_duration = max(1800, 1800, 1800) = 1800
        assert pb.sustained_throughput_rps == pytest.approx(500_000 / 1800.0, rel=1e-2)

        # stage_latency_profile = [bronze_ms, silver_ms, gold_ms]
        assert pb.stage_latency_profile == [200.0, 350.0, 500.0]

        # total_rows_processed = sum of input_rows
        assert pb.total_rows_processed == 500_000 + 480_000 + 450_000

        # total_elapsed_seconds: sustained mode uses wall-clock (max), not sum
        assert pb.total_elapsed_seconds == pytest.approx(1800.0)

    def test_sustained_batch_scores_zero(self):
        """Batch-only scores remain zero for a sustained pipeline."""
        run = self._make_streaming_run()
        pb = build_pipeline_benchmark(run)

        # time_to_value is batch-only (based on start/end wall-clock span)
        assert pb.time_to_value_seconds == 0.0

    def test_batch_sustained_scores_zero(self):
        """Sustained scores remain zero for a batch pipeline."""
        now = datetime.now()
        run = PipelineMetrics(
            run_id="batch-only",
            deployment_name="test",
            start_time=now,
            success=True,
            bronze_size_gb=10.0,
            silver_size_gb=5.0,
            gold_size_gb=0.5,
            jobs=[
                JobMetrics(
                    job_name="lakebench-bronze-verify",
                    job_type="bronze-verify",
                    elapsed_seconds=50.0,
                    success=True,
                    input_size_gb=10.0,
                    output_size_gb=10.0,
                    start_time=now,
                    end_time=now + timedelta(seconds=50),
                ),
            ],
        )
        pb = build_pipeline_benchmark(run)

        assert pb.pipeline_mode == "batch"
        assert pb.data_freshness_seconds is None
        assert pb.sustained_throughput_rps == 0.0
        assert pb.stage_latency_profile == []
        assert pb.total_rows_processed == 0
        # Batch scores are populated
        assert pb.time_to_value_seconds > 0


class TestAggregateBenchmarkRounds:
    """Tests for the aggregate_benchmark_rounds function."""

    def _make_round(
        self,
        round_index: int,
        qph: float,
        total_seconds: float,
        q1_elapsed: float = 5.0,
        q2_elapsed: float = 3.0,
        freshness: float = 0.0,
    ) -> BenchmarkMetrics:
        meta = BenchmarkRoundMeta(
            round_index=round_index,
            timestamp=datetime.now(),
            gold_event_age_seconds=freshness,
        )
        return BenchmarkMetrics(
            mode="power",
            cache="hot",
            scale=10,
            qph=qph,
            total_seconds=total_seconds,
            queries=[
                {
                    "name": "q1",
                    "display_name": "Full Scan",
                    "class": "scan",
                    "elapsed_seconds": q1_elapsed,
                    "rows_returned": 100,
                    "success": True,
                },
                {
                    "name": "q2",
                    "display_name": "Date Filter",
                    "class": "filter",
                    "elapsed_seconds": q2_elapsed,
                    "rows_returned": 50,
                    "success": True,
                },
            ],
            round_meta=meta,
        )

    def test_median_of_three(self):
        rounds = [
            self._make_round(1, qph=200.0, total_seconds=30.0, q1_elapsed=5.0),
            self._make_round(2, qph=300.0, total_seconds=25.0, q1_elapsed=4.0),
            self._make_round(3, qph=250.0, total_seconds=28.0, q1_elapsed=4.5),
        ]
        result = aggregate_benchmark_rounds(rounds)
        # Median of [200, 300, 250] = 250
        assert result.qph == 250.0
        # Median of [30, 25, 28] = 28
        assert result.total_seconds == 28.0
        # q1 elapsed median of [5.0, 4.0, 4.5] = 4.5
        q1 = next(q for q in result.queries if q["name"] == "q1")
        assert q1["elapsed_seconds"] == 4.5

    def test_even_count_median_is_the_mean_of_the_middle_two(self):
        rounds = [
            self._make_round(1, qph=200.0, total_seconds=30.0),
            self._make_round(2, qph=300.0, total_seconds=20.0),
        ]
        assert aggregate_benchmark_rounds(rounds).qph == 250.0


class TestPipelineBenchmarkRounds:
    """Tests for PipelineBenchmark in-stream benchmark round integration."""

    def _make_streaming_run_with_rounds(self) -> PipelineMetrics:
        now = datetime.now()
        rounds = []
        for i in range(3):
            meta = BenchmarkRoundMeta(
                round_index=i + 1,
                timestamp=now + timedelta(seconds=300 * (i + 1)),
                gold_event_age_seconds=30.0 + i * 10,  # 30, 40, 50
            )
            rounds.append(
                BenchmarkMetrics(
                    mode="power",
                    cache="hot",
                    scale=10,
                    qph=200.0 + i * 25,  # 200, 225, 250
                    total_seconds=30.0,
                    round_meta=meta,
                )
            )

        return PipelineMetrics(
            run_id="rounds-test",
            deployment_name="test",
            start_time=now,
            success=True,
            bronze_size_gb=10.0,
            silver_size_gb=5.0,
            gold_size_gb=0.5,
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=100,
                    total_rows_processed=500_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=277.8,
                    micro_batch_duration_ms=200.0,
                    freshness_seconds=5.0,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-gold-refresh",
                    job_type="gold-refresh",
                    total_batches=6,
                    total_rows_processed=450_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    freshness_seconds=15.0,
                ),
            ],
            benchmark=BenchmarkMetrics(
                mode="power",
                cache="hot",
                scale=10,
                qph=260.0,
                total_seconds=25.0,
            ),
            benchmark_rounds=rounds,
        )

    def test_query_time_event_age_computed(self):
        run = self._make_streaming_run_with_rounds()
        pb = build_pipeline_benchmark(run)
        # Freshness values: 30, 40, 50 -- median = 40
        assert pb.query_time_event_age_seconds == pytest.approx(40.0)


class TestSustainedScoringEdgeCases:
    """Edge-case tests for sustained (streaming) pipeline scoring."""

    def _make_streaming_run(
        self,
        streaming: list[StreamingJobMetrics] | None = None,
        bronze_size_gb: float = 10.0,
        silver_size_gb: float = 5.0,
        gold_size_gb: float = 0.5,
    ) -> PipelineMetrics:
        """Helper: build a streaming PipelineMetrics with customizable stages."""
        now = datetime.now()
        if streaming is None:
            streaming = [
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=100,
                    total_rows_processed=500_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=277.8,
                    micro_batch_duration_ms=200.0,
                    freshness_seconds=5.0,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-silver-stream",
                    job_type="silver-stream",
                    total_batches=95,
                    total_rows_processed=480_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=266.7,
                    micro_batch_duration_ms=350.0,
                    freshness_seconds=8.5,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-gold-refresh",
                    job_type="gold-refresh",
                    total_batches=90,
                    total_rows_processed=450_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=250.0,
                    micro_batch_duration_ms=500.0,
                    freshness_seconds=15.0,
                    batch_size=5000,
                ),
            ]
        return PipelineMetrics(
            run_id="edge-test",
            deployment_name="test",
            start_time=now,
            success=True,
            bronze_size_gb=bronze_size_gb,
            silver_size_gb=silver_size_gb,
            gold_size_gb=gold_size_gb,
            streaming=streaming,
        )

    def test_sustained_zero_batch_freshness(self):
        """All stages with freshness_seconds=0.0 produce unmeasurable freshness."""
        run = self._make_streaming_run(
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=50,
                    total_rows_processed=200_000,
                    elapsed_seconds=600.0,
                    success=True,
                    throughput_rps=333.3,
                    micro_batch_duration_ms=150.0,
                    freshness_seconds=0.0,
                    batch_size=4000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-silver-stream",
                    job_type="silver-stream",
                    total_batches=48,
                    total_rows_processed=190_000,
                    elapsed_seconds=600.0,
                    success=True,
                    throughput_rps=316.7,
                    micro_batch_duration_ms=200.0,
                    freshness_seconds=0.0,
                    batch_size=4000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-gold-refresh",
                    job_type="gold-refresh",
                    total_batches=45,
                    total_rows_processed=180_000,
                    elapsed_seconds=600.0,
                    success=True,
                    throughput_rps=300.0,
                    micro_batch_duration_ms=250.0,
                    freshness_seconds=0.0,
                    batch_size=4000,
                ),
            ],
        )
        pb = build_pipeline_benchmark(run)

        assert pb.pipeline_mode == "sustained"
        # freshness_seconds=0.0 is falsy, so stages get freshness_seconds=None.
        # _compute_sustained_scores filters for > 0, finds nothing --
        # data_freshness_seconds stays None (unmeasurable).
        assert pb.data_freshness_seconds is None
        # All stages should have freshness_seconds=None (0.0 converted to None)
        for stage in pb.stages:
            if stage.stage_type == "streaming":
                assert stage.freshness_seconds is None

    def test_sustained_saturation_detection(self):
        """Pipeline is saturated when ingest_ratio < 0.95."""
        run = self._make_streaming_run(
            streaming=[
                StreamingJobMetrics(
                    job_name="lakebench-bronze-ingest",
                    job_type="bronze-ingest",
                    total_batches=80,
                    total_rows_processed=400_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=222.2,
                    micro_batch_duration_ms=200.0,
                    freshness_seconds=10.0,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-silver-stream",
                    job_type="silver-stream",
                    total_batches=75,
                    total_rows_processed=380_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=211.1,
                    micro_batch_duration_ms=350.0,
                    freshness_seconds=12.0,
                    batch_size=5000,
                ),
                StreamingJobMetrics(
                    job_name="lakebench-gold-refresh",
                    job_type="gold-refresh",
                    total_batches=70,
                    total_rows_processed=350_000,
                    elapsed_seconds=1800.0,
                    success=True,
                    throughput_rps=194.4,
                    micro_batch_duration_ms=500.0,
                    freshness_seconds=20.0,
                    batch_size=5000,
                ),
            ],
        )
        # Bronze processed 400,000 rows but datagen produced 500,000
        pb = build_pipeline_benchmark(run, datagen_output_rows=500_000)

        # ingest_ratio = 400_000 / 500_000 = 0.8 < 0.95 -> saturated
        assert pb.pipeline_saturated is True
        assert pb.ingest_ratio == pytest.approx(0.8, rel=1e-2)

    def _make_sustained_pb(self, qph_values):
        """Create a PipelineBenchmark with benchmark rounds at given QpH values."""
        now = datetime.now()
        rounds = []
        for i, qph in enumerate(qph_values):
            rounds.append(
                BenchmarkMetrics(
                    mode="power",
                    cache="hot",
                    scale=10,
                    qph=qph,
                    total_seconds=30.0,
                    round_meta=BenchmarkRoundMeta(
                        round_index=i + 1,
                        timestamp=now + timedelta(minutes=i * 10),
                    ),
                )
            )
        stage = StageMetrics(
            stage_name="bronze-ingest",
            stage_type="streaming",
            engine="spark",
            elapsed_seconds=3600.0,
            success=True,
            input_rows=1_000_000,
            freshness_seconds=5.0,
            latency_ms=100.0,
        )
        stage.start_time = now
        stage.end_time = now + timedelta(hours=1)
        pb = PipelineBenchmark(
            run_id="test-deg",
            deployment_name="test",
            pipeline_mode="sustained",
            start_time=now,
            stages=[stage],
            benchmark_rounds=rounds,
            config_snapshot={"datagen_output_rows": 1_000_000},
        )
        pb.compute_aggregates()
        return pb

    def test_degradation_with_4_rounds(self):
        """4 rounds: first half median 200, second half median 100 = 50% degradation."""
        pb = self._make_sustained_pb([200, 200, 100, 100])
        assert pb.qph_degradation_pct is not None
        assert pb.qph_degradation_pct == pytest.approx(50.0)


def test_stored_record_carries_no_address_url_in_error_text():
    """An engine error quoting the object store's URL (DuckDB "HTTP GET to
    'http://<ip>:80/...'", 2026-10-07) is stored with the snapshot's
    endpoint hash in place of the address."""
    from datetime import datetime, timezone

    from lakebench.metrics import PipelineMetrics

    m = PipelineMetrics(
        run_id="scrub-001",
        deployment_name="t",
        start_time=datetime(2026, 10, 7, tzinfo=timezone.utc),
        success=False,
        config_snapshot={"s3": {"endpoint": "s3-endpoint-0123456789abcdef"}},
        failure_reasons=[
            "IOException: IO Error: Could not connect to server error for HTTP GET to "
            "'http://10.0.1.50:80/lb-silver/silver/x.parquet'"
        ],
    )
    d = m.to_dict()
    text = str(d)
    assert "10.0.1.50" not in text
    assert "http://s3-endpoint-0123456789abcdef/lb-silver/silver/x.parquet" in text


@pytest.mark.parametrize(
    ("buckets", "objects", "gold_gb"),
    [
        # Every listing worked: the count is the objects in all three.
        ({"b": 2, "s": 1, "g": 1}, 4, 1 / 1024**3),
        # The gold bucket is missing: its size and the total are unknown,
        # never 0.
        ({"b": 2, "s": 1}, None, None),
    ],
)
def test_bucket_listing_records_unknown_not_zero(buckets, objects, gold_gb):
    """A layer that could not be listed records None, and so does the run's
    object count, so the record never shows a measured-looking 0."""
    from lakebench.s3 import S3Client
    from tests.fixtures.recording_k8s import recording

    with recording("ns") as rec:
        for name, n in buckets.items():
            rec.add_bucket(name, {f"d/{i}.parquet": b"x" for i in range(n)})
        s3 = S3Client(endpoint="http://10.0.1.50", access_key="a", secret_key="b")
        c = MetricsCollector()
        c.start_run("r", "d", {})
        assert c.record_actual_sizes(s3, "b", "s", "g") == objects
        assert c.current_run.gold_size_gb == (None if gold_gb is None else pytest.approx(gold_gb))
        assert c.current_run.to_dict()["gold_size_gb"] == c.current_run.gold_size_gb
