"""Where each config field is read (the schema walk).

``READERS`` maps every leaf field of ``LakebenchConfig`` (dotted, as the
model stores it) to ``"module:qualname"``, a function that reads it.
``tests/test_schema_walk.py`` checks each entry: the function exists, it
accesses the field through its parent (``<...>.scratch.enabled``, an alias
assigned from it, or ``self`` inside the owning model), and a function in
``config/schema.py`` is a validator or is called from outside that file. A
field with no entry, or whose entry stops reading it, fails the walk, so a
setting nothing honours cannot be added silently.

Recording a value (``metrics/collector.py``, ``metrics/experiment.py``,
``metrics/fingerprint_inputs.py``) is not reading it.
"""

from __future__ import annotations

READERS: dict[str, str] = {
    "name": "lakebench.deploy.engine:DeploymentEngine._build_context",
    "recipe": "lakebench.config.schema:LakebenchConfig.apply_recipe_defaults",
    "images.datagen": "lakebench.deploy.datagen:DatagenDeployer._build_datagen_context",
    "images.spark": "lakebench.deploy.engine:DeploymentEngine._get_spark_major_minor",
    "images.postgres": "lakebench.deploy.engine:DeploymentEngine._build_context",
    "images.polaris": "lakebench.deploy.engine:DeploymentEngine._build_context",
    "images.polaris_admin_tool": "lakebench.deploy.engine:DeploymentEngine._build_context",
    "images.unity": "lakebench.deploy.engine:DeploymentEngine._build_context",
    "images.trino": "lakebench.deploy.engine:DeploymentEngine._build_context",
    "images.duckdb": "lakebench.deploy.engine:DeploymentEngine._build_context",
    "images.jmx_exporter": "lakebench.deploy.engine:DeploymentEngine._build_context",
    "images.pull_policy": "lakebench.deploy.engine:DeploymentEngine._build_context",
    "platform.kubernetes.context": "lakebench.deploy.destroy:_classify_buckets",
    "platform.kubernetes.namespace": "lakebench.config.schema:LakebenchConfig.get_namespace",
    "platform.kubernetes.create_namespace": "lakebench.deploy.destroy:destroy_all",
    "platform.deps.maven_repository": "lakebench.deps.request:_deps_key",
    "platform.deps.pypi_index": "lakebench.deps.request:_deps_key",
    "platform.deps.duckdb_extension_repository": "lakebench.deps.request:_deps_key",
    "platform.deps.storage_class": (
        "lakebench.deploy.deps:DependencyServerDeployer._check_storage_class"
    ),
    "platform.storage.s3.endpoint": (
        "lakebench.deploy.datagen:DatagenDeployer._clear_bronze_prefix_if_fresh"
    ),
    "platform.storage.s3.region": (
        "lakebench.deploy.datagen:DatagenDeployer._clear_bronze_prefix_if_fresh"
    ),
    "platform.storage.s3.path_style": (
        "lakebench.deploy.datagen:DatagenDeployer._clear_bronze_prefix_if_fresh"
    ),
    "platform.storage.s3.access_key": (
        "lakebench.deploy.datagen:DatagenDeployer._clear_bronze_prefix_if_fresh"
    ),
    "platform.storage.s3.secret_key": (
        "lakebench.deploy.datagen:DatagenDeployer._clear_bronze_prefix_if_fresh"
    ),
    "platform.storage.s3.ca_cert": (
        "lakebench.deploy.datagen:DatagenDeployer._clear_bronze_prefix_if_fresh"
    ),
    "platform.storage.s3.verify_ssl": (
        "lakebench.deploy.datagen:DatagenDeployer._clear_bronze_prefix_if_fresh"
    ),
    "platform.storage.s3.buckets.bronze": (
        "lakebench.deploy.datagen:DatagenDeployer._clear_bronze_prefix_if_fresh"
    ),
    "platform.storage.s3.buckets.silver": "lakebench.deploy.destroy:_classify_buckets",
    "platform.storage.s3.buckets.gold": "lakebench.deploy.destroy:_classify_buckets",
    "platform.storage.s3.create_buckets": (
        "lakebench.deploy.datagen:DatagenDeployer._clear_bronze_prefix_if_fresh"
    ),
    "platform.storage.scratch.enabled": (
        "lakebench.deploy.engine:DeploymentEngine._deploy_scratch_storageclass"
    ),
    "platform.storage.scratch.storage_class": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "platform.storage.scratch.provisioner": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "platform.storage.scratch.parameters": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "platform.compute.spark.operator.install": (
        "lakebench.deploy.engine:DeploymentEngine._deploy_spark_operator"
    ),
    "platform.compute.spark.operator.namespace": "lakebench.deploy.destroy:destroy_all",
    "platform.compute.spark.operator.version": "lakebench.deploy.destroy:destroy_all",
    "platform.compute.spark.bronze_executors": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_manifest"
    ),
    "platform.compute.spark.silver_executors": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_manifest"
    ),
    "platform.compute.spark.gold_executors": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_manifest"
    ),
    "platform.compute.spark.bronze_ingest_executors": (
        "lakebench.modules.pipeline_engines.spark.job:streaming_request_under_budget"
    ),
    "platform.compute.spark.silver_stream_executors": (
        "lakebench.modules.pipeline_engines.spark.job:streaming_request_under_budget"
    ),
    "platform.compute.spark.gold_refresh_executors": (
        "lakebench.modules.pipeline_engines.spark.job:streaming_request_under_budget"
    ),
    "platform.compute.spark.driver_memory": (
        "lakebench.modules.pipeline_engines.spark.job:streaming_request_under_budget"
    ),
    "platform.compute.spark.driver_cores": (
        "lakebench.modules.pipeline_engines.spark.job:_streaming_concurrent_budget"
    ),
    "platform.compute.postgres.storage": "lakebench.deploy.engine:DeploymentEngine._build_context",
    "platform.compute.postgres.storage_class": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.catalog.type": "lakebench.deploy.destroy:destroy_all",
    "architecture.catalog.hive.operator.install": (
        "lakebench.modules.catalogs.hive.deployer:HiveDeployer.deploy"
    ),
    "architecture.catalog.hive.operator.namespace": (
        "lakebench.modules.catalogs.hive.deployer:HiveDeployer._install_stackable_operators"
    ),
    "architecture.catalog.hive.operator.version": (
        "lakebench.modules.catalogs.hive.deployer:HiveDeployer._install_stackable_operators"
    ),
    "architecture.catalog.hive.resources.cpu_min": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.catalog.hive.resources.cpu_max": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.catalog.hive.resources.memory": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.catalog.polaris.port": "lakebench.deploy.engine:DeploymentEngine._build_context",
    "architecture.catalog.polaris.client_secret": (
        "lakebench.config.schema:require_polaris_client_secret"
    ),
    "architecture.catalog.polaris.resources.cpu": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.catalog.polaris.resources.memory": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.catalog.unity.spark_connector_version": (
        "lakebench.deps.request:jar_coordinates"
    ),
    "architecture.catalog.unity.port": "lakebench.deploy.engine:DeploymentEngine._build_context",
    "architecture.catalog.unity.resources.cpu": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.catalog.unity.resources.memory": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.table_format.type": "lakebench.deploy.destroy:destroy_all",
    "architecture.table_format.iceberg.version": ("lakebench.deps.request:jar_coordinates"),
    "architecture.table_format.delta.version": ("lakebench.deps.request:jar_coordinates"),
    "architecture.pipeline_engine": "lakebench.engine.protocol:get_engine",
    "architecture.query_engine.type": "lakebench.deploy.destroy:destroy_all",
    "architecture.query_engine.trino.coordinator.cpu": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.query_engine.trino.coordinator.memory": (
        "lakebench.deploy.engine:DeploymentEngine._trino_memory"
    ),
    "architecture.query_engine.trino.worker.replicas": (
        "lakebench.deploy.engine:DeploymentEngine._trino_memory"
    ),
    "architecture.query_engine.trino.worker.cpu": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.query_engine.trino.worker.memory": (
        "lakebench.deploy.engine:DeploymentEngine._trino_memory"
    ),
    "architecture.query_engine.trino.worker.spill_enabled": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.query_engine.trino.worker.spill_max_per_node": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.query_engine.trino.worker.storage": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.query_engine.trino.worker.storage_class": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.query_engine.trino.catalog_name": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.query_engine.spark_thrift.cores": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.query_engine.spark_thrift.memory": (
        "lakebench.deploy.engine:DeploymentEngine._thrift_pod_memory"
    ),
    "architecture.query_engine.spark_thrift.catalog_name": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.query_engine.duckdb.cores": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.query_engine.duckdb.memory": (
        "lakebench.deploy.engine:DeploymentEngine._build_context"
    ),
    "architecture.query_engine.duckdb.catalog_name": "lakebench.benchmark.executor:get_executor",
    "architecture.query_engine.duckdb.version": "lakebench.deps.request:select_request",
    "architecture.pipeline.pattern": "lakebench.config.autosizer:_apply_cluster_scaling",
    "architecture.pipeline.mode": "lakebench.cli._run:run",
    "architecture.pipeline.cycles": "lakebench.cli._run:run",
    "architecture.pipeline.pre_benchmark_maintenance": "lakebench.cli._run:run",
    "architecture.pipeline.sustained.bronze_trigger_interval": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_env_vars"
    ),
    "architecture.pipeline.sustained.silver_trigger_interval": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_env_vars"
    ),
    "architecture.pipeline.sustained.gold_refresh_interval": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_env_vars"
    ),
    "architecture.pipeline.sustained.run_duration": "lakebench.cli._sustained:_run_sustained",
    "architecture.pipeline.sustained.checkpoint_base": (
        "lakebench.modules.pipeline_engines.spark.job:bronze_ingest_checkpoint_uri"
    ),
    "architecture.pipeline.sustained.max_files_per_trigger": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_env_vars"
    ),
    "architecture.pipeline.sustained.bronze_target_file_size_mb": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_env_vars"
    ),
    "architecture.pipeline.sustained.silver_target_file_size_mb": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_env_vars"
    ),
    "architecture.pipeline.sustained.silver_bronze_wait_seconds": (
        "lakebench.config.schema:SustainedConfig.effective_silver_bronze_wait_seconds"
    ),
    "architecture.pipeline.sustained.gold_target_file_size_mb": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_env_vars"
    ),
    "architecture.pipeline.sustained.retention_interval": (
        "lakebench.cli._sustained:resolve_maintenance_schedule"
    ),
    "architecture.pipeline.sustained.retention_threshold": (
        "lakebench.cli._sustained:continuous_retention_record"
    ),
    "architecture.pipeline.sustained.compaction_enabled": (
        "lakebench.cli._sustained:resolve_maintenance_schedule"
    ),
    "architecture.pipeline.sustained.compaction_interval": (
        "lakebench.cli._sustained:resolve_maintenance_schedule"
    ),
    "architecture.pipeline.sustained.benchmark_interval": (
        "lakebench.cli._sustained:_run_sustained"
    ),
    "architecture.pipeline.sustained.benchmark_warmup": "lakebench.cli._sustained:_run_sustained",
    "architecture.workload.schema_type": "lakebench.deploy.datagen:bronze_datagen_prefix",
    "architecture.workload.datagen.scale": (
        "lakebench.modules.pipeline_engines.spark.job:_streaming_concurrent_budget"
    ),
    "architecture.workload.datagen.target_size": (
        "lakebench.config.schema:DatagenConfig.resolve_scale_from_target_size"
    ),
    "architecture.workload.datagen.mode": "lakebench.config.autosizer:_resolve_datagen_mode",
    "architecture.workload.datagen.seed": "lakebench.config.datagen_seed:config_seed",
    "architecture.workload.datagen.corpus_role": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_manifest"
    ),
    "architecture.workload.datagen.robustness_perturbation": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_env_vars"
    ),
    "architecture.workload.datagen.parallelism": (
        "lakebench.deploy.datagen:DatagenDeployer._build_datagen_context"
    ),
    "architecture.workload.datagen.file_size": (
        "lakebench.deploy.datagen:DatagenDeployer._build_datagen_context"
    ),
    "architecture.workload.datagen.dirty_data_ratio": (
        "lakebench.deploy.datagen:DatagenDeployer._build_datagen_context"
    ),
    "architecture.workload.datagen.cpu": (
        "lakebench.deploy.datagen:DatagenDeployer._build_datagen_context"
    ),
    "architecture.workload.datagen.memory": (
        "lakebench.deploy.datagen:DatagenDeployer._build_datagen_context"
    ),
    "architecture.workload.datagen.generators": (
        "lakebench.deploy.datagen:DatagenDeployer._build_datagen_context"
    ),
    "architecture.workload.datagen.timestamp_start": (
        "lakebench.deploy.datagen:DatagenDeployer._build_datagen_context"
    ),
    "architecture.workload.datagen.timestamp_end": (
        "lakebench.deploy.datagen:DatagenDeployer._build_datagen_context"
    ),
    "architecture.workload.customer360.unique_customers": (
        "lakebench.config.schema:LakebenchConfig.get_scale_dimensions"
    ),
    "architecture.workload.retention_workload": (
        "lakebench.cli._sustained:resolve_maintenance_retention"
    ),
    "architecture.workload.retention_months": (
        "lakebench.cli._sustained:resolve_maintenance_retention"
    ),
    "architecture.workload.w1_max_vertices": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_env_vars"
    ),
    "architecture.workload.tm_operations.enabled": "lakebench.cli._run:run",
    "architecture.workload.tm_operations.seed": "lakebench.config.schema:TmOperationsConfig.env",
    "architecture.workload.tm_operations.analyst_accuracy": (
        "lakebench.config.schema:TmOperationsConfig.env"
    ),
    "architecture.workload.tm_operations.investigator_accuracy": (
        "lakebench.config.schema:TmOperationsConfig.env"
    ),
    "architecture.workload.tm_operations.qa_sample_rate": (
        "lakebench.config.schema:TmOperationsConfig.env"
    ),
    "architecture.workload.tm_operations.alert_sla_days": (
        "lakebench.config.schema:TmOperationsConfig.env"
    ),
    "architecture.workload.tm_operations.case_lookback_months": (
        "lakebench.config.schema:TmOperationsConfig.env"
    ),
    "architecture.workload.tm_operations.late_filing_rate": (
        "lakebench.config.schema:TmOperationsConfig.env"
    ),
    "architecture.workload.tm_operations.no_suspect_rate": (
        "lakebench.config.schema:TmOperationsConfig.env"
    ),
    "architecture.workload.tm_operations.max_alerts_per_customer": (
        "lakebench.config.schema:TmOperationsConfig.env"
    ),
    "architecture.workload.tm_operations.continuous_interval_seconds": (
        "lakebench.config.schema:TmOperationsConfig.env"
    ),
    "architecture.workload.tm_operations.counterparty_scenarios": (
        "lakebench.config.schema:TmOperationsConfig.env"
    ),
    "architecture.benchmark.mode": "lakebench.cli._query:benchmark",
    "architecture.benchmark.streams": "lakebench.cli._query:benchmark",
    "architecture.benchmark.cache": "lakebench.cli._query:benchmark",
    "architecture.benchmark.iterations": "lakebench.cli._query:benchmark",
    "architecture.benchmark.maintenance_settle.enabled": (
        "lakebench.cli._run:_settle_after_maintenance"
    ),
    "architecture.benchmark.maintenance_settle.max_seconds": (
        "lakebench.cli._run:_settle_after_maintenance"
    ),
    "architecture.benchmark.maintenance_settle.interval_seconds": (
        "lakebench.cli._run:_settle_after_maintenance"
    ),
    "architecture.benchmark.maintenance_settle.tolerance_pct": (
        "lakebench.cli._run:_settle_after_maintenance"
    ),
    "architecture.benchmark.maintenance_settle.probe_query": (
        "lakebench.cli._run:_settle_after_maintenance"
    ),
    "architecture.benchmark.maintenance_settle.probe_samples": (
        "lakebench.cli._run:_settle_after_maintenance"
    ),
    "architecture.tables.bronze": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_env_vars"
    ),
    "architecture.tables.silver": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_env_vars"
    ),
    "architecture.tables.gold": (
        "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_env_vars"
    ),
    "architecture.tables.silver_entities": "lakebench.benchmark.executor:get_executor",
    "architecture.tables.silver_accounts": "lakebench.benchmark.executor:get_executor",
    "architecture.tables.silver_account_statements": "lakebench.benchmark.executor:get_executor",
    "architecture.tables.silver_counterparty_edges": "lakebench.benchmark.executor:get_executor",
    "architecture.tables.silver_entity_profiles": (
        "lakebench.config.schema:TableNamesConfig.financial_env"
    ),
    "architecture.tables.silver_batch_versions": (
        "lakebench.config.schema:TableNamesConfig.financial_env"
    ),
    "architecture.tables.gold_alerts": "lakebench.cli._financial:replay",
    "architecture.tables.gold_risk_scores": "lakebench.benchmark.executor:get_executor",
    "architecture.tables.gold_entity_clusters": "lakebench.benchmark.executor:get_executor",
    "architecture.tables.gold_daily_dashboards": "lakebench.benchmark.executor:get_executor",
    "architecture.tables.gold_tm_reconciliation": (
        "lakebench.config.schema:TableNamesConfig.financial_env"
    ),
    "architecture.tables.gold_scenario_coverage": (
        "lakebench.config.schema:TableNamesConfig.financial_env"
    ),
    "architecture.tables.gold_alert_dispositions": "lakebench.benchmark.executor:get_executor",
    "architecture.tables.gold_cases": "lakebench.benchmark.executor:get_executor",
    "observability.enabled": "lakebench.deploy.datagen:DatagenDeployer._build_datagen_context",
    "observability.dashboards_enabled": (
        "lakebench.deploy.observability:ObservabilityDeployer._apply_dashboard"
    ),
    "observability.retention": (
        "lakebench.deploy.observability:ObservabilityDeployer._deploy_locked"
    ),
    "observability.storage": (
        "lakebench.deploy.observability:ObservabilityDeployer._build_helm_values"
    ),
    "observability.chart_version": (
        "lakebench.deploy.observability:ObservabilityDeployer._deploy_locked"
    ),
    "observability.pushgateway_enabled": (
        "lakebench.deploy.datagen:DatagenDeployer._build_datagen_context"
    ),
    "observability.pushgateway_image": (
        "lakebench.deploy.observability:ObservabilityDeployer._apply_podmonitor_templates"
    ),
    "observability.pushgateway_storage": (
        "lakebench.deploy.observability:ObservabilityDeployer._apply_podmonitor_templates"
    ),
    "observability.pushgateway_storage_class": (
        "lakebench.deploy.observability:ObservabilityDeployer._apply_podmonitor_templates"
    ),
    "spark.conf": "lakebench.modules.pipeline_engines.spark.job:SparkJobManager._build_manifest",
}

# Fields the walk does not require a reader for, with the reason. Empty:
# a field that is not read is removed (or refused) instead.
WALK_EXEMPT: dict[str, str] = {}
