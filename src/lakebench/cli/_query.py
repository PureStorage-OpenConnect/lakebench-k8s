"""Query and benchmark CLI commands for Lakebench.

Extracted from cli/__init__.py to reduce module size.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer
from rich.panel import Panel
from rich.table import Table

from lakebench.aml.look_guard import refuse_if_protected
from lakebench.cli._helpers import (
    _journal_safe,
    console,
    emit_data,
    err_console,
    esc,
    journal_open,
    markup,
    print_error,
    print_info,
    print_warning,
    resolve_config_path,
)
from lakebench.cli._json import json_option
from lakebench.config import (
    ConfigError,
    LoadPurpose,
    load_config,
)
from lakebench.exit_codes import ExitCode
from lakebench.journal import CommandName, EventType

# =============================================================================
# Enterprise Query Examples
# =============================================================================

EXAMPLE_QUERIES: dict[str, tuple[str, str]] = {
    "count": (
        "Row counts per table",
        """SELECT 'silver.customer_interactions_enriched' AS table_name, count(*) AS row_count
FROM lakehouse.silver.customer_interactions_enriched
UNION ALL
SELECT 'gold.customer_executive_dashboard', count(*)
FROM lakehouse.gold.customer_executive_dashboard""",
    ),
    "revenue": (
        "Daily revenue with 7-day moving average",
        """SELECT
  interaction_date,
  total_daily_revenue,
  ROUND(avg_transaction_value, 2) AS avg_txn,
  daily_active_customers AS dau,
  ROUND(AVG(total_daily_revenue) OVER (
    ORDER BY interaction_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) AS revenue_ma7
FROM lakehouse.gold.customer_executive_dashboard
ORDER BY interaction_date DESC
LIMIT 30""",
    ),
    "channels": (
        "Channel revenue breakdown",
        """SELECT
  interaction_date,
  web_revenue,
  mobile_revenue,
  store_revenue,
  call_center_revenue,
  total_daily_revenue,
  web_interactions + mobile_interactions + store_interactions AS total_interactions,
  conversions
FROM lakehouse.gold.customer_executive_dashboard
ORDER BY interaction_date DESC
LIMIT 30""",
    ),
    "engagement": (
        "Engagement and churn risk summary",
        """SELECT
  interaction_date,
  daily_active_customers,
  ROUND(avg_engagement_score, 2) AS avg_engagement,
  high_churn_risk_count,
  medium_churn_risk_count,
  support_tickets_created,
  ROUND(avg_satisfaction_score, 2) AS avg_satisfaction,
  loyalty_member_interactions,
  total_points_earned
FROM lakehouse.gold.customer_executive_dashboard
ORDER BY interaction_date DESC
LIMIT 30""",
    ),
    "funnel": (
        "Daily conversion funnel",
        """SELECT
  interaction_date,
  awareness_interactions,
  consideration_interactions,
  conversions,
  retention_interactions,
  ROUND(CAST(conversions AS DOUBLE) / NULLIF(awareness_interactions, 0) * 100, 2) AS conversion_rate_pct
FROM lakehouse.gold.customer_executive_dashboard
ORDER BY interaction_date DESC
LIMIT 30""",
    ),
    "clv": (
        "Lifetime value estimates by day",
        """SELECT
  interaction_date,
  daily_active_customers,
  ROUND(total_estimated_ltv, 2) AS total_ltv,
  ROUND(avg_estimated_ltv, 2) AS avg_ltv,
  total_transactions,
  ROUND(avg_transaction_value, 2) AS avg_txn_value,
  ROUND(largest_transaction, 2) AS max_txn
FROM lakehouse.gold.customer_executive_dashboard
ORDER BY interaction_date DESC
LIMIT 30""",
    ),
}


def _run_query_repl(
    config_file: Path,
    timeout: int,
    output_format: str,
) -> None:
    """Interactive SQL REPL for the configured query engine."""
    import sys

    from rich.prompt import Prompt

    from lakebench.benchmark.executor import get_executor

    if not sys.stdin.isatty():
        print_error("Interactive mode requires a terminal")
        raise typer.Exit(ExitCode.USAGE)

    config_file = resolve_config_path(config_file)
    try:
        cfg = load_config(config_file, purpose=LoadPurpose.MUTATE)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904
    refuse_if_protected(cfg, "query")

    namespace = cfg.get_namespace()

    try:
        executor = get_executor(cfg, namespace)
    except ValueError as e:
        print_error(str(e))
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904

    console.print(
        Panel(
            f"Lakebench SQL REPL ({esc(executor.engine_name())})\n"
            f"Namespace: {esc(namespace)}\n"
            f"Format: {esc(output_format)} | Timeout: {esc(timeout)}s\n"
            f"Type 'exit', 'quit', or Ctrl+D to quit",
            expand=False,
        )
    )

    query_count = 0
    while True:
        try:
            sql = Prompt.ask("\n[bold cyan]SQL[/bold cyan]")
        except (EOFError, KeyboardInterrupt):
            break

        sql = sql.strip()

        # Exit commands
        if sql.lower() in ("exit", "quit", ".quit", "\\q"):
            break

        # Skip empty input
        if not sql:
            continue

        # Strip trailing semicolon
        if sql.endswith(";"):
            sql = sql[:-1]

        query_count += 1

        try:
            result = executor.execute_query(sql, timeout=timeout)
        except RuntimeError as e:
            print_error(str(e))
            continue

        if not result.success:
            print_error(result.error or "Query failed")
            continue

        # Display results
        output = result.raw_output
        rows = output.split("\n") if output else []
        row_count = result.rows_returned

        if rows:
            if output_format == "json":
                import json

                data = [row.split("\t") for row in rows]
                emit_data(json.dumps({"rows": data, "count": len(data)}, indent=2))
            elif output_format == "csv":
                import csv
                import io

                buf = io.StringIO()
                writer = csv.writer(buf)
                for row in rows:
                    writer.writerow(row.split("\t"))
                emit_data(buf.getvalue())
            else:  # table
                for line in rows[:50]:
                    console.print(line, markup=False, highlight=False)
                if row_count > 50:
                    console.print(f"[dim]... ({esc(row_count - 50)} more rows)[/dim]")

        err_console.print(f"[green]{esc(row_count)} rows in {result.duration_seconds:.2f}s[/green]")

    console.print(f"\n[dim]Executed {esc(query_count)} queries. Goodbye![/dim]")


def _rows_as_dicts(rows: list[str]) -> list[dict[str, str]]:
    """Tab-separated result lines as dicts: the first line names the
    columns; one line alone is keyed by position."""
    parsed = [row.split("\t") for row in rows if row.strip()]
    if len(parsed) > 1:
        headers = [h.strip().strip('"') for h in parsed[0]]
        return [
            dict(zip(headers, [v.strip().strip('"') for v in r], strict=False)) for r in parsed[1:]
        ]
    return [{str(i): v.strip().strip('"') for i, v in enumerate(r)} for r in parsed]


def _query_json_rows(engine: str, raw: str) -> tuple[list[str] | None, list[list[str]], str]:
    """``(columns, rows, row_format)`` of a result as each executor prints
    it. Trino: CSV, no header (columns None). Spark Thrift: tsv2, a header
    line then a line per row. DuckDB: a JSON payload whose ``data`` holds up
    to 100 rows as Python reprs (one cell each, columns None)."""
    import csv
    import io

    if engine == "duckdb":
        from lakebench.benchmark.fingerprint import last_json_line

        payload = last_json_line((raw or "").strip()) or {}
        return None, [[str(d)] for d in payload.get("data") or []], "python-repr"
    if engine == "spark-thrift":
        # raw_output is already trimmed of beeline's terminal newline
        # (executor._drop_terminal_newline); nothing more goes, so an empty
        # last cell, or a row holding one empty string, stays data.
        text = raw or ""
        if not text:
            return None, [], "tsv2"
        lines = text.split("\n")
        return lines[0].split("\t"), [ln.split("\t") for ln in lines[1:]], "tsv2"
    text = (raw or "").strip()
    return None, [list(r) for r in csv.reader(io.StringIO(text))], "csv"


def query(
    config_file: Annotated[
        Path | None,
        typer.Argument(
            help="Path to configuration YAML file (default: ./lakebench.yaml)",
        ),
    ] = None,
    file_option: Annotated[
        Path | None,
        typer.Option(
            "--file",
            "-f",
            help="Path to configuration YAML file (alternative to positional argument)",
        ),
    ] = None,
    sql: Annotated[
        str | None,
        typer.Option(
            "--sql",
            "-q",
            help="SQL query to execute",
        ),
    ] = None,
    example: Annotated[
        str | None,
        typer.Option(
            "--example",
            "-e",
            help="Run a built-in example query (count, revenue, channels, engagement, funnel, clv)",
        ),
    ] = None,
    sql_file: Annotated[
        Path | None,
        typer.Option(
            "--sql-file",
            help="Read SQL from file (use '-' for stdin)",
        ),
    ] = None,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="Start interactive SQL shell (REPL)",
        ),
    ] = False,
    output_format: Annotated[
        str,
        typer.Option(
            "--format",
            "-o",
            help="Output format: table (default), json, csv",
        ),
    ] = "table",
    show_query: Annotated[
        bool,
        typer.Option(
            "--show-query",
            help="Show the SQL query before executing",
        ),
    ] = False,
    query_timeout: Annotated[
        int,
        typer.Option(
            "--timeout",
            "-t",
            help="Query timeout in seconds",
        ),
    ] = 120,
    as_json: Annotated[bool, json_option()] = False,
) -> None:
    """Execute SQL queries against the configured query engine.

    Run custom SQL, built-in examples, read from file, or start interactive shell.
    Results can be displayed as table, JSON, or CSV.

    Examples:

        lakebench query --example count

        lakebench query --sql "SELECT count(*) FROM lakehouse.gold.customer_executive_dashboard"

        lakebench query --sql-file query.sql

        lakebench query --sql-file - < query.sql

        lakebench query --interactive

        lakebench query --example count --format json
    """
    import sys

    config_file = resolve_config_path(config_file, file_option)

    # Validate mutually exclusive options
    sources = sum(1 for x in [sql, example, sql_file, interactive] if x)
    if sources > 1:
        print_error("Specify only one of: --sql, --example, --sql-file, --interactive")
        raise typer.Exit(ExitCode.USAGE)

    if as_json and (output_format != "table" or interactive):
        print_error("--json does not combine with --format or --interactive")
        raise typer.Exit(ExitCode.USAGE)

    # Handle interactive mode early
    if interactive:
        _run_query_repl(config_file, query_timeout, output_format)
        return

    # Handle file input
    query_name = "custom"
    if sql_file is not None:
        if str(sql_file) == "-":
            if sys.stdin.isatty():
                print_error("No input from stdin (pipe SQL or use --sql/--example)")
                raise typer.Exit(ExitCode.USAGE)
            sql = sys.stdin.read().strip()
            query_name = "stdin"
        else:
            try:
                sql = sql_file.read_text().strip()
                query_name = sql_file.stem
            except FileNotFoundError:
                print_error(f"File not found: {sql_file}")
                raise typer.Exit(ExitCode.USAGE)  # noqa: B904
            except Exception as e:
                print_error(f"Error reading file: {e}")
                raise typer.Exit(ExitCode.FAILED)  # noqa: B904
    elif example:
        if example not in EXAMPLE_QUERIES:
            print_error(f"Unknown example: {example}")
            print_info(f"Available: {', '.join(EXAMPLE_QUERIES.keys())}")
            raise typer.Exit(ExitCode.USAGE)
        query_name = example
        _, sql = EXAMPLE_QUERIES[example]
    elif not sql:
        # No input specified - show help
        console.print(Panel("Built-in Enterprise Query Examples", expand=False))
        table = Table()
        table.add_column("Name", style="cyan")
        table.add_column("Description")
        for name, (desc, _) in EXAMPLE_QUERIES.items():
            table.add_row(name, desc)
        console.print(table)
        print_info("Usage: lakebench query <config> --example <name>")
        print_info('Usage: lakebench query <config> --sql "SELECT ..."')
        print_info("Usage: lakebench query <config> --sql-file query.sql")
        print_info("Usage: lakebench query <config> --interactive")
        return

    if not sql:
        print_error("No SQL query provided")
        raise typer.Exit(ExitCode.USAGE)

    # Load config
    try:
        cfg = load_config(config_file, purpose=LoadPurpose.MUTATE)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904
    refuse_if_protected(cfg, "query")

    namespace = cfg.get_namespace()

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.QUERY, {"query_name": query_name})

    if show_query or example:
        # stderr: with --format json|csv, stdout carries only the data.
        err_console.print(f"\n[dim]Query ({esc(query_name)}):[/dim]")
        err_console.print(f"[dim]{esc(sql)}[/dim]\n", soft_wrap=True)

    # Execute via QueryExecutor
    from lakebench.benchmark.executor import get_executor

    try:
        executor = get_executor(cfg, namespace)
    except ValueError as e:
        print_error(str(e))
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904

    try:
        result = executor.execute_query(sql, timeout=query_timeout)
    except FileNotFoundError:
        print_error("kubectl not found on PATH")
        _journal_safe(j.end_command, success=False, message="kubectl not found")
        raise typer.Exit(ExitCode.PREREQUISITE)  # noqa: B904
    except RuntimeError as e:
        print_error(str(e))
        print_info("Is the query engine deployed? Run: lakebench status")
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(ExitCode.FAILED)  # noqa: B904

    elapsed = result.duration_seconds

    if not result.success:
        detail = f": {result.error}" if result.error else ""
        print_error(f"Query failed ({elapsed:.2f}s){detail}")
        _journal_safe(
            j.record,
            EventType.QUERY_EXECUTED,
            message=f"Query '{query_name}' failed",
            success=False,
            details={
                "query_name": query_name,
                "elapsed_seconds": round(elapsed, 3),
                "success": False,
            },
        )
        _journal_safe(j.end_command, success=False, message="Query failed")
        raise typer.Exit(ExitCode.FAILED)

    # Parse and display results
    output = result.raw_output
    rows = output.split("\n") if output else []
    row_count = result.rows_returned

    if as_json:
        from lakebench.cli import _json

        engine = cfg.architecture.query_engine.type.value
        columns, cells, row_format = _query_json_rows(engine, output)
        _json.set_data(
            {
                "query_name": query_name,
                "engine": engine,
                "count": row_count,
                "elapsed_seconds": round(elapsed, 3),
                "columns": columns,
                "row_format": row_format,
                "rows": cells,
            }
        )
    elif rows:
        if output_format == "json":
            import json

            data = _rows_as_dicts(rows)
            emit_data(json.dumps({"rows": data, "count": len(data)}, indent=2))
        elif output_format == "csv":
            import csv
            import io

            buf = io.StringIO()
            writer = csv.writer(buf)
            for row in rows:
                if row.strip():
                    writer.writerow([c.strip().strip('"') for c in row.split("\t")])
            emit_data(buf.getvalue())
        else:  # table (default)
            console.print()
            for line in rows[:50]:
                console.print(line, markup=False, highlight=False)
            if row_count > 50:
                console.print(f"[dim]... ({esc(row_count - 50)} more rows)[/dim]")

    err_console.print(f"\n[green]{esc(row_count)} rows in {elapsed:.2f}s[/green]")

    _journal_safe(
        j.record,
        EventType.QUERY_EXECUTED,
        message=f"Query '{query_name}' returned {row_count} rows in {elapsed:.2f}s",
        success=True,
        details={
            "query_name": query_name,
            "elapsed_seconds": round(elapsed, 3),
            "rows": row_count,
            "success": True,
        },
    )
    # The query's numbers are printed above and journalled, never written
    # into a run record: a record is written once, by the run that owns it.
    _journal_safe(j.end_command, success=True)


def _display_power_results(result: Any) -> None:
    """Display power benchmark results."""
    console.print()
    for qr in result.queries:
        status = "[green]PASS[/green]" if qr.success else "[red]FAIL[/red]"
        name_padded = f"{qr.query.name[:4]}  {qr.query.display_name}"
        console.print(
            f"  {esc(name_padded):<40} {qr.elapsed_seconds:>7.2f}s   "
            f"{esc(qr.rows_returned):>6} rows   {markup(status)}"
        )

    console.print(f"\n  Total: {result.total_seconds:.2f}s")
    console.print(f"  [bold]Power QpH: {result.qph:.1f}[/bold]")


def _display_throughput_results(result: Any) -> None:
    """Display throughput benchmark results."""
    console.print()
    console.print(f"  [bold]Throughput run: {esc(result.streams)} streams[/bold]")
    for sr in result.stream_results:
        status = "[green]PASS[/green]" if sr.success else "[red]FAIL[/red]"
        console.print(
            f"    Stream {esc(sr.stream_id)}:  {len(sr.queries):>2} queries  "
            f"{sr.total_seconds:>7.1f}s  {markup(status)}"
        )
    console.print(f"\n  Wall clock: {result.total_seconds:.1f}s")
    console.print(f"  [bold]Throughput QpH: {result.qph:.1f}[/bold]")


def _latest_tm_run_id(cfg) -> str | None:
    """The deployment's newest recorded run, when its TM operations layer ran
    (verdict pass or fail). Each run overwrites the TM tables, so an older
    run's verdict says nothing about what the tables hold now: when the
    newest run's layer did not run, the investigator queries are skipped."""
    from lakebench.metrics import MetricsStorage

    try:
        storage = MetricsStorage()
        for info in storage.list_runs():
            if info.get("deployment_name") not in (None, cfg.name):
                continue
            if info.get("record_kind", "run") != "run":
                continue  # a benchmark record copies its run's TM status
            run = storage.load_run(info["run_id"])
            if run is None or run.deployment_name != cfg.name:
                continue
            status = (getattr(run, "tm_operations", None) or {}).get("status")
            return run.run_id if status in ("pass", "fail") else None
    except Exception:  # noqa: BLE001 -- no history: no investigator queries
        return None
    return None


def _save_benchmark_record(
    storage: Any, parent: Any, result: Any, *, started_at: str | None = None
) -> Path:
    """Save ``lakebench benchmark``'s record and return its path.

    *parent* is the deployment's latest run record as loaded (a copy in
    memory; its file is never opened for writing). The new record is that
    copy with *result* as its benchmark everywhere a reader takes a QpH
    from: ``benchmark``, the pipeline benchmark's ``query_benchmark`` and
    query stage (so its scores, ``compare`` and the HTML card show this
    benchmark), and the experiment block's benchmark half. A continuous
    parent's in-stream rounds are dropped from the copy, with the aggregates
    taken from them and the experiment block's round counts: they are the
    run's measurement, and the scores would prefer them. The parent's
    maintenance QpH pair (before and after compaction) is dropped too. ``provenance.benchmark``
    names the code that ran this benchmark and when. The record gets a new
    run id, ``record_kind`` "benchmark" and ``parent_run_id``; its series
    stamp is dropped (a benchmark is not a repetition). The save is a
    create; a run id already on disk is never overwritten (a second id is
    tried once)."""
    import uuid
    from datetime import datetime

    from lakebench._clock import utc_now
    from lakebench.metrics import BenchmarkMetrics
    from lakebench.metrics.collector import StageMetrics
    from lakebench.metrics.experiment import refresh_benchmark
    from lakebench.metrics.provenance import run_provenance
    from lakebench.metrics.storage import RecordExistsError

    record = parent
    parent_run_id = parent.run_id
    bench = BenchmarkMetrics(
        mode=result.mode,
        cache=result.cache,
        scale=result.scale,
        qph=result.qph,
        total_seconds=result.total_seconds,
        queries=[q.to_dict() for q in result.queries],
        iterations=result.iterations,
        streams=result.streams,
        stream_results=[s.to_dict() for s in result.stream_results],
        engine=result.engine,
    )
    record.benchmark = bench
    record.benchmark_rounds = []
    pb = record.pipeline_benchmark
    if pb is not None:
        pb.query_benchmark = bench
        pb.benchmark_rounds = []
        pb.stages = [s for s in pb.stages if s.stage_type != "query"]
        pb.stages.append(
            StageMetrics(
                stage_name="query",
                stage_type="query",
                engine=result.engine or "trino",
                elapsed_seconds=result.total_seconds,
                success=True,
                input_size_gb=record.gold_size_gb,
                queries_executed=len(result.queries),
                queries_per_hour=result.qph,
            )
        )
        # Aggregates of the parent's benchmark that the copy would otherwise
        # carry next to its own QpH: the in-stream QpH trend and event age,
        # and the maintenance pair (QpH before and after compaction and the
        # value between them), measured by the run's own benchmark rounds.
        pb.qph_degradation_pct = None
        pb.query_time_event_age_seconds = 0.0
        pb.pre_compaction_qph = 0.0
        pb.post_compaction_qph = 0.0
        pb.maintenance_value_pct = None
        pb.maintenance_value_reason = ""
        pb.maintenance_paired_queries = 0
        pb.pre_compaction_benchmark = None
        if pb.pipeline_mode != "sustained":
            # Batch elapsed is the stage-time sum, which held the parent's
            # query stage. Continuous elapsed is the stream window's wall
            # clock (the stages overlap) and does not include the benchmark.
            pb.total_elapsed_seconds = sum(s.elapsed_seconds for s in pb.stages)
    # The stored experiment block is never rebuilt; bring its benchmark half
    # (results, iterations, mode) in line with the benchmark it now holds.
    refresh_benchmark(record)
    record.provenance = dict(record.provenance or {})
    record.provenance["benchmark"] = {
        **run_provenance(),
        "started_at": started_at,
        "ended_at": utc_now().isoformat(),
    }
    record.record_kind = "benchmark"
    record.parent_run_id = parent_run_id
    record.series = None
    # The record's success follows its verdict (the parent's pipeline, this
    # benchmark): a stored success never stands beside a FAILED verdict.
    from lakebench.metrics.verdict import apply_save_gate

    apply_save_gate(record, record.success, print_warning)

    def _new_id() -> str:
        return datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]

    record.run_id = _new_id()
    if pb is not None:
        pb.run_id = record.run_id
    try:
        return Path(storage.save_run(record))
    except RecordExistsError:
        record.run_id = _new_id()
        if pb is not None:
            pb.run_id = record.run_id
        return Path(storage.save_run(record))


def benchmark(
    config_file: Annotated[
        Path | None,
        typer.Argument(
            help="Path to configuration YAML file (default: ./lakebench.yaml)",
        ),
    ] = None,
    file_option: Annotated[
        Path | None,
        typer.Option(
            "--file",
            "-f",
            help="Path to configuration YAML file (alternative to positional argument)",
        ),
    ] = None,
    mode: Annotated[
        str | None,
        typer.Option(
            "--mode",
            "-m",
            help="Benchmark mode: power, throughput, or composite (overrides config)",
        ),
    ] = None,
    streams: Annotated[
        int | None,
        typer.Option(
            "--streams",
            "-s",
            help="Number of concurrent query streams for throughput/composite (overrides config)",
        ),
    ] = None,
    cold: Annotated[
        bool,
        typer.Option(
            "--cold",
            help="Flush Iceberg metadata cache before each query (cold run)",
        ),
    ] = False,
    iterations: Annotated[
        int | None,
        typer.Option(
            "--iterations",
            "-n",
            min=1,
            help=(
                "Timed runs per query, scored by the median "
                "(overrides architecture.benchmark.iterations, default 3)"
            ),
        ),
    ] = None,
    query_class: Annotated[
        str | None,
        typer.Option(
            "--class",
            "-c",
            help=(
                "Run only queries of a specific class (scan, filter_prune, "
                "aggregation, analytics, operational; AML also investigator)"
            ),
        ),
    ] = None,
) -> None:
    """Run query benchmark and compute QpH.

    Executes the workload's analytical query set (8 queries for
    Customer 360, 12 for AML) against the silver and gold tables
    and reports Queries per Hour (QpH) throughput.

    Modes:

        power       Single sequential query stream (default)

        throughput   N concurrent query streams

        composite    Power + throughput, geometric mean QpH

    Examples:

        lakebench benchmark test-config.yaml

        lakebench benchmark test-config.yaml --cold

        lakebench benchmark test-config.yaml --iterations 5

        lakebench benchmark test-config.yaml --mode throughput --streams 8

        lakebench benchmark test-config.yaml --mode composite --streams 4

        lakebench benchmark test-config.yaml --class scan
    """
    from lakebench.benchmark import BenchmarkRunner
    from lakebench.metrics import MetricsStorage

    config_file = resolve_config_path(config_file, file_option)

    try:
        cfg = load_config(config_file, purpose=LoadPurpose.MUTATE)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(ExitCode.USAGE)  # noqa: B904
    refuse_if_protected(cfg, "benchmark")

    scale = cfg.architecture.workload.datagen.get_effective_scale()
    cache_mode = "cold" if cold else None  # None = let runner use config default

    # Resolve effective mode for display
    bench_cfg = cfg.architecture.benchmark
    effective_mode = mode or bench_cfg.mode.value
    effective_streams = streams if streams is not None else bench_cfg.streams
    effective_cache = cache_mode or bench_cfg.cache
    iterations = iterations if iterations is not None else bench_cfg.iterations

    console.print(
        Panel(
            f"Lakebench Query Benchmark\n"
            f"{esc('=' * 25)}\n"
            f"Scale: {esc(scale)}\n"
            f"Mode: {esc(effective_mode)}"
            + (f" ({esc(effective_streams)} streams)" if effective_mode != "power" else "")
            + f" ({esc(iterations)} iteration{esc('s' if iterations > 1 else '')})\n"
            f"Cache: {esc(effective_cache)}"
            + (f"\nClass: {esc(query_class)}" if query_class else ""),
            expand=False,
        )
    )

    # Before the benchmark runs: say now, not after an hour of queries, that
    # its results will not be recorded against the latest run (another
    # dependency set).
    from lakebench.deps import runtime as deps_runtime
    from lakebench.metrics import MetricsStorage as _Storage

    _latest = _Storage().get_latest_run_for_deployment(cfg.name, writable=True)
    early_refusal = deps_runtime.attach_refusal(cfg, _latest) if _latest else None
    if early_refusal and _latest is not None:
        print_warning(f"Results may not be recorded against run {_latest.run_id}: {early_refusal}")

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(
        CommandName.BENCHMARK,
        {
            "mode": effective_mode,
            "streams": effective_streams,
            "cold": cold,
            "iterations": iterations,
            "query_class": query_class,
        },
    )

    _journal_safe(
        j.record,
        EventType.BENCHMARK_START,
        message="Benchmark started",
        details={"mode": effective_mode, "cache": effective_cache, "scale": scale},
    )

    from lakebench._clock import utc_now

    benchmark_started_at = utc_now().isoformat()
    try:
        runner = BenchmarkRunner(cfg, tm_run_id=_latest_tm_run_id(cfg))
        if query_class == "investigator" and runner.tm_run_id is None:
            print_warning(
                "No run of this deployment has TM operations tables that ran "
                "(verdict pass or fail); the investigator queries are skipped."
            )
        run_result = runner.run(
            mode=mode,
            cache=cache_mode,
            iterations=iterations,
            streams=streams,
            query_class=query_class,
        )
    except RuntimeError as e:
        print_error(str(e))
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(ExitCode.FAILED)  # noqa: B904

    # Handle composite (returns tuple) vs single result. Track the
    # throughput half separately so the composite qph=0 gate below can
    # inspect it without indexing back into the union.
    composite_throughput_qph: float | None = None
    if isinstance(run_result, tuple):
        power_result, throughput_result, composite_result = run_result
        _display_power_results(power_result)
        _display_throughput_results(throughput_result)
        console.print(
            f"\n  [bold]Composite QpH: {composite_result.qph:.1f}[/bold]  "
            f"(geometric mean of power {power_result.qph:.1f} "
            f"and throughput {throughput_result.qph:.1f})"
        )
        primary_result = composite_result
        composite_throughput_qph = throughput_result.qph
    else:
        if run_result.mode == "throughput":
            _display_throughput_results(run_result)
        else:
            _display_power_results(run_result)
        primary_result = run_result

    # Benchmark gate (invariant 3: exit 0 is not a pass). Mirrors what
    # `run` applies via _benchmark_gate_problems: any query failure
    # outside the known-upstream allowlist, any zero-row query not
    # declared allow_empty, and the empty-set / all-failed case all fail
    # the run. Also refuse when composite mode's throughput half
    # produced qph=0 (the LB-044 shape: power passes, throughput is
    # empty, composite headline is 0.0 but exit was previously 0).
    from lakebench.cli._run import _benchmark_gate_problems

    total_queries = len(primary_result.queries)
    succeeded = [q for q in primary_result.queries if bool(getattr(q, "success", False))]
    gate_problems: list[str] = []
    if total_queries == 0 or not succeeded:
        reason = (
            "the query set was empty"
            if total_queries == 0
            else f"0 of {total_queries} queries succeeded"
        )
        gate_problems.append(
            f"Benchmark produced no query results ({reason}); "
            "QpH over the rest is not a valid score."
        )
    else:
        gate_problems.extend(_benchmark_gate_problems(cfg, primary_result.queries))
        if composite_throughput_qph is not None and composite_throughput_qph <= 0:
            gate_problems.append(
                f"Composite throughput half produced qph={composite_throughput_qph}; "
                "composite QpH is not a valid score."
            )
    if gate_problems:
        for problem in gate_problems:
            print_error(problem)
        _journal_safe(
            j.end_command,
            success=False,
            message="benchmark gate failed: " + "; ".join(gate_problems),
        )
        raise typer.Exit(ExitCode.FAILED)

    # Record the benchmark in a record of its own: a copy of the deployment's
    # latest run record (read, never written) with this benchmark, under a new
    # run id, record_kind "benchmark" and parent_run_id. Scoped by deployment
    # name, with no fallback to a legacy record of unknown deployment, so a
    # parallel deployment's run is never the parent.
    storage = MetricsStorage()
    parent = storage.get_latest_run_for_deployment(cfg.name, writable=True)
    from lakebench.deps import runtime as deps_runtime

    refusal = deps_runtime.attach_refusal(cfg, parent) if parent else None
    if refusal:
        print_warning(f"Benchmark not recorded: {refusal}")
        parent = None
    elif parent is None:
        print_warning(
            f"Benchmark not recorded: deployment {cfg.name} has no run record to measure "
            "against; `lakebench run` records one"
        )
    if parent is not None:
        parent_id = parent.run_id
        record_path = _save_benchmark_record(
            storage, parent, primary_result, started_at=benchmark_started_at
        )
        record_id = record_path.parent.name.removeprefix("run-")
        print_info(
            f"Benchmark recorded as run {record_id} (a benchmark record of run "
            f"{parent_id}, which is unchanged); lakebench report {record_id}"
        )

    _journal_safe(
        j.record,
        EventType.BENCHMARK_COMPLETE,
        message=f"Benchmark complete: QpH={primary_result.qph:.1f}",
        success=True,
        details={
            "mode": primary_result.mode,
            "qph": round(primary_result.qph, 1),
            "total_seconds": round(primary_result.total_seconds, 2),
            "queries_passed": sum(1 for q in primary_result.queries if q.success),
            "queries_total": len(primary_result.queries),
            "streams": primary_result.streams,
        },
    )
    _journal_safe(j.end_command, success=True)
