"""Query and benchmark CLI commands for Lakebench.

Extracted from cli/__init__.py to reduce module size.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

import typer
from rich.panel import Panel
from rich.table import Table

from lakebench.cli._helpers import (
    _journal_safe,
    console,
    journal_open,
    print_error,
    print_info,
    resolve_config_path,
)
from lakebench.config import (
    ConfigError,
    load_config,
)
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
        raise typer.Exit(1)

    config_file = resolve_config_path(config_file)
    try:
        cfg = load_config(config_file)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1)  # noqa: B904

    namespace = cfg.get_namespace()

    try:
        executor = get_executor(cfg, namespace)
    except ValueError as e:
        print_error(str(e))
        raise typer.Exit(1)  # noqa: B904

    console.print(
        Panel(
            f"Lakebench SQL REPL ({executor.engine_name()})\n"
            f"Namespace: {namespace}\n"
            f"Format: {output_format} | Timeout: {timeout}s\n"
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
                console.print(json.dumps({"rows": data, "count": len(data)}, indent=2))
            elif output_format == "csv":
                import csv
                import io

                buf = io.StringIO()
                writer = csv.writer(buf)
                for row in rows:
                    writer.writerow(row.split("\t"))
                console.print(buf.getvalue().strip())
            else:  # table
                for line in rows[:50]:
                    console.print(line)
                if row_count > 50:
                    console.print(f"[dim]... ({row_count - 50} more rows)[/dim]")

        console.print(f"[green]{row_count} rows in {result.duration_seconds:.2f}s[/green]")

    console.print(f"\n[dim]Executed {query_count} queries. Goodbye![/dim]")


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

    from lakebench.metrics import QueryMetrics

    config_file = resolve_config_path(config_file, file_option)

    # Validate mutually exclusive options
    sources = sum(1 for x in [sql, example, sql_file, interactive] if x)
    if sources > 1:
        print_error("Specify only one of: --sql, --example, --sql-file, --interactive")
        raise typer.Exit(1)

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
                raise typer.Exit(1)
            sql = sys.stdin.read().strip()
            query_name = "stdin"
        else:
            try:
                sql = sql_file.read_text().strip()
                query_name = sql_file.stem
            except FileNotFoundError:
                print_error(f"File not found: {sql_file}")
                raise typer.Exit(1)  # noqa: B904
            except Exception as e:
                print_error(f"Error reading file: {e}")
                raise typer.Exit(1)  # noqa: B904
    elif example:
        if example not in EXAMPLE_QUERIES:
            print_error(f"Unknown example: {example}")
            print_info(f"Available: {', '.join(EXAMPLE_QUERIES.keys())}")
            raise typer.Exit(1)
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
        print_info("Usage: lakebench query <config> --file query.sql")
        print_info("Usage: lakebench query <config> --interactive")
        return

    if not sql:
        print_error("No SQL query provided")
        raise typer.Exit(1)

    # Load config
    try:
        cfg = load_config(config_file)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1)  # noqa: B904

    namespace = cfg.get_namespace()

    # Journal
    j = journal_open(config_file, config_name=cfg.name)
    j.begin_command(CommandName.QUERY, {"query_name": query_name})

    if show_query or example:
        console.print(f"\n[dim]Query ({query_name}):[/dim]")
        console.print(f"[dim]{sql}[/dim]\n")

    # Execute via QueryExecutor
    from lakebench.benchmark.executor import get_executor

    try:
        executor = get_executor(cfg, namespace)
    except ValueError as e:
        print_error(str(e))
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904

    try:
        result = executor.execute_query(sql, timeout=query_timeout)
    except FileNotFoundError:
        print_error("kubectl not found on PATH")
        _journal_safe(j.end_command, success=False, message="kubectl not found")
        raise typer.Exit(1)  # noqa: B904
    except RuntimeError as e:
        print_error(str(e))
        print_info("Is the query engine deployed? Run: lakebench status")
        _journal_safe(j.end_command, success=False, message=str(e))
        raise typer.Exit(1)  # noqa: B904

    elapsed = result.duration_seconds

    if not result.success:
        print_error(f"Query failed ({elapsed:.2f}s)")
        if result.error:
            console.print(f"[red]{result.error}[/red]")
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
        raise typer.Exit(1)

    # Parse and display results
    output = result.raw_output
    rows = output.split("\n") if output else []
    row_count = result.rows_returned

    if rows:
        if output_format == "json":
            import json

            parsed = [row.split("\t") for row in rows if row.strip()]
            if len(parsed) > 1:
                headers = [h.strip().strip('"') for h in parsed[0]]
                data = [
                    dict(zip(headers, [v.strip().strip('"') for v in r], strict=False))
                    for r in parsed[1:]
                ]
            else:
                data = [{str(i): v.strip().strip('"') for i, v in enumerate(r)} for r in parsed]
            console.print(json.dumps({"rows": data, "count": len(data)}, indent=2))
        elif output_format == "csv":
            import csv
            import io

            buf = io.StringIO()
            writer = csv.writer(buf)
            for row in rows:
                if row.strip():
                    writer.writerow([c.strip().strip('"') for c in row.split("\t")])
            console.print(buf.getvalue().strip())
        else:  # table (default)
            console.print()
            for line in rows[:50]:
                console.print(line)
            if row_count > 50:
                console.print(f"[dim]... ({row_count - 50} more rows)[/dim]")

    console.print(f"\n[green]{row_count} rows in {elapsed:.2f}s[/green]")

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
    _journal_safe(j.end_command, success=True)

    # Record query metrics
    query_metrics = QueryMetrics(
        query_name=query_name,
        query_text=sql,
        elapsed_seconds=elapsed,
        rows_returned=row_count,
        success=True,
    )

    # Try to append to latest run's metrics
    from lakebench.metrics import MetricsStorage

    storage = MetricsStorage()
    latest_run = storage.get_latest_run()
    if latest_run:
        latest_run.queries.append(query_metrics)
        storage.save_run(latest_run)
        print_info(f"Query metrics appended to run {latest_run.run_id}")


def _display_power_results(result: Any) -> None:
    """Display power benchmark results."""
    console.print()
    for qr in result.queries:
        status = "[green]PASS[/green]" if qr.success else "[red]FAIL[/red]"
        name_padded = f"{qr.query.name[:4]}  {qr.query.display_name}"
        console.print(
            f"  {name_padded:<40} {qr.elapsed_seconds:>7.2f}s   "
            f"{qr.rows_returned:>6} rows   {status}"
        )

    console.print(f"\n  Total: {result.total_seconds:.2f}s")
    console.print(f"  [bold]Power QpH: {result.qph:.1f}[/bold]")


def _display_throughput_results(result: Any) -> None:
    """Display throughput benchmark results."""
    console.print()
    console.print(f"  [bold]Throughput run: {result.streams} streams[/bold]")
    for sr in result.stream_results:
        status = "[green]PASS[/green]" if sr.success else "[red]FAIL[/red]"
        console.print(
            f"    Stream {sr.stream_id}:  {len(sr.queries):>2} queries  "
            f"{sr.total_seconds:>7.1f}s  {status}"
        )
    console.print(f"\n  Wall clock: {result.total_seconds:.1f}s")
    console.print(f"  [bold]Throughput QpH: {result.qph:.1f}[/bold]")


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
        int,
        typer.Option(
            "--iterations",
            "-n",
            help="Number of iterations per query (>1 uses median)",
        ),
    ] = 1,
    query_class: Annotated[
        str | None,
        typer.Option(
            "--class",
            "-c",
            help="Run only queries of a specific class (scan, analytics, gold)",
        ),
    ] = None,
) -> None:
    """Run query benchmark and compute QpH.

    Executes 8 analytical queries against the Customer 360 pipeline
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
    from lakebench.metrics import BenchmarkMetrics, MetricsStorage

    config_file = resolve_config_path(config_file, file_option)

    try:
        cfg = load_config(config_file)
    except ConfigError as e:
        print_error(f"Config error: {e}")
        raise typer.Exit(1)  # noqa: B904

    scale = cfg.architecture.workload.datagen.get_effective_scale()
    cache_mode = "cold" if cold else None  # None = let runner use config default

    # Resolve effective mode for display
    bench_cfg = cfg.architecture.benchmark
    effective_mode = mode or bench_cfg.mode.value
    effective_streams = streams if streams is not None else bench_cfg.streams
    effective_cache = cache_mode or bench_cfg.cache

    console.print(
        Panel(
            f"Lakebench Query Benchmark\n"
            f"{'=' * 25}\n"
            f"Scale: {scale}\n"
            f"Mode: {effective_mode}"
            + (f" ({effective_streams} streams)" if effective_mode != "power" else "")
            + f" ({iterations} iteration{'s' if iterations > 1 else ''})\n"
            f"Cache: {effective_cache}" + (f"\nClass: {query_class}" if query_class else ""),
            expand=False,
        )
    )

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

    try:
        runner = BenchmarkRunner(cfg)
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
        raise typer.Exit(1)  # noqa: B904

    # Handle composite (returns tuple) vs single result
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
    else:
        if run_result.mode == "throughput":
            _display_throughput_results(run_result)
        else:
            _display_power_results(run_result)
        primary_result = run_result

    # Save to latest metrics if available
    storage = MetricsStorage()
    latest_run = storage.get_latest_run()
    if latest_run:
        bench_metrics = BenchmarkMetrics(
            mode=primary_result.mode,
            cache=primary_result.cache,
            scale=primary_result.scale,
            qph=primary_result.qph,
            total_seconds=primary_result.total_seconds,
            queries=[q.to_dict() for q in primary_result.queries],
            iterations=primary_result.iterations,
            streams=primary_result.streams,
            stream_results=[s.to_dict() for s in primary_result.stream_results],
        )
        latest_run.benchmark = bench_metrics
        storage.save_run(latest_run)
        print_info(f"Benchmark metrics appended to run {latest_run.run_id}")

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
