"""Interactive init wizard for Lakebench.

Multi-step guided setup that collects configuration values through
Rich-formatted panels with inline validation and back-navigation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

logger = logging.getLogger(__name__)


class _BackSentinel:
    """Sentinel to signal 'go back one step'."""


# Sentinel to signal "go back one step"
_BACK = _BackSentinel()


@dataclass
class WizardState:
    """Mutable state accumulated across wizard steps."""

    name: str = "my-lakehouse"
    namespace: str = ""
    recipe: str = ""
    endpoint: str = ""
    access_key: str = ""
    secret_key: str = ""
    region: str = "us-east-1"
    scale: int = 10
    mode: str = "batch"
    cycles: int = 1
    # Filled during review
    config_yaml: str = ""


def _prompt(
    console: Console,
    label: str,
    default: str = "",
    hide_input: bool = False,
    validator: Any = None,
    hint: str = "",
) -> str | _BackSentinel:
    """Prompt with back-navigation support.

    Returns the input string, or ``_BACK`` if the user types 'back' or 'b'.
    """
    import typer

    if hint:
        console.print(f"  [dim]{hint}[/dim]")

    suffix = f" [dim](default: {default})[/dim]" if default else ""
    back_hint = "[dim](type 'back' to go back)[/dim]"
    console.print(f"  {back_hint}")

    raw = typer.prompt(
        f"  {label}{suffix}", default=default, show_default=False, hide_input=hide_input
    )

    if isinstance(raw, str) and raw.strip().lower() in ("back", "b"):
        return _BACK

    if validator:
        err = validator(raw)
        if err:
            console.print(f"  [red]{err}[/red]")
            return _prompt(console, label, default, hide_input, validator, hint="")

    return raw


def _prompt_int(
    console: Console,
    label: str,
    default: int = 0,
    min_val: int | None = None,
    max_val: int | None = None,
) -> int | _BackSentinel:
    """Prompt for an integer with bounds checking and back support."""
    import typer

    console.print("  [dim](type 'back' to go back)[/dim]")
    raw = typer.prompt(f"  {label}", default=str(default), show_default=False)

    if isinstance(raw, str) and raw.strip().lower() in ("back", "b"):
        return _BACK

    try:
        val = int(raw)
    except ValueError:
        console.print("  [red]Please enter a number[/red]")
        return _prompt_int(console, label, default, min_val, max_val)

    if min_val is not None and val < min_val:
        console.print(f"  [red]Minimum is {min_val}[/red]")
        return _prompt_int(console, label, default, min_val, max_val)
    if max_val is not None and val > max_val:
        console.print(f"  [red]Maximum is {max_val}[/red]")
        return _prompt_int(console, label, default, min_val, max_val)

    return val


def _step_header(console: Console, step: int, total: int, title: str) -> None:
    """Print a step header."""
    console.print()
    console.print(
        Panel(
            f"[bold]Step {step}/{total}:[/bold] {title}",
            border_style="cyan",
            expand=False,
        )
    )


# ---------------------------------------------------------------------------
# Step 1: Deployment Identity
# ---------------------------------------------------------------------------


def step_identity(console: Console, state: WizardState) -> bool:
    """Collect deployment name and namespace. Returns False to go back."""
    _step_header(console, 1, 5, "Deployment Identity")

    result = _prompt(
        console,
        "Deployment name",
        default=state.name,
        hint="Unique name for this lakehouse deployment",
    )
    if isinstance(result, _BackSentinel):
        return False
    state.name = result

    result = _prompt(
        console,
        "Kubernetes namespace",
        default=state.namespace or state.name,
        hint="K8s namespace (defaults to deployment name)",
    )
    if isinstance(result, _BackSentinel):
        return False
    state.namespace = result

    return True


# ---------------------------------------------------------------------------
# Step 2: Architecture Recipe
# ---------------------------------------------------------------------------


def step_recipe(console: Console, state: WizardState) -> bool:
    """Select architecture recipe. Returns False to go back."""
    from lakebench.config.recipes import RECIPE_DESCRIPTIONS

    _step_header(console, 2, 5, "Architecture Recipe")
    console.print()

    table = Table(show_header=True, header_style="bold", expand=False, padding=(0, 2))
    table.add_column("#", style="cyan", width=3)
    table.add_column("Recipe", min_width=32)
    table.add_column("Components")

    choices = list(RECIPE_DESCRIPTIONS.items())
    for i, (rname, desc) in enumerate(choices, 1):
        marker = " [green]*[/green]" if rname == (state.recipe or "default") else ""
        table.add_row(str(i), f"{rname}{marker}", desc)

    console.print(table)
    console.print()
    console.print("  [dim]The default recipe (Hive + Iceberg + Spark + Trino) is recommended[/dim]")
    console.print(
        "  [dim]for most users. Polaris recipes use REST catalog instead of Thrift.[/dim]"
    )

    result = _prompt_int(console, "Recipe number", default=1, min_val=1, max_val=len(choices))
    if isinstance(result, _BackSentinel):
        return False

    state.recipe = choices[result - 1][0]
    console.print(f"  [green]Selected:[/green] {state.recipe}")
    return True


# ---------------------------------------------------------------------------
# Step 3: Storage (S3)
# ---------------------------------------------------------------------------


def step_storage(console: Console, state: WizardState) -> bool:
    """Collect S3 credentials and optionally validate connectivity."""
    _step_header(console, 3, 5, "Object Storage (S3)")

    console.print()
    console.print("  [dim]Examples:[/dim]")
    console.print("  [dim]  FlashBlade:  http://10.0.0.1:80 or https://fb.example.com:443[/dim]")
    console.print("  [dim]  MinIO:       http://minio.default.svc:9000[/dim]")
    console.print("  [dim]  AWS S3:      https://s3.us-east-1.amazonaws.com[/dim]")
    console.print()

    result = _prompt(
        console,
        "S3 endpoint URL",
        default=state.endpoint,
        validator=_validate_endpoint,
    )
    if isinstance(result, _BackSentinel):
        return False
    state.endpoint = result

    result = _prompt(console, "S3 access key", default=state.access_key)
    if isinstance(result, _BackSentinel):
        return False
    state.access_key = result

    result = _prompt(
        console,
        "S3 secret key",
        default=state.secret_key if state.secret_key else "",
        hide_input=True,
    )
    if isinstance(result, _BackSentinel):
        return False
    state.secret_key = result

    result = _prompt(console, "S3 region", default=state.region)
    if isinstance(result, _BackSentinel):
        return False
    state.region = result

    # Inline connectivity test
    if state.endpoint and state.access_key and state.secret_key:
        _test_s3(console, state)

    return True


def _validate_endpoint(value: str) -> str | None:
    """Return error string if endpoint looks invalid, None if OK."""
    v = value.strip()
    if not v:
        return "Endpoint cannot be empty"
    if not v.startswith("http://") and not v.startswith("https://"):
        return "Endpoint must start with http:// or https://"
    return None


def _test_s3(console: Console, state: WizardState) -> None:
    """Quick S3 connectivity check -- non-blocking."""
    console.print()
    console.print("  Testing S3 connectivity...", end=" ")
    try:
        from lakebench.s3 import test_s3_connectivity

        result = test_s3_connectivity(
            endpoint=state.endpoint,
            access_key=state.access_key,
            secret_key=state.secret_key,
            region=state.region,
        )
        if result.get("reachable"):
            console.print("[green]OK[/green]")
            bucket_count = result.get("bucket_count", "?")
            console.print(f"  [dim]Found {bucket_count} existing bucket(s)[/dim]")
        else:
            err = result.get("error", "unknown error")
            console.print(f"[yellow]WARN[/yellow] {err}")
            console.print("  [dim]You can continue and fix credentials later[/dim]")
    except Exception as e:
        console.print(f"[yellow]WARN[/yellow] {e}")
        console.print("  [dim]Connectivity test failed -- you can continue and fix later[/dim]")


# ---------------------------------------------------------------------------
# Step 4: Workload Profile
# ---------------------------------------------------------------------------


def step_workload(console: Console, state: WizardState) -> bool:
    """Configure scale, pipeline mode, and cycles."""
    _step_header(console, 4, 5, "Workload Profile")

    console.print()
    scale_table = Table(show_header=True, header_style="bold", expand=False, padding=(0, 2))
    scale_table.add_column("Scale", style="cyan", width=8)
    scale_table.add_column("Bronze Data", width=12)
    scale_table.add_column("Use Case")
    scale_table.add_row("1", "~10 GB", "Quick smoke test")
    scale_table.add_row("10", "~100 GB", "Development / CI")
    scale_table.add_row("100", "~1 TB", "Production baseline")
    scale_table.add_row("1000", "~10 TB", "Large-scale stress test")
    console.print(scale_table)
    console.print()

    result = _prompt_int(console, "Scale factor", default=state.scale, min_val=1, max_val=10000)
    if isinstance(result, _BackSentinel):
        return False
    state.scale = result

    # Pipeline mode
    console.print()
    console.print("  [bold]Pipeline mode:[/bold]")
    console.print(
        "    [cyan]1[/cyan]. batch      Single end-to-end run (bronze -> silver -> gold -> benchmark)"
    )
    console.print(
        "    [cyan]2[/cyan]. sustained   Continuous streaming with periodic benchmark rounds"
    )
    console.print()

    current_default = 1 if state.mode == "batch" else 2
    result = _prompt_int(console, "Pipeline mode", default=current_default, min_val=1, max_val=2)
    if isinstance(result, _BackSentinel):
        return False
    state.mode = "batch" if result == 1 else "sustained"

    # Cycles (batch only)
    if state.mode == "batch":
        console.print()
        console.print("  [dim]Batch cycles: 1 = single run (day 1 behavior).[/dim]")
        console.print("  [dim]  2+ = iterative: cycle 1 overwrites, cycles 2-N append/merge.[/dim]")
        console.print(
            "  [dim]  Simulates multi-day lakehouse accumulation and QpH degradation.[/dim]"
        )
        result = _prompt_int(console, "Batch cycles", default=state.cycles, min_val=1, max_val=50)
        if isinstance(result, _BackSentinel):
            return False
        state.cycles = result
    else:
        state.cycles = 1

    return True


# ---------------------------------------------------------------------------
# Step 5: Review & Write
# ---------------------------------------------------------------------------


def step_review(console: Console, state: WizardState) -> bool:
    """Show config preview and confirm."""
    _step_header(console, 5, 5, "Review Configuration")

    # Build summary table
    console.print()
    summary = Table(show_header=False, expand=False, padding=(0, 2), show_edge=False)
    summary.add_column("Field", style="bold", width=20)
    summary.add_column("Value")

    summary.add_row("Name", state.name)
    summary.add_row("Namespace", state.namespace or state.name)
    summary.add_row("Recipe", state.recipe or "default")
    summary.add_row("S3 Endpoint", state.endpoint or "[red]not set[/red]")
    summary.add_row("S3 Access Key", state.access_key or "[red]not set[/red]")
    summary.add_row("S3 Secret Key", "***" if state.secret_key else "[red]not set[/red]")
    summary.add_row("Region", state.region)
    summary.add_row("Scale", f"{state.scale} (~{state.scale * 10} GB)")
    summary.add_row("Mode", state.mode)
    if state.mode == "batch" and state.cycles > 1:
        summary.add_row("Cycles", str(state.cycles))

    console.print(summary)

    # Generate and preview YAML
    state.config_yaml = _build_config_yaml(state)

    console.print()
    syntax = Syntax(state.config_yaml, "yaml", theme="monokai", line_numbers=False)
    console.print(
        Panel(syntax, title="Generated Configuration", border_style="green", expand=False)
    )

    return True


def _build_config_yaml(state: WizardState) -> str:
    """Generate config YAML from wizard state."""
    import re

    from lakebench.config import generate_example_config_yaml

    content = generate_example_config_yaml()
    content = content.replace("name: my-lakehouse", f"name: {state.name}")
    content = re.sub(r"scale:\s*\d+", f"scale: {state.scale}", content)

    if state.recipe and state.recipe != "default":
        content = content.replace(
            f"name: {state.name}",
            f"name: {state.name}\nrecipe: {state.recipe}",
        )

    if state.endpoint:
        content = content.replace('endpoint: ""', f'endpoint: "{state.endpoint}"', 1)
    if state.access_key:
        content = content.replace('access_key: ""', f'access_key: "{state.access_key}"')
    if state.secret_key:
        content = content.replace('secret_key: ""', f'secret_key: "{state.secret_key}"')

    ns = state.namespace or state.name
    content = content.replace('namespace: ""', f'namespace: "{ns}"', 1)

    if state.region and state.region != "us-east-1":
        content = content.replace('# region: "us-east-1"', f'region: "{state.region}"')

    # Pipeline mode and cycles
    mode_line = "mode: batch"
    if state.mode == "sustained":
        content = content.replace(mode_line, "mode: sustained", 1)
    elif state.cycles > 1:
        content = content.replace(mode_line, f"mode: batch\n    cycles: {state.cycles}", 1)

    return content


# ---------------------------------------------------------------------------
# Wizard runner
# ---------------------------------------------------------------------------


STEPS = [step_identity, step_recipe, step_storage, step_workload, step_review]


def run_wizard(console: Console) -> WizardState | None:
    """Run the multi-step init wizard.

    Returns the completed WizardState, or None if the user aborts (Ctrl+C).
    """
    state = WizardState()

    console.print()
    console.print(
        Panel(
            Text.from_markup(
                "[bold cyan]Lakebench Configuration Wizard[/bold cyan]\n\n"
                "This wizard will guide you through creating a lakebench.yaml file.\n"
                "Type [bold]'back'[/bold] at any prompt to return to the previous step.\n"
                "Press [bold]Ctrl+C[/bold] to cancel."
            ),
            expand=False,
        )
    )

    step_idx = 0
    while step_idx < len(STEPS):
        try:
            ok = STEPS[step_idx](console, state)
        except (KeyboardInterrupt, EOFError):
            console.print("\n[yellow]Cancelled.[/yellow]")
            return None

        if ok:
            step_idx += 1
        else:
            # Go back
            if step_idx > 0:
                step_idx -= 1
            else:
                console.print("  [dim]Already at the first step[/dim]")

    return state
