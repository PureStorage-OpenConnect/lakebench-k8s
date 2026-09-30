"""The ``benchmark --class`` help lists every query class that exists.

The runner filters by exact match on ``BenchmarkQuery.query_class``, so a
class named in the help but absent from the query sets (the old ``gold``)
silently runs nothing.
"""

import re

import typer

from lakebench.benchmark.queries import BENCHMARK_QUERIES_BY_DOMAIN
from lakebench.cli._query import benchmark


def _class_help() -> str:
    # Resolve through Typer's own command build: typing.get_type_hints on
    # Python 3.10 wraps an Annotated hint with a None default in Optional and
    # drops the typer.Option metadata.
    app = typer.Typer()
    app.command()(benchmark)
    cmd = typer.main.get_command(app)
    for param in cmd.params:
        if "--class" in getattr(param, "opts", []):
            assert param.help, "--class option has no help text"
            return param.help
    raise AssertionError("benchmark has no --class option")


def test_class_help_names_only_real_classes():
    real = {q.query_class for qs in BENCHMARK_QUERIES_BY_DOMAIN.values() for q in qs}
    named = set(re.findall(r"[a-z_]+", _class_help().split("(", 1)[1]))
    named -= {"aml", "also"}
    assert real <= named, f"help omits {sorted(real - named)}"
    assert named <= real, f"help names classes with no queries: {sorted(named - real)}"
