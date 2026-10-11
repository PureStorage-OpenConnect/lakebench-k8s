"""Leave the process without interpreter teardown.

Background daemon threads (the driver log capturer, the system sampler) can
be frozen at interpreter shutdown while holding the logging module's lock;
teardown then garbage-collects a handler, whose weakref callback waits on
that lock forever. A finished ``lakebench run`` hung that way for hours
after writing its record, so the next step of a script (destroy) never ran.
Lakebench registers no exit handlers of its own, so after the command has
returned and its output is flushed nothing is lost by skipping teardown.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable


def exit_without_teardown(code: int) -> None:
    """Flush stdout and stderr, then end the process with *code*."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:  # noqa: BLE001 -- a closed stream must not block the exit
            pass
    os._exit(code)


def run_and_exit(app: Callable[[], object]) -> None:
    """Run the CLI *app* and exit with its status, skipping teardown.

    ``SystemExit`` carries the status as Click and Python do: None is 0, an
    int is itself, any other value is printed to stderr and exits 1. Any
    other exception propagates with its traceback, as before.
    """
    try:
        app()
        code = 0
    except SystemExit as e:
        if e.code is None:
            code = 0
        elif isinstance(e.code, int):
            code = e.code
        else:
            print(e.code, file=sys.stderr)
            code = 1
    exit_without_teardown(code)
