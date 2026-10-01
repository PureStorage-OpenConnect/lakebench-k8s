"""The deploy timeout inside every wait (DEP-6, ch01 s8).

``deploy_all`` used to check ``--timeout`` only between steps, while each
wait inside a step kept its own timeout (Thrift 300 s, DuckDB 900 s, Polaris
600 s plus 600 s, operator rollouts), so a deploy could run far past the
timeout the user asked for. ``deploy_all`` now calls :func:`start`; every
wait bounds its own timeout with :func:`clamp`, and a wait that the deploy
deadline (not its own timeout) cut short raises :class:`DeployTimeout`,
which names the component and what it was waiting for.

Outside ``deploy`` (destroy, ``run``, admin) no deadline is set, ``clamp``
returns its argument unchanged and ``check`` never raises.

The deadline lives in a ``contextvars.ContextVar``, so it is per thread and
per context: a concurrent caller in another thread never sees it.
"""

from __future__ import annotations

import math
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass


@dataclass(frozen=True)
class _Deadline:
    started: float  # time.monotonic()
    timeout: float  # seconds the user allowed
    at: float  # started + timeout


DEPLOY_DEADLINE: ContextVar[_Deadline | None] = ContextVar(
    "lakebench_deploy_deadline", default=None
)
# A wait the deadline cut short ends on its own clock a moment before the
# deadline; check() treats "within this many seconds of it" as passed.
_SLACK_S = 1.0
# The deploy step running now, so a wait deep inside a step can name it.
CURRENT_COMPONENT: ContextVar[str] = ContextVar("lakebench_deploy_component", default="")


class DeployTimeout(Exception):
    """The deploy deadline passed while a step was waiting.

    Not a ``TimeoutError``: ``deploy_all`` retries ``TimeoutError`` as
    transient, and an exhausted deadline must never be retried.
    """

    def __init__(
        self,
        component: str,
        waiting_for: str,
        elapsed: float,
        timeout: float,
        last_state: str = "",
    ) -> None:
        self.component = component
        self.waiting_for = waiting_for
        self.elapsed = elapsed
        self.timeout = timeout
        self.last_state = last_state
        state = f" ({last_state})" if last_state else ""
        super().__init__(
            f"deploy timeout ({int(timeout)} s) reached after {int(elapsed)} s "
            f"while waiting for {component or 'deploy'}: {waiting_for}{state}"
        )


def start(timeout: float) -> Token:
    """Set the deadline ``timeout`` seconds from now (``<= 0``: none).
    Returns the token for :func:`reset`."""
    if timeout and timeout > 0:
        now = time.monotonic()
        return DEPLOY_DEADLINE.set(_Deadline(now, float(timeout), now + float(timeout)))
    return DEPLOY_DEADLINE.set(None)


def reset(token: Token) -> None:
    DEPLOY_DEADLINE.reset(token)


@contextmanager
def deploy_deadline(timeout: float) -> Iterator[None]:
    token = start(timeout)
    try:
        yield
    finally:
        reset(token)


@contextmanager
def component(name: str) -> Iterator[None]:
    """Name the step that the waits below belong to."""
    token = CURRENT_COMPONENT.set(name)
    try:
        yield
    finally:
        CURRENT_COMPONENT.reset(token)


def remaining() -> float | None:
    """Seconds left before the deploy deadline (never negative), or None."""
    d = DEPLOY_DEADLINE.get()
    if d is None:
        return None
    return max(0.0, d.at - time.monotonic())


def expired() -> bool:
    r = remaining()
    return r is not None and r <= _SLACK_S


def clamp(t: float) -> float:
    """``min(t, remaining)``; ``t`` unchanged when no deadline is set."""
    r = remaining()
    return t if r is None else min(t, r)


def clamp_whole_seconds(t: float, waiting_for: str) -> int:
    """``clamp`` for a tool that takes whole seconds (``kubectl --timeout``,
    ``helm --timeout``), where 0 means "wait forever": raises
    :class:`DeployTimeout` instead of passing 0."""
    check(waiting_for)
    return max(1, math.ceil(clamp(t)))


def check(waiting_for: str, last_state: str = "", component_name: str | None = None) -> None:
    """Raise :class:`DeployTimeout` when the deploy deadline has passed."""
    d = DEPLOY_DEADLINE.get()
    if d is None or time.monotonic() < d.at - _SLACK_S:
        return
    raise DeployTimeout(
        component_name if component_name is not None else CURRENT_COMPONENT.get(),
        waiting_for,
        time.monotonic() - d.started,
        d.timeout,
        last_state,
    )
