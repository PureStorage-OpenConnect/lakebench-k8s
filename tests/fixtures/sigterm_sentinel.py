"""A sentinel SIGTERM handler for tests that send real signals to this process.

Import ``sentinel_sigterm`` into a test module: it is autouse, so a change
that stops handling SIGTERM fails the test instead of killing the test
process, and both handlers are restored afterwards.
"""

from __future__ import annotations

import signal

import pytest


class SentinelSigterm(Exception):
    """SIGTERM reached the handler that was installed before the run."""


@pytest.fixture(autouse=True)
def sentinel_sigterm():
    def handler(signum, frame):
        raise SentinelSigterm

    previous = signal.signal(signal.SIGTERM, handler)
    previous_int = signal.getsignal(signal.SIGINT)
    try:
        yield handler
    finally:
        signal.signal(signal.SIGTERM, previous)
        signal.signal(signal.SIGINT, previous_int)
