"""Why a config is being loaded, and the notes a load collects.

``load_config`` (``config/loader.py``) takes a :class:`LoadPurpose` and passes
it to the schema validators in the Pydantic validation context. The purpose
decides three things:

- a removed key is refused (MUTATE, RUN) or dropped with a note (the
  others), so a v1.6 config can still be destroyed and inspected;
- the derived-name length check is skipped (TEARDOWN, READ, INSPECT);
- a config with no ``name`` is refused (MUTATE, RUN), refused when its name
  would be only a suggestion (TEARDOWN) or would come from the v1.6
  ``.lakebench/state.json`` (TEARDOWN, READ), and otherwise loads under the
  name ``deploy_state.resolve_name`` resolves.

Deprecations, dropped keys and dead fields are collected into
:class:`LoadNotes` while a load runs, instead of each validator logging on its
own, and ``load_config`` prints them once as one block. This module imports
nothing from the schema, so the schema can import it.
"""

from __future__ import annotations

import logging
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger("lakebench.config.schema")


class LoadPurpose(str, Enum):
    """What the command that loads the config is going to do with it."""

    MUTATE = "mutate"  # deploy, generate, benchmark, query, clean, financial, reproduce
    RUN = "run"  # run (and the perf gate's pinned configs): MUTATE plus run-only refusals
    TEARDOWN = "teardown"  # destroy, stop, admin
    READ = "read"  # status, logs, report, results: read about a deployment
    COMPARE = "compare"  # compare's config resolution (read-only compare)
    INSPECT = "inspect"  # config show, info, config storage/recommend/upgrade: no deployment


#: Purposes under which a config may change data: removed keys and a
#: missing name are refused.
CHANGES_DATA = frozenset({LoadPurpose.MUTATE, LoadPurpose.RUN})

#: Purposes that skip the derived-name length check, so a
#: deployment whose name is too long to finish deploying can be inspected
#: and torn down.
SKIPS_NAME_LENGTH = frozenset({LoadPurpose.TEARDOWN, LoadPurpose.READ, LoadPurpose.INSPECT})

#: Purposes that act on or report a deployment by the config's name. They
#: refuse a nameless config whose name would come from the v1.6
#: ``.lakebench/state.json``, because v1.6 gave every nameless config in the
#: directory that name. INSPECT loads it: it only reads the
#: file.
TARGETS_DEPLOYMENT = frozenset({LoadPurpose.TEARDOWN, LoadPurpose.READ})


def purpose_from_context(context: object) -> LoadPurpose | None:
    """The purpose in a Pydantic validation context, or None outside load_config."""
    if not isinstance(context, dict):
        return None
    value = context.get("purpose")
    if value is None:
        return None
    return LoadPurpose(value)


@dataclass(frozen=True)
class LoadNote:
    """One line of the notes block: ``kind`` is removed, dead or deprecated."""

    kind: str
    text: str


class LoadNotes(list[LoadNote]):
    """The notes one ``load_config`` call collected, in the order they arose."""

    def add(self, kind: str, text: str) -> None:
        note = LoadNote(kind, text)
        if note not in self:
            self.append(note)

    def texts(self) -> list[str]:
        return [n.text for n in self]


_ACTIVE: ContextVar[LoadNotes | None] = ContextVar("lakebench_load_notes", default=None)


@contextmanager
def collecting_notes() -> Iterator[LoadNotes]:
    """Collect every note emitted while the block runs."""
    notes = LoadNotes()
    token = _ACTIVE.set(notes)
    try:
        yield notes
    finally:
        _ACTIVE.reset(token)


def emit_note(
    text: str,
    *,
    kind: str = "deprecated",
    category: type[Warning] | None = DeprecationWarning,
) -> None:
    """Record a load note.

    A DeprecationWarning is raised as well, so programmatic callers and tests
    still see it; Python does not print one raised from inside lakebench, so
    the CLI shows the note once, in the block ``load_config`` prints. Pass
    ``category=None`` for a note with no Python warning: a UserWarning would
    be printed a second time. Outside ``load_config`` (a model built
    directly) the note is logged.
    """
    if category is not None:
        warnings.warn(text, category, stacklevel=3)
    active = _ACTIVE.get()
    if active is not None:
        active.add(kind, text)
    else:
        logger.warning(text)
