"""Release harness state: the row log and the markdown deployments ledger.

Two stores, both written only by ``harness.py``:

``RowLog``
    ``<out>/rows.jsonl``, append-only, one JSON object per row transition,
    each append flushed and fsynced. The harness holds an exclusive lock on
    ``<out>/harness.lock`` for its whole life, so two harness processes never
    share an ``--out``. ``latest()`` folds the lines into one state per row;
    a later line's fields override an earlier one's, so ``incarnation``,
    ``run_ids`` and the like survive the transitions that do not repeat them.

``MarkdownLedger``
    The deployments table in the evidence file (CLAUDE.md section 8: the
    authority for "your own deployment"). Every write is defensive: under an
    exclusive lock (``--ledger-lock``; pass the lock the other writers of the
    file use, or ``<ledger>.lock`` by default) it reads the file, keeps a byte
    copy under ``<out>/ledger-backups/``, inserts or replaces exactly one
    line, checks that every other line survived in order, writes a temporary
    file and replaces the ledger only if its size and mtime are still those
    it read (otherwise it re-reads and retries), then reads the file again
    and refuses if its own change is not there. It refuses when the table
    header is missing or the file is less than 75% of the newest backup's
    size (a truncated read). A removed row is replaced by a ``closed <namespace> <utc>
    destroy DONE`` line, the convention the table already uses. The lock is
    reentrant within this process (``transaction``), so admission can read
    the ledger and add its row under one lock.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import shutil
import tempfile
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

#: Every row status. ``TERMINAL`` rows are never acted on again.
STATUSES = (
    "planned",
    "ledgered",
    "deploying",
    "deployed",
    "running",
    "recorded",
    "destroying",
    "destroyed",
    "failed",
    "destroy-refused",
    "left",
    "not-deployed",
)
TERMINAL = frozenset({"destroyed", "failed", "destroy-refused", "left", "not-deployed"})

LEDGER_HEADER = "| Namespace | Config path | Session or lane | Scale | Deployed |"


class LedgerError(Exception):
    """The ledger or the row log cannot be written safely."""


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# -- row log -----------------------------------------------------------------


class RowLog:
    """``<out>/rows.jsonl`` with a process-lifetime lock on ``<out>``."""

    def __init__(self, out: Path) -> None:
        self.out = out
        self.path = out / "rows.jsonl"
        self._lock_fh: Any = None

    def acquire(self) -> None:
        """Take the ``<out>`` lock for this process, or raise LedgerError."""
        self.out.mkdir(parents=True, exist_ok=True)
        fh = open(self.out / "harness.lock", "a+")  # noqa: SIM115 -- held for the process life
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            fh.close()
            raise LedgerError(
                f"another harness process holds {self.out / 'harness.lock'}; one harness per --out"
            ) from e
        self._lock_fh = fh

    def release(self) -> None:
        if self._lock_fh is not None:
            fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_UN)
            self._lock_fh.close()
            self._lock_fh = None

    def append(self, row: str, status: str, **fields: Any) -> dict[str, Any]:
        """Append one transition; returns the line written."""
        if status not in STATUSES:
            raise LedgerError(f"unknown row status {status!r}")
        if self._lock_fh is None:
            raise LedgerError("the row log is written only while the harness lock is held")
        entry: dict[str, Any] = {"row": row, "status": status, "utc": utc_now(), **fields}
        line = json.dumps(entry, sort_keys=True) + "\n"
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(line)
            fh.flush()
            os.fsync(fh.fileno())
        return entry

    def entries(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        out = []
        for n, line in enumerate(self.path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except ValueError as e:
                raise LedgerError(f"{self.path}:{n} is not JSON: {e}") from e
            if not isinstance(entry, dict) or "row" not in entry or "status" not in entry:
                raise LedgerError(f"{self.path}:{n} is not a row transition")
            out.append(entry)
        return out

    def latest(self) -> dict[str, dict[str, Any]]:
        """{row id: folded state}, in first-seen order."""
        folded: dict[str, dict[str, Any]] = {}
        for entry in self.entries():
            state = folded.setdefault(entry["row"], {})
            state.update(entry)
        return folded


# -- markdown ledger ---------------------------------------------------------


@dataclass(frozen=True)
class LedgerRow:
    namespace: str
    config: str
    session: str
    scale: str
    deployed: str

    def line(self) -> str:
        cells = (self.namespace, self.config, self.session, self.scale, self.deployed)
        for c in cells:
            if "|" in c or "\n" in c:
                raise LedgerError(f"ledger cell {c!r} holds a pipe or a newline")
        return "| " + " | ".join(cells) + " |"


def _table_span(lines: list[str]) -> tuple[int, int]:
    """(header index, index after the table's last line).

    The table runs from the header through the separator and every following
    line that starts with ``|`` or ``closed `` (the closed-row convention);
    it ends at the first other line.
    """
    try:
        h = lines.index(LEDGER_HEADER)
    except ValueError as e:
        raise LedgerError(f"the ledger has no deployments table header ({LEDGER_HEADER})") from e
    i = h + 1
    while i < len(lines) and (lines[i].startswith("|") or lines[i].startswith("closed ")):
        i += 1
    return h, i


def _first_cell(line: str) -> str | None:
    if not line.startswith("|"):
        return None
    parts = line.split("|")
    return parts[1].strip() if len(parts) > 2 else None


class MarkdownLedger:
    """The deployments table of the evidence file."""

    RETRIES = 5
    #: A ledger smaller than this share of its newest backup is refused as
    #: truncated (a half-written file); ordinary edits never shrink it so far.
    SHRINK_FLOOR = 0.75

    def __init__(self, path: Path, backups: Path, lock_path: Path | None = None) -> None:
        # A symlinked ledger is edited at its target, never replaced by a copy.
        self.path = path.resolve()
        self.backups = backups
        self.lock_path = lock_path or self.path.with_name(self.path.name + ".lock")
        self._tlock = threading.RLock()
        self._depth = 0
        self._fh: Any = None

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Hold the ledger lock across several reads and writes."""
        with self._locked():
            yield

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        with self._tlock:
            if self._depth == 0:
                fh = open(self.lock_path, "a+")  # noqa: SIM115 -- closed on release
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                self._fh = fh
            self._depth += 1
            try:
                yield
            finally:
                self._depth -= 1
                if self._depth == 0:
                    fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
                    self._fh.close()
                    self._fh = None

    def _lines(self) -> list[str]:
        return self.path.read_bytes().decode("utf-8").split("\n")

    def live_rows(self) -> list[LedgerRow]:
        """The table's pipe rows below the separator (not the closed lines)."""
        with self._locked():
            lines = self._lines()
        h, end = _table_span(lines)
        rows = []
        for line in lines[h + 2 : end]:
            if not line.startswith("|"):
                continue
            parts = [p.strip() for p in line.split("|")[1:-1]]
            if len(parts) != 5 or not parts[0] or set(parts[0]) <= {"-"}:
                continue
            rows.append(LedgerRow(*parts))
        return rows

    def has_row(self, namespace: str) -> bool:
        return namespace in self.live_namespaces()

    def live_namespaces(self) -> set[str]:
        return {r.namespace for r in self.live_rows()}

    def add(self, row: LedgerRow) -> None:
        """Insert *row* as the table's last line."""

        def edit(lines: list[str]) -> list[str]:
            h, end = _table_span(lines)
            if any(_first_cell(ln) == row.namespace for ln in lines[h + 2 : end]):
                raise LedgerError(f"the ledger already has a row for {row.namespace}")
            return [*lines[:end], row.line(), *lines[end:]]

        self._rewrite(edit, inserted=1, expect=row.line())

    def close(self, namespace: str, when: str | None = None) -> None:
        """Replace *namespace*'s row with ``closed <ns> <utc> destroy DONE``."""
        closed = f"closed {namespace} {when or utc_now()} destroy DONE"

        def edit(lines: list[str]) -> list[str]:
            h, end = _table_span(lines)
            hits = [i for i in range(h + 2, end) if _first_cell(lines[i]) == namespace]
            if len(hits) != 1:
                raise LedgerError(
                    f"expected one ledger row for {namespace}, found {len(hits)}; nothing changed"
                )
            out = list(lines)
            out[hits[0]] = closed
            return out

        self._rewrite(edit, inserted=0, expect=closed)

    def _rewrite(self, edit: Any, *, inserted: int, expect: str) -> None:
        with self._locked():
            for _attempt in range(self.RETRIES):
                st = self.path.stat()
                raw = self.path.read_bytes()
                floor = self._newest_backup_size()
                if floor is not None and st.st_size < floor * self.SHRINK_FLOOR:
                    raise LedgerError(
                        f"{self.path} is {st.st_size} bytes, less than "
                        f"{self.SHRINK_FLOOR:.0%} of its newest backup ({floor}); "
                        "refusing to write over a truncated ledger"
                    )
                lines = raw.decode("utf-8").split("\n")
                new = edit(lines)
                _check_preserved(lines, new, inserted)
                self._backup(raw)
                body = "\n".join(new)
                fd, tmp = tempfile.mkstemp(prefix=".ledger-", dir=str(self.path.parent))
                try:
                    with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
                        fh.write(body)
                        fh.flush()
                        os.fsync(fh.fileno())
                    shutil.copymode(self.path, tmp)
                    now = self.path.stat()
                    if (now.st_size, now.st_mtime_ns) != (st.st_size, st.st_mtime_ns):
                        os.unlink(tmp)
                        time.sleep(0.2)
                        continue  # edited under us: read again
                    os.replace(tmp, self.path)
                except BaseException:
                    with contextlib.suppress(FileNotFoundError):
                        os.unlink(tmp)
                    raise
                if expect not in self._lines():
                    raise LedgerError(
                        f"{self.path} lost the harness's change right after it was written "
                        "(another writer without the lock?); nothing more is done"
                    )
                return
            raise LedgerError(f"{self.path} kept changing under the harness; nothing written")

    def _newest_backup_size(self) -> int | None:
        try:
            backups = sorted(self.backups.glob(f"{self.path.name}.*"))
        except OSError:
            return None
        return backups[-1].stat().st_size if backups else None

    def _backup(self, raw: bytes) -> None:
        self.backups.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        (self.backups / f"{self.path.name}.{stamp}").write_bytes(raw)


def _check_preserved(old: list[str], new: list[str], inserted: int) -> None:
    """*new* is *old* with exactly *inserted* added lines, or (inserted 0)
    exactly one replaced line; anything else is refused."""
    if inserted:
        if len(new) != len(old) + inserted:
            raise LedgerError("ledger edit changed more than the inserted row")
        it = iter(new)
        if not all(any(line == n for n in it) for line in old):
            raise LedgerError("ledger edit lost or reordered existing lines")
        return
    if len(new) != len(old) or sum(a != b for a, b in zip(old, new, strict=True)) != 1:
        raise LedgerError("ledger edit changed more than the closed row")
