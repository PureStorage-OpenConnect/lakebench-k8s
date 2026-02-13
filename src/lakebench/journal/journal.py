"""Journal manager -- append-only provenance tracking for Lakebench.

Provides session-scoped event recording across CLI commands. Each session
is stored as a JSONL file (one JSON object per line), enabling crash-safe
append-only writes and easy inspection with standard tools.

Sessions are keyed by the deployment name (config ``name`` field), not by
config file hash. This means deploy, generate, run, and destroy for the
same deployment all append to a single journal file -- regardless of config
edits between commands. The config hash is still recorded per-event for
drift detection.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from lakebench import __version__
from lakebench._constants import DEFAULT_OUTPUT_DIR

from .events import CommandName, EventType, JournalEvent

logger = logging.getLogger(__name__)

DEFAULT_JOURNAL_DIR = str(Path(DEFAULT_OUTPUT_DIR) / "journal")


def _generate_session_id() -> str:
    """Generate session ID matching existing run_id format."""
    return datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]


def _sanitize_name(name: str) -> str:
    """Sanitize a config name for use as a filename component."""
    return re.sub(r"[^a-zA-Z0-9_-]", "_", name) if name else "unnamed"


def _hash_config_file(config_path: Path) -> str:
    """SHA256 hash (truncated) of config file for drift detection."""
    try:
        return hashlib.sha256(config_path.read_bytes()).hexdigest()[:16]
    except OSError:
        return "unknown"


class Journal:
    """Append-only journal for command and execution provenance.

    Sessions are keyed by deployment name (the ``name`` field in config).
    All commands for the same deployment (deploy, generate, run, destroy)
    append to a single session file. A session is only closed by destroy
    or clean data -- after which a new deploy starts a fresh session.

    Usage::

        journal = Journal()
        journal.open_session(config_path=Path("lakebench.yaml"), config_name="my-bench")
        journal.begin_command(CommandName.DEPLOY, {"dry_run": False})
        journal.record(EventType.DEPLOY_COMPONENT, "Namespace created", ...)
        journal.end_command(success=True)
    """

    def __init__(self, journal_dir: Path | str = DEFAULT_JOURNAL_DIR) -> None:
        self.journal_dir = Path(journal_dir)
        self.session_id: str | None = None
        self._session_file: Path | None = None
        self._current_command: CommandName | None = None
        self._command_start_time: datetime | None = None
        self._event_count: int = 0
        self._command_count: int = 0

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def open_session(
        self,
        config_path: Path | None = None,
        config_name: str = "",
    ) -> str:
        """Open or resume a session.

        Sessions are keyed by config_name (deployment name). If a session
        file for this name exists and is not closed, resume it. Otherwise
        start a new session. Config hash is recorded per-event for drift
        detection but does not affect session identity.

        Args:
            config_path: Path to config file (for hashing)
            config_name: Deployment name from config

        Returns:
            The session_id
        """
        self.journal_dir.mkdir(parents=True, exist_ok=True)
        config_hash = _hash_config_file(config_path) if config_path else "none"
        safe_name = _sanitize_name(config_name)

        # Try to resume existing session for this deployment name
        existing = self._find_resumable_session(safe_name)
        if existing:
            self.session_id = existing["session_id"]
            self._session_file = existing["path"]
            self._event_count = existing["event_count"]
            logger.debug("Resumed session %s for '%s'", self.session_id, config_name)
            return self.session_id

        # Start new session
        self.session_id = _generate_session_id()
        self._session_file = self.journal_dir / f"session-{safe_name}.jsonl"
        self._event_count = 0
        self._command_count = 0

        self.record(
            EventType.SESSION_START,
            message=f"Session started for '{config_name}'",
            details={
                "config_file": str(config_path) if config_path else None,
                "config_name": config_name,
                "config_hash": config_hash,
                "lakebench_version": __version__,
            },
            config_hash=config_hash,
        )

        return self.session_id

    def close_session(self) -> None:
        """Explicitly close the current session.

        After closing, the next open_session with the same name will start
        a fresh session file (the old one is renamed with a timestamp).
        """
        if not self.session_id:
            return

        self.record(
            EventType.SESSION_END,
            message="Session ended",
            details={
                "events_recorded": self._event_count,
                "commands_run": self._command_count,
            },
        )

        # Rotate the session file so the next lifecycle gets a clean file.
        # Rename session-{name}.jsonl -> session-{name}-{timestamp}.jsonl
        if self._session_file and self._session_file.exists():
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            stem = self._session_file.stem  # e.g. "session-lakebench-test"
            archive_name = f"{stem}-{ts}.jsonl"
            archive_path = self._session_file.parent / archive_name
            try:
                self._session_file.rename(archive_path)
                logger.debug("Archived session to %s", archive_path)
            except OSError as e:
                logger.warning("Failed to archive session file: %s", e)

        self.session_id = None
        self._session_file = None

    # ------------------------------------------------------------------
    # Command lifecycle
    # ------------------------------------------------------------------

    def begin_command(
        self,
        command: CommandName,
        args: dict[str, Any] | None = None,
    ) -> None:
        """Mark the start of a CLI command."""
        self._current_command = command
        self._command_start_time = datetime.now()

        self.record(
            EventType.COMMAND_START,
            message=f"Command '{command.value}' started",
            command=command.value,
            details={"args": args or {}},
        )

    def end_command(self, success: bool, message: str = "") -> None:
        """Mark the end of a CLI command."""
        duration = None
        if self._command_start_time:
            duration = (datetime.now() - self._command_start_time).total_seconds()

        cmd_name = self._current_command.value if self._current_command else "unknown"
        self.record(
            EventType.COMMAND_END,
            message=message or f"Command '{cmd_name}' {'succeeded' if success else 'failed'}",
            command=cmd_name,
            success=success,
            duration_s=duration,
            details={"exit_code": 0 if success else 1},
        )

        self._current_command = None
        self._command_start_time = None
        self._command_count += 1

    # ------------------------------------------------------------------
    # Generic event recording
    # ------------------------------------------------------------------

    def record(
        self,
        event_type: EventType,
        message: str,
        command: str | None = None,
        success: bool | None = None,
        details: dict[str, Any] | None = None,
        config_hash: str | None = None,
        duration_s: float | None = None,
    ) -> JournalEvent:
        """Append an event to the journal file.

        Each call writes one JSON line to the session file.
        If the journal is not open, returns a stub event without writing.
        """
        if not self.session_id or not self._session_file:
            logger.debug("Journal not open, skipping event")
            return JournalEvent(
                event_type=event_type,
                session_id="none",
                message=message,
            )

        event = JournalEvent(
            event_type=event_type,
            session_id=self.session_id,
            message=message,
            command=command or (self._current_command.value if self._current_command else None),
            success=success,
            details=details or {},
            config_hash=config_hash,
            duration_s=duration_s,
        )

        with open(self._session_file, "a") as f:
            f.write(json.dumps(event.to_dict()) + "\n")

        self._event_count += 1
        return event

    # ------------------------------------------------------------------
    # Session discovery
    # ------------------------------------------------------------------

    def _find_resumable_session(self, safe_name: str) -> dict[str, Any] | None:
        """Find an unclosed session file for the given deployment name."""
        path = self.journal_dir / f"session-{safe_name}.jsonl"
        if not path.exists():
            return None

        try:
            lines = path.read_text().strip().splitlines()
            if not lines:
                return None

            first_event = json.loads(lines[0])

            # Check session was not explicitly closed
            last_event = json.loads(lines[-1])
            if last_event.get("event_type") == EventType.SESSION_END.value:
                return None

            return {
                "session_id": first_event["session_id"],
                "path": path,
                "event_count": len(lines),
            }
        except (json.JSONDecodeError, KeyError, OSError):
            return None

    # ------------------------------------------------------------------
    # Query / display helpers
    # ------------------------------------------------------------------

    def list_sessions(self) -> list[dict[str, Any]]:
        """List all sessions with summary info.

        Returns:
            List of session summary dicts, most recent first.
        """
        sessions = []
        for path in self.journal_dir.glob("session-*.jsonl"):
            try:
                lines = path.read_text().strip().splitlines()
                if not lines:
                    continue

                events = [json.loads(line) for line in lines]
                first = events[0]
                last = events[-1]

                commands = [
                    e.get("command", "")
                    for e in events
                    if e.get("event_type") == EventType.COMMAND_START.value
                ]
                closed = last.get("event_type") == EventType.SESSION_END.value

                sessions.append(
                    {
                        "session_id": first.get("session_id", ""),
                        "config_name": first.get("details", {}).get("config_name", ""),
                        "started": first.get("timestamp", ""),
                        "ended": last.get("timestamp", "") if closed else None,
                        "closed": closed,
                        "event_count": len(events),
                        "commands": commands,
                        "path": str(path),
                    }
                )
            except (json.JSONDecodeError, OSError):
                continue

        # Sort by timestamp (most recent first), falling back to session_id
        sessions.sort(key=lambda s: s.get("started", ""), reverse=True)
        return sessions

    def load_session_events(self, session_id: str) -> list[dict[str, Any]]:
        """Load all events for a specific session.

        Searches all session files for events matching the given session_id.

        Args:
            session_id: Session identifier

        Returns:
            List of event dicts in chronological order
        """
        # Search all session files for the matching session_id
        for path in self.journal_dir.glob("session-*.jsonl"):
            try:
                events = self._load_events(path)
                if events and events[0].get("session_id") == session_id:
                    return events
            except (json.JSONDecodeError, OSError):
                continue
        return []

    def _load_events(self, path: Path) -> list[dict[str, Any]]:
        """Load events from a JSONL file."""
        events = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(json.loads(line))
        return events

    # ------------------------------------------------------------------
    # Reset
    # ------------------------------------------------------------------

    def purge(self) -> int:
        """Delete all journal files.

        Returns:
            Number of files deleted
        """
        count = 0
        for path in self.journal_dir.glob("session-*.jsonl"):
            path.unlink()
            count += 1
        return count
