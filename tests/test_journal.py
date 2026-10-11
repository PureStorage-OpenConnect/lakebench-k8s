"""Tests for journal provenance tracking."""

import json
from pathlib import Path

from lakebench.journal import CommandName, EventType, Journal, JournalEvent


class TestJournalEvent:
    """Tests for JournalEvent dataclass."""

    def test_to_dict_roundtrip(self) -> None:
        event = JournalEvent(
            event_type=EventType.DEPLOY_COMPONENT,
            session_id="s1",
            message="Namespace created",
            command="deploy",
            success=True,
            details={"component": "namespace", "elapsed_seconds": 3.2},
        )
        d = event.to_dict()
        assert d["event_type"] == "deploy.component"
        assert d["command"] == "deploy"
        assert d["details"]["component"] == "namespace"

        restored = JournalEvent.from_dict(d)
        assert restored.event_type == EventType.DEPLOY_COMPONENT
        assert restored.session_id == "s1"
        assert restored.success is True
        assert restored.details["elapsed_seconds"] == 3.2

        nested = JournalEvent.from_dict(
            {**d, "event_type": "command.start", "details": {"args": {"dry_run": True}}}
        )
        assert nested.event_type == EventType.COMMAND_START
        assert nested.details["args"]["dry_run"] is True


class TestJournal:
    """Tests for Journal session and event management."""

    def test_open_new_session(self, tmp_path: Path) -> None:
        j = Journal(journal_dir=tmp_path)
        sid = j.open_session(config_name="test-bench")
        assert sid is not None
        assert j.session_id == sid

        # Session file should exist with one event
        files = list(tmp_path.glob("session-*.jsonl"))
        assert len(files) == 1

        events = _read_jsonl(files[0])
        assert len(events) == 1
        assert events[0]["event_type"] == "session.start"
        assert events[0]["details"]["config_name"] == "test-bench"

    def test_session_resume_same_config(self, tmp_path: Path) -> None:
        config_file = tmp_path / "lakebench.yaml"
        config_file.write_text("name: test\n")

        # First session
        j1 = Journal(journal_dir=tmp_path)
        sid1 = j1.open_session(config_path=config_file, config_name="test")
        j1.begin_command(CommandName.DEPLOY, {"dry_run": True})
        j1.end_command(success=True)

        # Second journal instance -- should resume
        j2 = Journal(journal_dir=tmp_path)
        sid2 = j2.open_session(config_path=config_file, config_name="test")
        assert sid2 == sid1

        # Should still be one session file
        files = list(tmp_path.glob("session-*.jsonl"))
        assert len(files) == 1

    def test_no_resume_different_config(self, tmp_path: Path) -> None:
        config_a = tmp_path / "a.yaml"
        config_a.write_text("name: alpha\n")
        config_b = tmp_path / "b.yaml"
        config_b.write_text("name: beta\n")

        j1 = Journal(journal_dir=tmp_path)
        sid1 = j1.open_session(config_path=config_a, config_name="alpha")

        j2 = Journal(journal_dir=tmp_path)
        sid2 = j2.open_session(config_path=config_b, config_name="beta")
        assert sid2 != sid1

        files = list(tmp_path.glob("session-*.jsonl"))
        assert len(files) == 2

    def test_no_resume_after_close(self, tmp_path: Path) -> None:
        config_file = tmp_path / "lakebench.yaml"
        config_file.write_text("name: test\n")

        j1 = Journal(journal_dir=tmp_path)
        sid1 = j1.open_session(config_path=config_file, config_name="test")
        j1.close_session()

        j2 = Journal(journal_dir=tmp_path)
        sid2 = j2.open_session(config_path=config_file, config_name="test")
        assert sid2 != sid1

    def test_begin_end_command(self, tmp_path: Path) -> None:
        j = Journal(journal_dir=tmp_path)
        j.open_session(config_name="test")
        j.begin_command(CommandName.DEPLOY, {"dry_run": False})
        j.end_command(success=True, message="Deploy completed")

        files = list(tmp_path.glob("session-*.jsonl"))
        events = _read_jsonl(files[0])

        # session.start, command.start, command.end
        assert len(events) == 3
        assert events[1]["event_type"] == "command.start"
        assert events[1]["command"] == "deploy"
        assert events[2]["event_type"] == "command.end"
        assert events[2]["success"] is True
        assert isinstance(events[2]["duration_s"], (int, float))
        assert events[2]["duration_s"] >= 0

    def test_record_event_appends_jsonl(self, tmp_path: Path) -> None:
        j = Journal(journal_dir=tmp_path)
        j.open_session(config_name="test")

        j.record(EventType.DEPLOY_COMPONENT, "NS created", details={"component": "namespace"})
        j.record(EventType.DEPLOY_COMPONENT, "PG created", details={"component": "postgres"})

        files = list(tmp_path.glob("session-*.jsonl"))
        events = _read_jsonl(files[0])

        # session.start + 2 deploy.component
        assert len(events) == 3
        assert events[1]["details"]["component"] == "namespace"
        assert events[2]["details"]["component"] == "postgres"

    def test_record_without_open_is_noop(self, tmp_path: Path) -> None:
        j = Journal(journal_dir=tmp_path)
        event = j.record(EventType.DEPLOY_COMPONENT, "Should not write")
        assert event.session_id == "none"

        files = list(tmp_path.glob("session-*.jsonl"))
        assert len(files) == 0

    def test_list_sessions(self, tmp_path: Path) -> None:
        config = tmp_path / "test.yaml"
        config.write_text("name: test\n")

        # Session 1
        j1 = Journal(journal_dir=tmp_path)
        j1.open_session(config_path=config, config_name="bench-1")
        j1.begin_command(CommandName.DEPLOY)
        j1.end_command(success=True)
        j1.close_session()

        # Session 2 (different config so new session)
        config2 = tmp_path / "test2.yaml"
        config2.write_text("name: test2\n")
        j2 = Journal(journal_dir=tmp_path)
        j2.open_session(config_path=config2, config_name="bench-2")

        sessions = j2.list_sessions()
        assert len(sessions) == 2

        # Most recent first
        assert sessions[0]["config_name"] == "bench-2"
        assert sessions[0]["closed"] is False
        assert sessions[1]["config_name"] == "bench-1"
        assert sessions[1]["closed"] is True
        assert "deploy" in sessions[1]["commands"]

    def test_load_session_events(self, tmp_path: Path) -> None:
        j = Journal(journal_dir=tmp_path)
        sid = j.open_session(config_name="test")
        j.begin_command(CommandName.VALIDATE)
        j.end_command(success=True)

        events = j.load_session_events(sid)
        assert len(events) == 3
        types = [e["event_type"] for e in events]
        assert types == ["session.start", "command.start", "command.end"]

    def test_load_session_events_missing(self, tmp_path: Path) -> None:
        j = Journal(journal_dir=tmp_path)
        events = j.load_session_events("nonexistent-session")
        assert events == []

    def test_purge_deletes_all(self, tmp_path: Path) -> None:
        j = Journal(journal_dir=tmp_path)
        j.open_session(config_name="test-1")
        j.close_session()
        j.open_session(config_name="test-2")
        j.close_session()

        files_before = list(tmp_path.glob("session-*.jsonl"))
        assert len(files_before) == 2

        deleted = j.purge()
        assert deleted == 2

        files_after = list(tmp_path.glob("session-*.jsonl"))
        assert len(files_after) == 0

    def test_close_session_marks_ended(self, tmp_path: Path) -> None:
        j = Journal(journal_dir=tmp_path)
        j.open_session(config_name="test")
        j.begin_command(CommandName.DEPLOY)
        j.end_command(success=True)
        j.close_session()

        files = list(tmp_path.glob("session-*.jsonl"))
        events = _read_jsonl(files[0])
        assert events[-1]["event_type"] == "session.end"
        assert events[-1]["details"]["events_recorded"] == 3
        assert events[-1]["details"]["commands_run"] == 1

    def test_config_hash_for_drift_detection(self, tmp_path: Path) -> None:
        def session_hash(directory: Path, content: str) -> str:
            directory.mkdir()
            config = directory / "lakebench.yaml"
            config.write_text(content)
            Journal(journal_dir=directory).open_session(config_path=config, config_name="test")
            (path,) = directory.glob("session-*.jsonl")
            return _read_jsonl(path)[0]["config_hash"]

        a = session_hash(tmp_path / "a", "name: test\nscale: 10\n")
        same = session_hash(tmp_path / "b", "name: test\nscale: 10\n")
        other = session_hash(tmp_path / "c", "name: test\nscale: 50\n")
        assert a == same
        assert a != other


def _read_jsonl(path: Path) -> list[dict]:
    """Helper to read all events from a JSONL file."""
    events = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events
