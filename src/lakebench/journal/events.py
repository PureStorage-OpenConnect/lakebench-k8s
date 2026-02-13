"""Journal event definitions for Lakebench provenance tracking."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class EventType(str, Enum):
    """Types of journal events."""

    # Session lifecycle
    SESSION_START = "session.start"
    SESSION_END = "session.end"

    # Command lifecycle
    COMMAND_START = "command.start"
    COMMAND_END = "command.end"

    # Config
    CONFIG_LOADED = "config.loaded"

    # Deploy
    DEPLOY_COMPONENT = "deploy.component"
    DEPLOY_COMPLETE = "deploy.complete"

    # Destroy
    DESTROY_COMPONENT = "destroy.component"
    DESTROY_COMPLETE = "destroy.complete"

    # Datagen
    GENERATE_START = "generate.start"
    GENERATE_COMPLETE = "generate.complete"

    # Pipeline
    PIPELINE_STAGE = "pipeline.stage"
    PIPELINE_COMPLETE = "pipeline.complete"

    # Cleanup
    CLEAN_TARGET = "clean.target"

    # Query
    QUERY_EXECUTED = "query.executed"

    # Benchmark
    BENCHMARK_START = "benchmark.start"
    BENCHMARK_COMPLETE = "benchmark.complete"

    # Streaming
    STREAMING_START = "streaming.start"
    STREAMING_STOP = "streaming.stop"
    STREAMING_HEALTH = "streaming.health"

    # Metrics bridge
    METRICS_SAVED = "metrics.saved"


class CommandName(str, Enum):
    """CLI command names for journal tracking."""

    VALIDATE = "validate"
    DEPLOY = "deploy"
    DESTROY = "destroy"
    GENERATE = "generate"
    RUN = "run"
    CLEAN = "clean"
    QUERY = "query"
    BENCHMARK = "benchmark"
    STOP = "stop"


@dataclass
class JournalEvent:
    """A single journal entry.

    Uniform envelope for all provenance events. Each event is serialized
    as one JSON line in a session JSONL file.
    """

    event_type: EventType
    session_id: str
    message: str
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    command: str | None = None
    success: bool | None = None
    details: dict[str, Any] = field(default_factory=dict)
    config_hash: str | None = None
    duration_s: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to JSON-safe dict."""
        d = asdict(self)
        d["event_type"] = self.event_type.value
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> JournalEvent:
        """Deserialize from dict."""
        data = dict(data)  # shallow copy to avoid mutating input
        data["event_type"] = EventType(data["event_type"])
        return cls(**data)
