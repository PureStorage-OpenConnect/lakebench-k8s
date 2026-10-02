"""A ``run --repeat N`` series: its context, membership and manifest.

The loop itself lives in ``cli/_series.py``; this module holds what other
readers (the comparison that counts n over a series) need without importing
the CLI:

- ``SeriesContext``: what one repetition is told (series id, index, size, the
  config hash the series loaded, the corpus it inherits) and what it reports
  back (run id, the corpus digest its record observed). ``seal`` is called
  once, immediately before the repetition's ``save_run``.
- ``member_of_series``: whether a saved repetition record is a member of the
  series' one corpus (ch03 section 6 "Series corpus identity", which
  ``metrics.corpus_identity`` applies when the record is built).
- the manifest ``lakebench-output/series/<id>.json`` (schema ``lb-series/1``),
  rewritten atomically after every repetition. ``passed`` counts only
  repetitions whose verdict is PASSED and that are members of the series'
  corpus.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MANIFEST_SCHEMA = "lb-series/1"
#: The corpus problem ER-9's rule records when markers give another id v2.
ID_MISMATCH_PROBLEM = "series corpus id differs from repetition 1"


def new_series_id(now: datetime | None = None) -> str:
    """``s-YYYYMMDD-HHMMSS-<6 hex>``."""
    now = now or datetime.now()
    return f"s-{now.strftime('%Y%m%d-%H%M%S')}-{secrets.token_hex(3)}"


@dataclass
class SeriesContext:
    """One repetition's view of its series (module docstring)."""

    series_id: str
    index: int
    size: int
    config_sha256: str | None
    #: ``experiment_inputs.inherited_corpus`` for repetitions 2 to N
    #: (``corpus_identity.inherited_corpus_from`` of repetition 1's record).
    inherited: dict[str, Any] | None = None
    #: D1, repetition 1's ``bronze_listing_sha256`` (None for repetition 1).
    d1: str | None = None
    # -- reported back by the repetition --
    run_id: str | None = None
    #: The ``bronze_listing_sha256`` this repetition's own record observed.
    observed_digest: str | None = None
    sealed: bool = False

    def seal(self, run_metrics: Any) -> None:
        """Immediately before ``save_run``, after the corpus observation:
        stamp ``series``, persist the inherited block, and note the digest the
        record observed. Never raises (a failure leaves the record without
        the inherited block, so it is not a series member)."""
        try:
            self.run_id = getattr(run_metrics, "run_id", None)
            run_metrics.series = {"id": self.series_id, "index": self.index, "size": self.size}
            inputs = (getattr(run_metrics, "config_snapshot", None) or {}).get("experiment_inputs")
            obs = inputs.get("corpus_observation") if isinstance(inputs, dict) else None
            self.observed_digest = (
                obs.get("bronze_listing_sha256") if isinstance(obs, Mapping) else None
            )
            if self.inherited is not None and isinstance(inputs, dict):
                inputs["inherited_corpus"] = self.inherited
            self.sealed = True
        except Exception as e:  # noqa: BLE001 -- never stop a save
            logger.warning(
                "series %s: could not seal repetition %d: %s", self.series_id, self.index, e
            )


def _corpus(record: Mapping[str, Any]) -> Mapping[str, Any]:
    exp = record.get("experiment")
    corpus = exp.get("corpus") if isinstance(exp, Mapping) else None
    return corpus if isinstance(corpus, Mapping) else {}


def _observed_digest(record: Mapping[str, Any]) -> str | None:
    inputs = (record.get("config_snapshot") or {}).get("experiment_inputs") or {}
    obs = inputs.get("corpus_observation") if isinstance(inputs, Mapping) else None
    value = obs.get("bronze_listing_sha256") if isinstance(obs, Mapping) else None
    return value if isinstance(value, str) else None


def member_of_series(
    record: Mapping[str, Any], rep1: Mapping[str, Any], d1: str
) -> tuple[bool, str | None]:
    """Whether a saved repetition 2..N record is a member of the series whose
    repetition 1 is *rep1* and whose corpus digest is *d1*: its own
    observation read *d1*, and its corpus block is either repetition 1's
    inherited verbatim (no markers) or carries the same id v2 computed from
    markers, with no series corpus problem. ``(False, reason)`` otherwise."""
    digest = _observed_digest(record)
    if digest != d1:
        return False, "bronze changed during this repetition"
    corpus = _corpus(record)
    problems = list(corpus.get("problems") or [])
    if ID_MISMATCH_PROBLEM in problems:
        return False, ID_MISMATCH_PROBLEM
    if corpus.get("inherited_from") is not None:
        if corpus.get("inherited_from") != rep1.get("run_id"):
            return False, "inherited a corpus from another run"
        if corpus.get("bronze_listing_sha256") != d1:
            return False, "inherited block does not carry the series digest"
        return True, None
    id_v2 = corpus.get("id_v2")
    if id_v2 is not None and id_v2 == _corpus(rep1).get("id_v2"):
        return True, None
    return False, "neither inherits repetition 1's corpus nor has its id v2"


def bronze_verified(record: Mapping[str, Any]) -> bool:
    """Every ``bronze-verify`` job of the record succeeded (and there is one)."""
    jobs = [j for j in record.get("jobs") or [] if j.get("job_type") == "bronze-verify"]
    return bool(jobs) and all(j.get("success") is True for j in jobs)


def stage_succeeded(record: Mapping[str, Any], stage: str) -> bool:
    jobs = [j for j in record.get("jobs") or [] if j.get("job_type") == stage]
    return bool(jobs) and all(j.get("success") is True for j in jobs)


def verdict_of(record: Mapping[str, Any] | None) -> str | None:
    if not record:
        return None
    v = record.get("verdict")
    status = v.get("status") if isinstance(v, Mapping) else None
    return status if isinstance(status, str) else None


@dataclass
class SeriesManifest:
    """``lakebench-output/series/<id>.json`` (schema ``lb-series/1``)."""

    series_id: str
    config_path: str
    config_sha256: str | None
    deployment_name: str
    requested: int
    runs: list[dict[str, Any]] = field(default_factory=list)
    corpus: dict[str, Any] = field(default_factory=dict)
    stopped_reason: str | None = None

    def add_run(
        self,
        *,
        index: int,
        run_id: str | None,
        exit_code: int,
        verdict: str | None,
        member: bool,
        reason: str | None = None,
    ) -> None:
        entry: dict[str, Any] = {
            "run_id": run_id,
            "index": index,
            "exit_code": int(exit_code),
            "verdict": verdict,
            "member": bool(member),
        }
        if reason:
            entry["not_member_reason"] = reason
        self.runs.append(entry)

    def to_dict(self) -> dict[str, Any]:
        passed = sum(1 for r in self.runs if r["verdict"] == "PASSED" and r["member"])
        return {
            "schema": MANIFEST_SCHEMA,
            "series_id": self.series_id,
            "config_path": self.config_path,
            "config_sha256": self.config_sha256,
            "deployment_name": self.deployment_name,
            "requested": self.requested,
            "runs": list(self.runs),
            "attempted": len(self.runs),
            "passed": passed,
            "failed": len(self.runs) - passed,
            "corpus": dict(self.corpus),
            "stopped_reason": self.stopped_reason,
        }

    def write(self, directory: Path) -> Path:
        """Atomic rewrite (temp file, then rename)."""
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{self.series_id}.json"
        fd, tmp = tempfile.mkstemp(dir=directory, suffix=".json.tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(self.to_dict(), f, indent=2)
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        return path


def load_manifest(path: Path) -> dict[str, Any]:
    """A manifest as written, refused when it is not ``lb-series/1``."""
    data = json.loads(Path(path).read_text())
    if not isinstance(data, dict) or data.get("schema") != MANIFEST_SCHEMA:
        raise ValueError(f"{path} is not an {MANIFEST_SCHEMA} series manifest")
    return data
