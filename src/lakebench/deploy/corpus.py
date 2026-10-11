"""The corpus series marker: what a generate wrote under the datagen prefix.

``<bronze>/<datagen prefix>/_corpus/series.json``
records the generate that made the corpus: the cycle count, the cycles whose
datagen Job completed, every cycle's event-time window and the generation
parameters (``generation``), with the image digest the datagen pods ran.
It lives beside the generator's per-node markers (written by images from
the look image on; the pinned 1.6.0 image writes none), under the prefix the
bronze gate clears and the listing digest covers. Every clear of that prefix
first writes a marker that says a clear is under way and keeps it
(``mark_clearing``), so a prefix never holds part of a corpus with no
marker because of a clear that stopped. ``lakebench.corpus_digest`` is the one parser;
this module writes the marker and applies the reuse rule.

Lifecycle, all on the CLI host:

1. ``begin_series``: the datagen deployer writes it right after its fresh
   clear and before the first datagen Job (``DatagenDeployer.deploy`` and
   ``deploy_cycle(0)``), with no cycle complete. A generate that stops part
   way leaves this incomplete marker, so the corpus is never taken for a
   finished one. A failed write fails the generate.
2. ``record_cycle``: after each cycle's datagen Job succeeded, a
   read-modify-write that adds the cycle. It builds only on a marker this run
   wrote for the same generation with every earlier cycle complete. Behind
   its own marker it writes nothing (the marker stays incomplete); over
   another run's marker it writes nothing and says so (``conflict``); with
   no marker at all it records the cycle alone. Never raises: a failed read
   or write leaves the marker as it was, the safe side.
3. ``series_problem``: before a run that reuses the corpus (``--skip-generate``,
   or a single-cycle run without ``--generate``), the marker must be
   complete for the config's cycle count and windows, and its generation
   must equal the config's (the image digest excepted). A single-cycle run
   over a corpus with no marker (v1.6, or an older ``generate``) proceeds.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from lakebench.config.c360_run import config_windows, cycle_windows, run_cycles
from lakebench.corpus_digest import (
    MarkerSet,
    corpus_series_sha256,
    datagen_scope,
    read_corpus_markers,
    series_key,
)

logger = logging.getLogger(__name__)

__all__ = [
    "SERIES_FORMAT",
    "SeriesRead",
    "SeriesWriteError",
    "begin_series",
    "corpus_series_sha256",
    "digest_from_image_ids",
    "generation_for",
    "pod_image_digest",
    "read_node_markers",
    "read_series",
    "record_cycle",
    "seed_ref",
    "series_problem",
]

#: The series marker format this module writes and the reuse rule reads.
SERIES_FORMAT = 1

#: ``generation`` keys the reuse rule does not compare: the config has no
#: observed digest (metrics.corpus_identity reads them for lineage).
OBSERVED_KEYS = frozenset({"image_digest", "image_digest_reason"})

#: Tag of the interim financial ``seed_ref`` until ``datagen_seed`` defines
#: ``seed_ref``. It keeps the seed out of the marker's text; it does not
#: hide it (a small seed space is searchable), and the AML seeds it can name
#: today are public.
_INTERIM_SEED_TAG = "lakebench-series-seed-ref-interim/1:"

_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


class SeriesWriteError(RuntimeError):
    """``series.json`` could not be written where a generate must write it."""


# ---------------------------------------------------------------------------
# What the config would generate
# ---------------------------------------------------------------------------


def seed_ref(cfg: Any) -> str:
    """The seed as the series marker records it: ``datagen_seed.seed_ref``
    once it exists; until then the plaintext seed for Customer 360
    and, for financial, ``sha256:`` of a fixed tag and the seed (never the
    plaintext). A corpus recorded with the interim form is refused for
    reuse once the salted form replaces it, which is the safe side."""
    from lakebench.config import datagen_seed

    schema = cfg.architecture.workload.schema_type.value
    seed = datagen_seed.config_seed(cfg)
    official = getattr(datagen_seed, "seed_ref", None)
    if callable(official):
        return str(official(schema, seed))
    if schema == "financial":
        digest = hashlib.sha256(f"{_INTERIM_SEED_TAG}{int(seed)}".encode()).hexdigest()
        return f"sha256:{digest}"
    return str(int(seed))


def generation_for(cfg: Any, total: int | None = None) -> dict[str, Any]:
    """The ``generation`` block *cfg* would write, without the observed
    image digest: the parameters that change what datagen writes. The
    window bounds are the resolved ones (``config.c360_run``), so leaving a
    default unset and spelling it out are the same corpus. ``parallelism``
    and the delivery mode are left out: rows do not depend on them (each
    node writes the file ids ``fid % total_nodes == node`` and a row is keyed
    on its file id, ``generate.rs``; ``datagen_rs/tests/cycles.rs`` pins the
    delivery mode), and the autosizer may change the pod count between
    runs."""
    from lakebench.config.datagen_seed import config_perturbation
    from lakebench.deploy.datagen import parse_size_to_bytes

    workload = cfg.architecture.workload
    dg = workload.datagen
    dims = cfg.get_scale_dimensions()
    cycles = total or run_cycles(cfg)
    windows = cycle_windows(cycles, dg.timestamp_start, dg.timestamp_end)
    out: dict[str, Any] = {
        "seed_ref": seed_ref(cfg),
        "scale": float(dg.get_effective_scale()),
        "customer_id_max": int(dims.customers),
        "file_size_mb": int(parse_size_to_bytes(dg.file_size) // (1024 * 1024)),
        "target_tb_per_cycle": round(float(dims.approx_bronze_gb) / cycles / 1024.0, 6),
        "dirty_ratio": float(dg.dirty_data_ratio),
        "image": cfg.images.datagen,
        "timestamp_start": windows[0][0],
        "timestamp_end": windows[-1][1],
    }
    if workload.schema_type.value == "financial":
        out["robustness_perturbation"] = bool(config_perturbation(cfg))
    return out


def _json(value: Any) -> Any:
    """*value* as it reads back from JSON (tuples become lists)."""
    return json.loads(json.dumps(value, default=str))


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


@dataclass
class SeriesRead:
    """What ``read_series`` found under the datagen prefix."""

    #: The parsed marker; None when absent or unreadable.
    series: dict[str, Any] | None = None
    #: A ``series.json`` object is listed (parsed or not).
    present: bool = False
    #: S3 could not be read: the reuse cannot be checked.
    error: str | None = None
    #: The marker exists but cannot be used (not JSON, not the format).
    problem: str | None = None
    #: ``corpus_digest.series_check``: why the marker does not describe the
    #: generator's node markers beside it.
    series_check: str | None = None
    markers: MarkerSet = field(default_factory=MarkerSet)
    where: str = ""


def _where(cfg: Any) -> tuple[str, str]:
    from lakebench.deploy.datagen import bronze_datagen_prefix

    bucket = cfg.platform.storage.s3.buckets.bronze
    return bucket, bronze_datagen_prefix(cfg)


def bronze_holds_data(cfg: Any, s3: Any) -> bool | None:
    """Whether the datagen prefix in bronze holds any object of the corpus
    (Lakebench's own keys aside); None when it cannot be listed."""
    bucket, prefix = _where(cfg)
    try:
        return bool(s3.bucket_exists(bucket) and s3.has_user_objects(bucket, prefix))
    except Exception:  # noqa: BLE001 -- the caller then reuses as before
        return None


def read_series(cfg: Any, s3: Any) -> SeriesRead:
    """One listing of the datagen scope through
    ``corpus_digest.read_corpus_markers`` (*s3* is an ``S3Client``). A
    missing bronze bucket reads as no marker. Never raises."""
    bucket, prefix = _where(cfg)
    out = SeriesRead()
    try:
        scope = datagen_scope(prefix)
    except ValueError as e:
        out.error = str(e)
        return out
    out.where = f"s3://{bucket}/{series_key(scope)}"
    if getattr(s3, "_init_error", None):
        out.error = f"cannot read s3://{bucket} ({s3._init_error})"
        return out
    try:
        if not s3.bucket_exists(bucket):
            return out
        client = s3.raw_client
    except Exception as e:  # noqa: BLE001 -- reported as a read failure
        out.error = f"cannot read s3://{bucket}: {type(e).__name__}: {str(e)[:200]}"
        return out
    if client is None:
        out.error = f"cannot read s3://{bucket}: the S3 client did not initialise"
        return out
    ms = read_corpus_markers(client, bucket, prefix)
    out.markers = ms
    if ms.error:
        out.error = ms.error
        return out
    out.present = ms.series_present
    out.series = ms.series
    out.series_check = ms.series_check
    if ms.series_present and ms.series is None:
        why = next((p for p in ms.problems if "series" in p), "it could not be parsed")
        out.problem = f"the series marker {out.where} is unusable: {why}"
    return out


def read_node_markers(cfg: Any, s3: Any) -> dict[int, list[dict[str, Any]]]:
    """The generator's per-node markers by cycle (``read_corpus_markers``);
    empty when none could be read."""
    return read_series(cfg, s3).markers.markers


# ---------------------------------------------------------------------------
# The reuse rule
# ---------------------------------------------------------------------------


def series_problem(cfg: Any, read: SeriesRead) -> str | None:
    """Why the corpus may not be reused by a run of *cfg*, or None.

    One rule for every cycle count: the marker must be readable, of this
    format and schema, agree with the node markers beside it, name the
    config's cycle count with every cycle complete, the config's windows,
    and the config's generation (``OBSERVED_KEYS`` excepted). No marker is
    allowed only for a single-cycle config (a corpus from v1.6 or an older
    ``generate``). A seed is never printed, only that it differs.
    """
    cycles = run_cycles(cfg)
    if read.problem:
        return read.problem
    series = read.series
    if series is None:
        if cycles == 1 and read.markers.later_cycle_files:
            return (
                f"no series marker at {read.where}, and the corpus holds "
                f"{read.markers.later_cycle_files} file(s) of cycles after the first "
                "(part-cNNN-*): a multi-cycle run made it, and one cycle would read it whole"
            )
        if cycles == 1:
            return None
        return (
            f"no series marker at {read.where}: a multi-cycle run reuses only a corpus a "
            "multi-cycle run generated and finished"
        )
    if series.get("clearing") is True:
        return (
            "a clear of the datagen prefix stopped part way (the series marker says a "
            "clear is under way)"
        )
    if series.get("format") != SERIES_FORMAT:
        return f"the series marker has format {series.get('format')!r}, not {SERIES_FORMAT}"
    schema = cfg.architecture.workload.schema_type.value
    if series.get("schema") != schema:
        return f"series made for schema {series.get('schema')!r}, config says {schema!r}"
    if read.series_check:
        return f"the series marker does not describe the corpus beside it: {read.series_check}"
    if series.get("cycles_total") != cycles:
        return f"series made with {series.get('cycles_total')!r} cycle(s), config says {cycles}"
    complete = series.get("cycles_complete")
    want = list(range(cycles))
    if complete != want:
        done = complete if isinstance(complete, list) else []
        missing = [i for i in want if i not in done]
        if missing:
            return (
                f"series incomplete: cycle(s) {missing} missing (the generate that wrote it "
                "did not finish)"
            )
        return f"series lists cycles {complete!r}, expected {want}"
    expected_windows = _json(config_windows(cfg))
    if _json(series.get("windows")) != expected_windows:
        return f"series windows {series.get('windows')!r}, config says {expected_windows!r}"
    gen = series.get("generation")
    if not isinstance(gen, dict):
        return "the series marker has no generation block"
    expected = _json(generation_for(cfg))
    for key in sorted((set(gen) | set(expected)) - OBSERVED_KEYS):
        if not _same(key, gen.get(key), expected.get(key)):
            if key == "seed_ref":
                return "series made with another seed than the config's"
            return f"series made with {key} {gen.get(key)!r}, config says {expected.get(key)!r}"
    return None


def _same(key: str, a: Any, b: Any) -> bool:
    from lakebench.corpus_digest import _same as same

    return same(key, a, b)


# ---------------------------------------------------------------------------
# The image the datagen pods ran
# ---------------------------------------------------------------------------


def digest_from_image_ids(ids: list[str]) -> tuple[str | None, str | None]:
    """``(digest, reason)`` from the datagen pods' container ``imageID``s,
    by ``metrics.experiment``'s rule: one distinct id gives its ``@`` part;
    several, none, or one without a ``sha256:`` digest give None and why."""
    distinct = sorted({i for i in ids if i})
    if not distinct:
        return None, "no pod image id"
    if len(distinct) > 1:
        return None, "datagen pods ran different images"
    if "@" not in distinct[0]:
        return None, "the pod image id carries no digest"
    digest = distinct[0].split("@", 1)[1]
    if not _DIGEST.match(digest):
        return None, "the pod image id digest is not sha256:<64 hex>"
    return digest, None


def pod_image_digest(namespace: str) -> tuple[str | None, str | None]:
    """The image digest the current ``lakebench-datagen`` Job's pods ran
    (pods owned by that Job's uid; ``metrics.datagen_aggregator``'s reader,
    the same source as the fleet record's ``image_ids``). Never raises."""
    from kubernetes import client as k8s_client

    from lakebench.deploy.datagen import DATAGEN_POD_SELECTOR, _pod_owned_by_job
    from lakebench.metrics.datagen_aggregator import _datagen_image

    try:
        job = k8s_client.BatchV1Api().read_namespaced_job(
            name="lakebench-datagen", namespace=namespace, _request_timeout=30
        )
        uid = job.metadata.uid if job.metadata else None
        pods = k8s_client.CoreV1Api().list_namespaced_pod(
            namespace, label_selector=DATAGEN_POD_SELECTOR, _request_timeout=30
        )
    except Exception as e:  # noqa: BLE001 -- recorded as the reason
        return None, f"the datagen pods could not be read ({type(e).__name__})"
    ids = []
    for pod in pods.items or []:
        if uid and not _pod_owned_by_job(pod, uid):
            continue
        ids.append(_datagen_image(pod)[1] or "")
    return digest_from_image_ids(ids)


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _body(
    cfg: Any,
    total: int,
    complete: list[int],
    run_id: str,
    digest: str | None,
    reason: str | None,
    stale: dict[str, Any] | None,
) -> dict[str, Any]:
    dg = cfg.architecture.workload.datagen
    gen = generation_for(cfg, total)
    gen["image_digest"] = digest
    if digest is None:
        gen["image_digest_reason"] = reason or "no pod image id"
    body: dict[str, Any] = {
        "format": SERIES_FORMAT,
        "schema": cfg.architecture.workload.schema_type.value,
        "cycles_total": total,
        "cycles_complete": complete,
        "windows": _json(cycle_windows(total, dg.timestamp_start, dg.timestamp_end)),
        "generation": gen,
        "written_by_run": run_id,
        "updated_utc": _now(),
    }
    if stale:
        # Generated over objects that were already there (--allow-stale-bronze):
        # a run that reuses the corpus carries the label.
        body["stale_bronze"] = dict(stale)
    return body


def _put(cfg: Any, s3: Any, body: dict[str, Any]) -> None:
    bucket, prefix = _where(cfg)
    client = s3.raw_client
    if client is None:
        raise SeriesWriteError(f"cannot write to s3://{bucket}: the S3 client did not initialise")
    client.put_object(
        Bucket=bucket,
        Key=series_key(datagen_scope(prefix)),
        Body=json.dumps(body, sort_keys=True, indent=2).encode(),
        ContentType="application/json",
    )


def begin_series(
    cfg: Any, s3: Any, total: int, run_id: str, stale: dict[str, Any] | None = None
) -> None:
    """Write the marker of a generate that is starting: *total* cycles,
    none complete. Raises ``SeriesWriteError`` when it cannot be written."""
    body = _body(cfg, total, [], run_id, None, "the generate has not finished", stale)
    try:
        _put(cfg, s3, body)
    except SeriesWriteError:
        raise
    except Exception as e:  # noqa: BLE001 -- the caller fails the generate
        raise SeriesWriteError(
            f"could not write the series marker: {type(e).__name__}: {str(e)[:200]}"
        ) from e


def mark_clearing(cfg: Any, s3: Any, run_id: str) -> str:
    """Before the datagen prefix is cleared: write a marker that says a
    clear is under way (no cycle complete) and return its key, which the
    clear keeps (``S3Client.delete_prefix(keep_keys=...)``). S3 lists
    ``_corpus/`` before the part files, so a clear that deleted the marker
    first and then stopped (an interrupt, a delete error) would leave part
    of a corpus with no marker, which a single-cycle run would reuse
    unchecked. Raises ``SeriesWriteError``; the caller then clears nothing."""
    bucket, prefix = _where(cfg)
    body = _body(cfg, run_cycles(cfg), [], run_id, None, "the corpus is being cleared", None)
    body["clearing"] = True
    try:
        _put(cfg, s3, body)
    except SeriesWriteError:
        raise
    except Exception as e:  # noqa: BLE001 -- the caller refuses to clear
        raise SeriesWriteError(
            f"could not write the series marker before clearing s3://{bucket}/{prefix}: "
            f"{type(e).__name__}: {str(e)[:200]}"
        ) from e
    return series_key(datagen_scope(prefix))


class _ReadFailed(Exception):
    """The marker could be there but could not be read."""


def _get(cfg: Any, s3: Any) -> dict[str, Any] | None:
    """The marker as stored; None when there is none (or it is not JSON).
    Raises ``_ReadFailed`` when S3 could not answer (a transient error is
    never taken for "no marker", which would let a cycle be recorded over
    another run's marker)."""
    bucket, prefix = _where(cfg)
    client = s3.raw_client
    try:
        raw = client.get_object(Bucket=bucket, Key=series_key(datagen_scope(prefix)))["Body"]
        body = raw.read()
    except Exception as e:  # noqa: BLE001 -- classified below
        code = str(getattr(e, "response", {}).get("Error", {}).get("Code", ""))
        if code in ("NoSuchKey", "404", "NotFound"):
            return None
        raise _ReadFailed(f"{type(e).__name__}: {str(e)[:200]}") from e
    try:
        data = json.loads(body)
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


def _builds_on(prior: Any, cfg: Any, cycle: int, total: int, run_id: str) -> bool:
    """Whether *prior* is this run's marker for this generation with exactly
    the cycles before *cycle* complete."""
    if not isinstance(prior, dict) or not run_id:
        return False
    gen = prior.get("generation")
    if not isinstance(gen, dict):
        return False
    expected = _json(generation_for(cfg, total))
    same_gen = all(
        _same(k, gen.get(k), expected.get(k)) for k in (set(gen) | set(expected)) - OBSERVED_KEYS
    )
    return (
        prior.get("format") == SERIES_FORMAT
        and prior.get("written_by_run") == run_id
        and prior.get("cycles_total") == total
        and prior.get("cycles_complete") == list(range(cycle))
        and same_gen
    )


def record_cycle(
    cfg: Any,
    s3: Any,
    cycle: int,
    total: int,
    run_id: str,
    digest: str | None,
    reason: str | None,
) -> str:
    """Add *cycle* to the marker once its datagen Job succeeded:
    ``"written"``; ``"unwritten"`` when it could not be written or this
    run's marker is missing an earlier cycle (it stays incomplete);
    ``"conflict"`` when another generate's marker is there (the corpus is no
    longer this run's; the caller fails). Never raises.

    The image digest is the first cycle's; a cycle whose pods ran another
    image, or whose image was not observed, leaves it null with the reason
    ("cycles ran different images"), which lineage reads as unobserved.
    """
    try:
        prior = _get(cfg, s3)
        builds = _builds_on(prior, cfg, cycle, total, run_id)
        if not builds and isinstance(prior, dict):
            if run_id and prior.get("written_by_run") == run_id:
                # This run's marker is behind (an earlier cycle's write
                # failed): it stays incomplete, as it should.
                logger.warning(
                    "series marker: cycle %d not recorded, an earlier cycle of this run is "
                    "missing from it; the corpus reads as incomplete",
                    cycle,
                )
                return "unwritten"
            # Another generate wrote the marker since this one began: the
            # corpus is no longer this run's.
            logger.warning(
                "series marker: written by run %s, not this run (%s)",
                prior.get("written_by_run"),
                run_id,
            )
            return "conflict"
        complete = list(range(cycle + 1)) if builds else [cycle]
        stale = None
        if builds and isinstance(prior, dict):
            stale = (
                prior.get("stale_bronze") if isinstance(prior.get("stale_bronze"), dict) else None
            )
            if cycle > 0:
                pgen = prior.get("generation") or {}
                prior_digest = pgen.get("image_digest")
                if prior_digest is None:
                    digest = None
                    reason = pgen.get("image_digest_reason") or reason or "no pod image id"
                elif digest is not None and digest != prior_digest:
                    digest, reason = None, "cycles ran different images"
        if not builds:
            # No marker at all (the bucket did not exist when the generate
            # began): this cycle alone.
            logger.warning("series marker: none found; cycle %d recorded alone", cycle)
        _put(cfg, s3, _body(cfg, total, complete, run_id, digest, reason, stale))
        return "written"
    except Exception as e:  # noqa: BLE001 -- an unwritten marker refuses reuse later
        logger.warning("could not write the series marker for cycle %d: %s", cycle, e)
        return "unwritten"
