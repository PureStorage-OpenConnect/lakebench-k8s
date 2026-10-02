"""The corpus markers and hashes several modules must read the same way.

One definition each, so the writers and readers of a corpus never disagree:

* ``datagen_scope(prefix)``: the normalised key prefix a corpus lives under
  (``bronze_datagen_prefix(cfg)`` stripped of slashes, plus one ``/``), so a
  listing never takes in a sibling prefix such as ``customer/interactions_v2/``;
* ``listing_sha256(objects)`` and ``listing_digest(client, bucket, prefix)``:
  the bronze listing digest, sha256 over the sorted ``(key, size, etag)``
  triples of every object in the scope (ch02 CC-30's series guard and ch03
  ER-9's ``bronze_listing_sha256`` are this one value);
* ``corpus_series_sha256(markers)``: ch05 section 3.1's multi-cycle form of
  the generator's ``corpus_args_sha256``, sha256 over the canonical JSON
  array of the per-cycle hashes in cycle order, or None when the per-node
  markers do not describe one complete corpus;
* ``read_corpus_markers(client, bucket, prefix) -> MarkerSet``: the one
  parser of the generator's per-node markers and of ``series.json`` (ch05
  sections 3.1 and 7.1), from one listing of the scope. ch03 ER-9 persists
  ``MarkerSet.to_dict()``; ch05 CD-18's ``read_node_markers`` and DAT-4's
  completeness check wrap ``MarkerSet.markers``.

It imports nothing from Lakebench (stdlib only), so ``s3/``, ``deploy/``
and ``metrics/`` can all use it without import cycles.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

#: Directory under the datagen prefix that holds the generator's per-node
#: markers and the series marker.
MARKER_DIR = "_corpus"

#: ``c{cycle:03}-node-{node:04}.json`` (ch05 section 3.1).
MARKER_NAME = re.compile(r"^c(\d{3})-node-(\d{4})\.json$")

SERIES_NAME = "series.json"

#: Marker file format this reader understands (ch05 section 3.1).
MARKER_FORMAT = 1

#: More markers than this is not a datagen corpus; reading stops.
MAX_MARKERS = 10_000

_HEX64 = re.compile(r"^[0-9a-f]{64}$")


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _positive_int(value: Any) -> int | None:
    return value if _is_int(value) and value >= 1 else None


def _node_ids(markers: Sequence[Mapping[str, Any]]) -> list[int] | None:
    """The markers' node ids, or None when any is not an integer."""
    out: list[int] = []
    for m in markers:
        node: Any = m.get("node_id")
        if not _is_int(node):
            return None
        out.append(node)
    return out


def _key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def distinct(values: Iterable[Any]) -> list[Any]:
    """Distinct JSON values in a stable order (lists and dicts included)."""
    seen: dict[str, Any] = {}
    for v in values:
        seen.setdefault(_key(v), v)
    return [seen[k] for k in sorted(seen)]


def is_sha256_hex(value: Any) -> bool:
    """True for a 64-character lowercase hex string."""
    return isinstance(value, str) and bool(_HEX64.match(value))


def canonical_json(obj: Any) -> str:
    """Sorted keys, no whitespace, UTF-8 text (the form every hash here uses)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def datagen_scope(prefix: str) -> str:
    """The corpus key scope for a datagen prefix: ``a/b/`` for ``a/b``,
    ``/a/b/`` or ``a/b//``. Raises ValueError for an empty prefix, which
    would make the scope the whole bucket."""
    stripped = (prefix or "").strip("/")
    if not stripped:
        raise ValueError("the datagen prefix is empty; a corpus scope is never the whole bucket")
    return stripped + "/"


def marker_key(scope: str, cycle: int, node: int) -> str:
    return f"{scope}{MARKER_DIR}/c{cycle:03d}-node-{node:04d}.json"


def series_key(scope: str) -> str:
    return f"{scope}{MARKER_DIR}/{SERIES_NAME}"


def _etag(value: Any) -> str:
    return str(value or "").strip('"')


def listing_sha256(objects: Iterable[Mapping[str, Any]]) -> str:
    """sha256 hex over the sorted ``[key, size, etag]`` triples of
    ``list_objects_v2`` ``Contents`` entries (ETag quotes stripped). A
    rewrite that keeps the object count and total bytes still changes it."""
    triples = sorted(
        [str(o["Key"]), int(o.get("Size") or 0), _etag(o.get("ETag"))] for o in objects
    )
    return hashlib.sha256(canonical_json(triples).encode()).hexdigest()


#: Bucket-level prefix of the objects Lakebench itself keeps in a bucket
#: (the bucket owner marker). They are never corpus data.
RESERVED_PREFIX = ".lakebench/"


def list_scope(client: Any, bucket: str, scope: str) -> list[dict[str, Any]]:
    """Every object under *scope* in *bucket*, through the boto3
    ``list_objects_v2`` paginator. Lakebench's own bucket objects (the
    owner marker under ``RESERVED_PREFIX``, at the bucket root) are never
    counted: a scope is never empty (``datagen_scope``) nor under that
    prefix (refused here), and only keys inside the scope are kept, even
    from a backend that returns others. Errors propagate to the caller."""
    if scope.startswith(RESERVED_PREFIX):
        raise ValueError(f"a corpus scope is never under {RESERVED_PREFIX}")
    out: list[dict[str, Any]] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=scope):
        for obj in page.get("Contents") or []:
            if str(obj.get("Key", "")).startswith(scope):
                out.append(dict(obj))
    return out


def listing_digest(client: Any, bucket: str, prefix: str) -> str | None:
    """``listing_sha256`` of everything under ``datagen_scope(prefix)``, or
    None when the scope holds no object (as ``read_corpus_markers``
    records it: an empty scope is not a corpus)."""
    objects = list_scope(client, bucket, datagen_scope(prefix))
    return listing_sha256(objects) if objects else None


def corpus_series_sha256(markers: Mapping[int, Sequence[Mapping[str, Any]]]) -> str | None:
    """ch05 section 3.1: sha256 hex of ``["h0","h1",...]``, the per-cycle
    ``corpus_args_sha256`` values in cycle order (for one cycle, the hash of
    ``["h0"]``, never ``h0`` itself).

    *markers* maps a cycle index to that cycle's parsed per-node markers.
    None when there are no markers, the cycles are not exactly
    ``0..cycles-1`` (``cycles`` as the markers state it, one value), a
    cycle's nodes are not exactly ``0..total_nodes-1`` (one ``total_nodes``
    value per cycle), any marker lacks a hash, or the nodes of one cycle
    disagree on it. The reasons are ``marker_problems``' job; this only
    refuses to hash an incomplete or mixed corpus.
    """
    if not markers:
        return None
    declared = distinct(m.get("cycles") for ms in markers.values() for m in ms)
    if len(declared) != 1:
        return None
    cycles = _positive_int(declared[0])
    if cycles is None:
        return None
    if sorted(markers) != list(range(cycles)):
        return None
    hashes: list[str] = []
    for cycle in range(cycles):
        ms = markers[cycle]
        totals = distinct(m.get("total_nodes") for m in ms)
        if len(totals) != 1:
            return None
        total = _positive_int(totals[0])
        if total is None:
            return None
        nodes = _node_ids(ms)
        if nodes is None or sorted(nodes) != list(range(total)):
            return None
        values = distinct(m.get("corpus_args_sha256") for m in ms)
        if len(values) != 1 or not is_sha256_hex(values[0]):
            return None
        hashes.append(str(values[0]))
    return hashlib.sha256(canonical_json(hashes).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Reading the markers (S3, once per run)
# ---------------------------------------------------------------------------


@dataclass
class MarkerSet:
    """What one listing of a datagen scope found."""

    scope: str = ""
    markers: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    series: dict[str, Any] | None = None
    bronze_listing_sha256: str | None = None
    objects: int = 0
    problems: list[str] = field(default_factory=list)
    #: Markers refused for a format this reader does not know.
    unreadable_format: int = 0
    series_check: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """The persisted form (ch03 section 0.1): per cycle the nodes found
        and the distinct values of the fields identity reads. Marker bodies
        (``corpus_args``, ``seed_ref``) are never persisted."""
        cycles = []
        for cycle in sorted(self.markers):
            ms = self.markers[cycle]
            cycles.append(
                {
                    "cycle": cycle,
                    "nodes_found": sorted(int(m["node_id"]) for m in ms),
                    "total_nodes": distinct(m.get("total_nodes") for m in ms),
                    "cycles": distinct(m.get("cycles") for m in ms),
                    "corpus_args_sha256": distinct(m.get("corpus_args_sha256") for m in ms),
                    "model_version": distinct(m.get("model_version") for m in ms),
                    "build_commit": distinct(m.get("build_commit") for m in ms),
                }
            )
        first, last = completed_range(self.markers)
        return {
            "format": 1,
            "scope": self.scope,
            "cycles": cycles,
            "completed_first": first,
            "completed_last": last,
            "corpus_series_sha256": corpus_series_sha256(self.markers),
            "objects": self.objects,
            "problems": list(self.problems),
            "unreadable_format": self.unreadable_format,
            "series_check": self.series_check,
            "error": self.error,
        }


def _reason(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {str(exc)[:200]}"


def _get_json(client: Any, bucket: str, key: str) -> Any:
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)


def read_corpus_markers(client: Any, bucket: str, prefix: str) -> MarkerSet:
    """One ``list_objects_v2`` pass over ``datagen_scope(prefix)`` in
    *bucket* (a boto3 client), then a GET per marker and of ``series.json``.

    The listing digest covers every object in the scope, ``_corpus/``
    included. The first failed S3 call stops the read and is recorded as
    ``error``, so an outage costs one call's retries (the boto3 client's
    own timeouts and retry count), not one per marker. Never raises.
    """
    out = MarkerSet()
    try:
        _read_into(out, client, bucket, prefix)
    except Exception as e:  # noqa: BLE001 -- recorded, never raised from a save
        out.error = f"reading the corpus markers failed: {_reason(e)}"
    return out


def _read_into(out: MarkerSet, client: Any, bucket: str, prefix: str) -> None:
    try:
        out.scope = datagen_scope(prefix)
    except ValueError as e:
        out.error = str(e)
        return
    try:
        objects = list_scope(client, bucket, out.scope)
    except Exception as e:  # noqa: BLE001
        out.error = f"listing {out.scope} failed: {_reason(e)}"
        return
    out.objects = len(objects)
    if not objects:
        # An empty scope is not a corpus: no digest, so two empty listings
        # never read as one corpus (a series cannot inherit from one).
        out.problems.append(f"no objects under {out.scope}")
        return
    out.bronze_listing_sha256 = listing_sha256(objects)

    marker_dir = f"{out.scope}{MARKER_DIR}/"
    marker_keys: list[tuple[str, int, int]] = []
    series_at = None
    for obj in objects:
        key = str(obj["Key"])
        if not key.startswith(marker_dir):
            continue
        name = key[len(marker_dir) :]
        if name == SERIES_NAME:
            series_at = key
            continue
        m = MARKER_NAME.match(name)
        if m:
            marker_keys.append((key, int(m.group(1)), int(m.group(2))))
    if len(marker_keys) > MAX_MARKERS:
        out.error = (
            f"{len(marker_keys)} corpus markers under {marker_dir} (more than {MAX_MARKERS})"
        )
        return

    for key, cycle, node in sorted(marker_keys):
        name = key[len(marker_dir) :]
        try:
            body = _get_json(client, bucket, key)
        except ValueError:  # JSONDecodeError and UnicodeDecodeError
            out.problems.append(f"corpus marker {name} is not valid JSON")
            continue
        except Exception as e:  # noqa: BLE001
            out.error = f"reading corpus marker {name} failed: {_reason(e)}"
            return
        if not isinstance(body, dict):
            out.problems.append(f"corpus marker {name} is not a JSON object")
            continue
        if body.get("format") != MARKER_FORMAT:
            out.unreadable_format += 1
            out.problems.append(
                f"corpus marker {name} has format {body.get('format')!r}; "
                f"this Lakebench reads format {MARKER_FORMAT}"
            )
            continue
        if body.get("cycle") != cycle or body.get("node_id") != node:
            out.problems.append(
                f"corpus marker {name} does not match its content "
                f"(cycle {body.get('cycle')!r}, node {body.get('node_id')!r})"
            )
            continue
        out.markers.setdefault(cycle, []).append(body)

    if series_at is not None:
        try:
            series = _get_json(client, bucket, series_at)
        except ValueError:
            out.problems.append("the series marker series.json is not valid JSON")
            series = None
        except Exception as e:  # noqa: BLE001
            out.error = f"reading series.json failed: {_reason(e)}"
            return
        if series is not None and (
            not isinstance(series, dict) or not isinstance(series.get("generation", {}), dict)
        ):
            out.problems.append("the series marker series.json is not in the series format")
            series = None
        out.series = series
    try:
        out.series_check = series_check(out.series, out.markers)
    except Exception as e:  # noqa: BLE001 -- a bad series lends no lineage, nothing more
        out.series_check = f"series.json could not be checked against the markers ({_reason(e)})"


#: series.json ``generation`` keys that must equal the markers' resolved
#: ``corpus_args`` when both carry them (ch05 sections 3.1 and 7.1), per
#: schema. Customer 360 pods are never given ``--scale`` (the template
#: passes it to financial only), so their resolved scale is the generator's
#: default while series.json records the config's; their size is
#: ``customer_id_max``.
_SERIES_ARGS = {
    "financial": ("scale", "file_size_mb"),
    "customer360": ("file_size_mb", "customer_id_max"),
}

#: Clock allowance between the CLI host (series.json ``updated_utc``) and
#: the datagen pods (marker ``completed_utc``).
SERIES_CLOCK_SKEW_S = 60.0


def _num(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, str)):
        try:
            out = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return out if out == out and abs(out) != float("inf") else None
    return None


#: Keys compared with a float tolerance; every other key compares exactly
#: (as a number when both sides are numeric, so ``"64"`` equals ``64``).
_FLOAT_KEYS = frozenset({"scale"})


def _int(value: Any) -> int | None:
    """An integer, or the integer an integral string or float spells."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()):
        return int(value)
    if isinstance(value, float) and value.is_integer() and abs(value) < 2**53:
        return int(value)
    return None


def _same(name: str, a: Any, b: Any) -> bool:
    """Equal as numbers, else as JSON. ``scale`` allows the pods' ``%.6f``
    rounding (``"1.000000"`` equals ``1.0``); every other key compares
    exactly, as integers when both sides spell one."""
    if name in _FLOAT_KEYS:
        na, nb = _num(a), _num(b)
        if na is not None and nb is not None:
            return abs(na - nb) <= 1e-6 * max(1.0, abs(na))
    ia, ib = _int(a), _int(b)
    if ia is not None and ib is not None:
        return ia == ib
    return _key(a) == _key(b)


_ISO_FRACTION = re.compile(r"(\.\d+)(?=(?:[+-]\d{2}:?\d{2}|Z)?$)")


def iso_for_python(value: str) -> str:
    """*value* in the ISO form every supported Python parses: ``Z`` as
    ``+00:00`` and the fraction cut or padded to six digits (Python 3.10's
    ``fromisoformat`` takes only three or six)."""
    text = value.strip().replace("Z", "+00:00").replace("z", "+00:00")
    return _ISO_FRACTION.sub(lambda m: (m.group(1) + "000000")[:7], text, count=1)


def utc_seconds(value: Any) -> float | None:
    """Seconds since the epoch of an ISO-8601 UTC time, or None. Accepts a
    ``Z`` suffix and any fraction length (Rust's RFC 3339 writes nine
    digits, which ``fromisoformat`` rejects before Python 3.11)."""
    from datetime import datetime, timezone

    if not isinstance(value, str) or not value:
        return None
    text = iso_for_python(value)
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


_utc = utc_seconds


def completed_range(markers: Mapping[int, Sequence[Mapping[str, Any]]]) -> tuple[Any, Any]:
    """The earliest and latest marker ``completed_utc`` (ISO strings, or
    None when any marker lacks a readable one)."""
    stamps = [
        (m.get("completed_utc"), _utc(m.get("completed_utc")))
        for ms in markers.values()
        for m in ms
    ]
    if not stamps or any(t is None for _, t in stamps):
        return None, None
    stamps.sort(key=lambda p: p[1])  # type: ignore[arg-type, return-value]
    return stamps[0][0], stamps[-1][0]


def series_check(
    series: Mapping[str, Any] | None, markers: Mapping[int, Sequence[Mapping[str, Any]]]
) -> str | None:
    """Why ``series.json`` does not describe the generate the node markers
    came from (None when it does, or when either is absent). A series left
    by an earlier generate must not lend the markers its lineage."""
    if not series or not markers:
        return None
    gen = series.get("generation") or {}
    bodies = [m for ms in markers.values() for m in ms]
    # Written after the markers: record_cycle runs once the datagen Job
    # completed, so a series older than the last marker is from an earlier
    # generate (a failed series write leaves the old one in place).
    last = completed_range(markers)[1]
    written = _utc(series.get("updated_utc"))
    if last is None or written is None:
        return "series.json or the corpus markers carry no readable completion time"
    if written + SERIES_CLOCK_SKEW_S < (_utc(last) or 0.0):
        return "series.json was written before the corpus markers (an earlier generate)"
    if "seed_ref" in gen and {str(m.get("seed_ref")) for m in bodies} != {str(gen["seed_ref"])}:
        return "series.json names another seed than the corpus markers"
    if "cycles_total" in series and distinct(m.get("cycles") for m in bodies) != [
        series["cycles_total"]
    ]:
        return "series.json names another cycle count than the corpus markers"
    schemas = distinct(m.get("schema") for m in bodies)
    if len(schemas) != 1 or schemas[0] not in _SERIES_ARGS:
        return f"the corpus markers name no single known schema ({schemas})"
    if series.get("schema") is not None and series.get("schema") != schemas[0]:
        return "series.json names another schema than the corpus markers"
    for name in _SERIES_ARGS[schemas[0]]:
        if gen.get(name) is None:
            continue
        for m in bodies:
            args = m.get("corpus_args")
            seen = args.get(name) if isinstance(args, dict) else None
            if seen is not None and not _same(name, seen, gen[name]):
                return f"series.json names another {name} than the corpus markers"
    return None
