"""The corpus hashes several modules must compute the same way (stdlib only).

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
  markers do not describe one complete corpus.

It imports nothing from Lakebench, so ``s3/``, ``deploy/`` and ``metrics/``
can all use it without import cycles.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

#: Directory under the datagen prefix that holds the generator's per-node
#: markers and the series marker.
MARKER_DIR = "_corpus"

#: ``c{cycle:03}-node-{node:04}.json`` (ch05 section 3.1).
MARKER_NAME = re.compile(r"^c(\d{3})-node-(\d{4})\.json$")

SERIES_NAME = "series.json"

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


def list_scope(client: Any, bucket: str, scope: str) -> list[dict[str, Any]]:
    """Every object under *scope* in *bucket*, through the boto3
    ``list_objects_v2`` paginator. Errors propagate to the caller."""
    out: list[dict[str, Any]] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=scope):
        for obj in page.get("Contents") or []:
            if str(obj.get("Key", "")).startswith(scope):
                out.append(dict(obj))
    return out


def listing_digest(client: Any, bucket: str, prefix: str) -> str:
    """``listing_sha256`` of everything under ``datagen_scope(prefix)``."""
    return listing_sha256(list_scope(client, bucket, datagen_scope(prefix)))


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
    declared = {m.get("cycles") for ms in markers.values() for m in ms}
    if len(declared) != 1:
        return None
    cycles = _positive_int(next(iter(declared)))
    if cycles is None:
        return None
    if sorted(markers) != list(range(cycles)):
        return None
    hashes: list[str] = []
    for cycle in range(cycles):
        ms = markers[cycle]
        totals = {m.get("total_nodes") for m in ms}
        if len(totals) != 1:
            return None
        total = _positive_int(next(iter(totals)))
        if total is None:
            return None
        nodes = _node_ids(ms)
        if nodes is None or sorted(nodes) != list(range(total)):
            return None
        values = {m.get("corpus_args_sha256") for m in ms}
        value = next(iter(values))
        if len(values) != 1 or not is_sha256_hex(value):
            return None
        hashes.append(str(value))
    return hashlib.sha256(canonical_json(hashes).encode()).hexdigest()
