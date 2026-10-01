"""The rule-5 fixture scrubber (SPEC v1.7 section 6 rule 5, DESIGN ch03 ER-1).

Every run record that enters ``tests/fixtures/`` (and, through the release
harness, ``uat/runs/`` and ``docs/benchmarks/examples/``) goes through
``scrub_record``. It removes what a tracked file must never carry and leaves
everything a test reads:

* S3 endpoints: the host of any IPv4 address, and of any URL under a key
  named ``endpoint`` (or ending in ``_endpoint``/``endpoint_url``), becomes
  ``10.0.1.50``; scheme, port and path stay.
* Bucket names: every name under ``s3.buckets`` and under any key named
  ``bucket`` becomes ``scrubbed-<layer>`` (``scrubbed-bucket-<n>`` when the
  layer is unknown), everywhere it appears in a string, so ``s3a://`` paths
  stay consistent.
* Credentials: a string under a credential-named key (access key, secret,
  password, token, api key) becomes a ``${LAKEBENCH_...}`` placeholder.

It refuses, rather than rewrites, two things it cannot fix:

* a value that still looks like a credential or a lab address after the
  rewrite (``check_clean``);
* a seed that is a live held-out seed (the evaluation or robustness role in
  the AML pre-registration; spent seeds are retired and may appear). The seed is identity, so it cannot be scrubbed; such a
  record is never a fixture. The message names the path and the role, never
  the value.

And it refuses a scrub that would move identity: the stored experiment
block's identity digest, corpus id and result fingerprints, and the same
three for the block ``PipelineMetrics.experiment_block()`` rebuilds, must be
equal before and after. A rewrite that reaches an identity field (a registry
host inside a generator image reference, say) is an error, not a fixture.

Usage::

    python -m tests.fixtures.scrub SRC_METRICS_JSON DEST_METRICS_JSON
    python -m tests.fixtures.scrub --check PATH [PATH ...]
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import ipaddress
import json
import re
import sys
import tempfile
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

#: Bump when a rule changes; recorded in tests/fixtures/records/MANIFEST.json.
SCRUBBER_VERSION = 1

#: The documented placeholder host (CLAUDE.md section 8).
PLACEHOLDER_HOST = "10.0.1.50"

#: Addresses that may stay in a fixture: the placeholder, loopback, any.
ALLOWED_IPS = frozenset({PLACEHOLDER_HOST, "127.0.0.1", "0.0.0.0"})

#: Placeholder written over a credential value, by key family.
CREDENTIAL_PLACEHOLDERS = (
    (re.compile(r"(?i)access[_-]?key"), "${LAKEBENCH_S3_ACCESS_KEY}"),
    (re.compile(r"(?i)secret"), "${LAKEBENCH_S3_SECRET_KEY}"),
    (re.compile(r"(?i).*"), "${LAKEBENCH_CREDENTIAL}"),
)

#: Keys whose string value is a credential. Anchored: ``secret_ref`` or
#: ``token_count`` are names and counts, not credentials.
_CREDENTIAL_KEY = re.compile(
    r"(?i)^(?:[a-z0-9]+[_-])*"
    r"(?:access[_-]?key(?:[_-]?id)?|secret(?:[_-]?access)?(?:[_-]?key)?|password|passwd"
    r"|token|session[_-]?token|api[_-]?key|credentials?)$"
)
_ENDPOINT_KEY = re.compile(r"(?i)^(?:.*[_-])?endpoint(?:[_-]?url)?$")
_BUCKET_KEY = re.compile(r"(?i)^(?:.*[_-])?bucket(?:[_-]?name)?$")
_SEED_KEY = re.compile(r"(?i)^(?:.*_)?seed$")

_IPV4 = re.compile(r"(?<![\d.])(\d{1,3}(?:\.\d{1,3}){3})(?![\d.])")
_URL = re.compile(r"^(?P<scheme>[a-zA-Z][a-zA-Z0-9+.-]*://)(?P<host>[^/:?#\s]+)(?P<rest>.*)$")

#: Value patterns that are credentials wherever they appear (the formats
#: .gitleaks.toml adds, plus the AWS key id and PEM private keys).
_CREDENTIAL_VALUES = (
    re.compile(r"\bPSFB[A-Z]{38}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
)


class ScrubError(ValueError):
    """A record that cannot become a fixture."""


# ---------------------------------------------------------------------------
# Walking
# ---------------------------------------------------------------------------


def _walk(
    obj: Any, path: str = "", key: str | None = None
) -> Iterator[tuple[str, str | None, Any]]:
    """(path, key, value) for every leaf; key is the nearest dict key, so a
    list under ``endpoint`` is read as endpoints (as ``_rewrite`` does)."""
    if isinstance(obj, Mapping):
        for k, v in obj.items():
            yield from _walk(v, f"{path}.{k}", str(k))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _walk(v, f"{path}[{i}]", key)
    else:
        yield path, key, obj


def _rewrite(obj: Any, fn, key: str | None = None) -> Any:
    """Copy of *obj* with every string leaf replaced by ``fn(key, value)``."""
    if isinstance(obj, Mapping):
        return {k: _rewrite(v, fn, str(k)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_rewrite(v, fn, key) for v in obj]
    if isinstance(obj, str):
        return fn(key, obj)
    return obj


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------


def _foreign_ip(text: str) -> bool:
    try:
        ipaddress.IPv4Address(text)
    except ValueError:
        return False  # 999.1.2.3 is not an address (a version string, say)
    return text not in ALLOWED_IPS


def _scrub_ips(value: str) -> str:
    return _IPV4.sub(lambda m: PLACEHOLDER_HOST if _foreign_ip(m.group(1)) else m.group(1), value)


def _scrub_endpoint(value: str) -> str:
    m = _URL.match(value)
    if m:
        return f"{m.group('scheme')}{PLACEHOLDER_HOST}{m.group('rest')}"
    # A bare host[:port].
    host, sep, rest = value.partition(":")
    return f"{PLACEHOLDER_HOST}{sep}{rest}" if host else value


def _credential_placeholder(key: str) -> str:
    for pattern, placeholder in CREDENTIAL_PLACEHOLDERS:
        if pattern.search(key):
            return placeholder
    raise AssertionError("unreachable: the last pattern matches everything")


def _bucket_names(record: Mapping[str, Any]) -> dict[str, str]:
    """Every bucket name in *record* mapped to its placeholder."""
    names: dict[str, str] = {}

    def add(name: Any, layer: str | None) -> None:
        if not isinstance(name, str) or not name or name.startswith("scrubbed-"):
            return
        if name not in names:
            names[name] = f"scrubbed-{layer}" if layer else ""

    for path, key, value in _walk(record):
        parts = path.split(".")
        if len(parts) >= 3 and parts[-2] == "buckets" and parts[-3].startswith("s3"):
            add(value, key)
        elif key is not None and _BUCKET_KEY.match(key):
            add(value, None)
    taken = {v for v in names.values() if v}
    n = 0
    for name, placeholder in names.items():
        if not placeholder:
            n += 1
            while f"scrubbed-bucket-{n}" in taken:
                n += 1
            names[name] = f"scrubbed-bucket-{n}"
    return names


def _seed_problems(record: Mapping[str, Any]) -> list[str]:
    """Paths holding a seed the AML protocol protects. Never the value."""
    seeds = [(p, v) for p, k, v in _walk(record) if k and _SEED_KEY.match(k) and _is_int(v)]
    if not seeds:
        return []
    from lakebench.config import datagen_seed

    # Only the live held-out roles refuse. Spent seeds are retired, and the
    # calibration seed is the public development seed 43.
    protected = datagen_seed.protected_seeds()
    return [f"{path} holds the {protected[seed]} seed" for path, seed in seeds if seed in protected]


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def check_clean(obj: Any) -> list[str]:
    """Problems that make *obj* (a parsed record) unfit for a tracked file:
    a foreign IP, an unscrubbed endpoint, credential or bucket, or a
    protected seed. Empty when clean."""
    problems: list[str] = []
    for path, key, value in _walk(obj):
        if not isinstance(value, str):
            continue
        for m in _IPV4.finditer(value):
            if _foreign_ip(m.group(1)):
                problems.append(f"{path}: address outside the placeholder set")
        for pattern in _CREDENTIAL_VALUES:
            if pattern.search(value):
                problems.append(f"{path}: value has a credential format")
        if key and _CREDENTIAL_KEY.match(key) and value and not value.startswith("${"):
            problems.append(f"{path}: credential-named key with a literal value")
        if key and _ENDPOINT_KEY.match(key) and value:
            m = _URL.match(value)
            host = m.group("host") if m else value.partition(":")[0]
            if host not in ALLOWED_IPS:
                problems.append(f"{path}: endpoint host is not the placeholder")
    for name in _bucket_names(obj):
        problems.append(f"bucket name {name!r} is not scrubbed")
    problems.extend(_seed_problems(obj))
    return problems


# ---------------------------------------------------------------------------
# Identity guard
# ---------------------------------------------------------------------------


def identity_view(record: Mapping[str, Any]) -> dict[str, Any]:
    """What the scrubber must not change: identity digest, corpus id and
    result fingerprints of the stored experiment block and of the block
    ``experiment_block()`` rebuilds on load."""
    from lakebench.metrics.experiment import experiment_of, identity_hash, result_fingerprints
    from lakebench.metrics.storage import MetricsStorage

    def view(exp: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if not exp:
            return None
        return {
            "identity_digest": identity_hash(exp),
            "corpus_id": (exp.get("corpus") or {}).get("id"),
            "fingerprints": result_fingerprints(exp),
        }

    with tempfile.TemporaryDirectory() as tmp:
        rebuilt = MetricsStorage(tmp)._dict_to_metrics(copy.deepcopy(dict(record)))
        rebuilt_block = rebuilt.experiment_block()
    return {"stored": view(experiment_of(record)), "rebuilt": view(rebuilt_block)}


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def scrub_record(record: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """(scrubbed copy, sorted list of rewritten paths). Raises ScrubError
    when the record cannot become a fixture (see the module docstring)."""
    seeds = _seed_problems(record)
    if seeds:
        raise ScrubError("record holds a protected AML seed: " + "; ".join(seeds))
    buckets = _bucket_names(record)
    # Longest first, so "x-bronze" is not half-replaced by a bucket "x".
    ordered = sorted(buckets, key=len, reverse=True)

    def fn(key: str | None, value: str) -> str:
        out = value
        if key and _CREDENTIAL_KEY.match(key) and out and not out.startswith("${"):
            return _credential_placeholder(key)
        if key and _ENDPOINT_KEY.match(key) and out:
            out = _scrub_endpoint(out)
        out = _scrub_ips(out)
        for name in ordered:
            if name in out:
                out = out.replace(name, buckets[name])
        return out

    scrubbed = _rewrite(record, fn)
    assert isinstance(scrubbed, dict)
    before, after = record_pairs(record), record_pairs(scrubbed)
    changed = sorted(p for p in before if before[p] != after.get(p))

    problems = check_clean(scrubbed)
    if problems:
        raise ScrubError("record is not clean after scrubbing: " + "; ".join(problems))
    if identity_view(record) != identity_view(scrubbed):
        raise ScrubError(
            "scrubbing would change the record's identity; a rewritten value reaches an "
            f"identity field (rewritten paths: {', '.join(changed)})"
        )
    return scrubbed, changed


def record_pairs(record: Mapping[str, Any]) -> dict[str, Any]:
    """Flat {path: value} view used to list rewritten paths."""
    return {p: v for p, _k, v in _walk(record)}


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_fixture(record: Mapping[str, Any], dest: Path) -> list[str]:
    """Scrub *record* and write it to *dest* (stable JSON). Returns the
    rewritten paths."""
    scrubbed, changed = scrub_record(record)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(scrubbed, indent=2, sort_keys=False) + "\n")
    return changed


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m tests.fixtures.scrub", description=__doc__)
    ap.add_argument("--check", action="store_true", help="only check the given files")
    ap.add_argument("paths", nargs="+", type=Path)
    args = ap.parse_args(argv)
    if args.check:
        bad = 0
        for p in args.paths:
            for problem in check_clean(json.loads(p.read_text())):
                print(f"{p}: {problem}")
                bad += 1
        return 1 if bad else 0
    if len(args.paths) != 2:
        ap.error("scrub takes SRC and DEST")
    src, dest = args.paths
    try:
        changed = write_fixture(json.loads(src.read_text()), dest)
    except ScrubError as exc:
        print(f"{src}: {exc}", file=sys.stderr)
        return 1
    print(f"{dest}: {len(changed)} value(s) rewritten")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
