"""The rule-5 fixture scrubber (SPEC v1.7 section 6 rule 5, DESIGN ch03 ER-1).

A run record enters ``tests/fixtures/`` only through ``scrub_record``; driver
logs and other text go through ``scrub_text``. What it rewrites:

* **Endpoints.** The host of a value under an endpoint-named key (``endpoint``,
  ``s3_endpoint``, ``endpointOverride``, ``fs.s3a.endpoint``,
  ``AWS_ENDPOINT_URL_S3`` and the like), the host of any URL whose host is a
  private IPv4 address or one of the record's endpoint hosts, every private
  IPv4 address, and every whole-token occurrence of an endpoint host become
  ``10.0.1.50``. URL user-info (``user:password@``) is dropped. Scheme, port
  and path stay; a value under an endpoint key that is a path (``/metrics``)
  is left alone.
* **Bucket names.** Every name under ``s3.buckets`` or a bucket-named key
  becomes ``scrubbed-<layer>`` (``scrubbed-bucket-<n>`` when the layer is
  unknown), wherever it appears as a whole token, so ``s3a://`` paths stay
  consistent and ``lakebench-bronze-verify`` is not touched by a bucket
  ``lakebench-bronze``.
* **Credentials.** A string under a credential-named key (access key, secret
  key, password, token, api key, credentials; snake, kebab, dotted or camel
  case), under any key inside a credential-named mapping, or the ``value`` of
  a ``{name: <credential-named>, value: ...}`` entry (a Kubernetes env list)
  becomes a ``${LAKEBENCH_...}`` placeholder. Dict keys are scrubbed for
  addresses, hosts and buckets as values are; a key rename that would merge
  two keys, or that falls inside ``experiment`` or ``verdict``, is refused.
  An endpoint host without a dot (``minio``) is rewritten only in the
  endpoint value itself, never as a word elsewhere.

It refuses, rather than rewrites:

* a value or key that still looks like a credential or a lab address after
  the rewrite (``check_clean``);
* a seed that is a live held-out seed (the evaluation or robustness role in
  the AML pre-registration), whether held as an int, a digit string, a list
  under ``seeds``, or ``--seed N`` inside a string. Spent seeds are retired,
  and the calibration seed is the public development seed 43. The seed is
  identity, so it cannot be scrubbed; the message names the path and role,
  never the value;
* a rewrite inside the ``experiment`` or ``verdict`` blocks, or of a
  ``job_type``, ``job_name``, ``name``, ``status``, ``digest`` or
  ``query_set_id`` value, other than at an endpoint, bucket or credential
  key;
* a scrub that changes the identity dict (every key, ``generator digest``
  included), corpus id, result fingerprints, query set, stages run or bound
  kinds of the stored block, or the identity of the block
  ``PipelineMetrics.experiment_block()`` rebuilds.

Not covered: IPv6 addresses, and hostnames that are neither in a URL nor an
endpoint value of the same record. A report.html is not scrubbed; re-render
it from the scrubbed record instead.

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
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

#: Bump when a rule changes; recorded in tests/fixtures/records/MANIFEST.json.
SCRUBBER_VERSION = 2

#: The documented placeholder host (CLAUDE.md section 8).
PLACEHOLDER_HOST = "10.0.1.50"

#: Hosts that may stay in a fixture.
ALLOWED_HOSTS = frozenset({PLACEHOLDER_HOST, "127.0.0.1", "0.0.0.0", "localhost"})

_CREDENTIAL_KEY = re.compile(
    r"(?:^|_)(?:access_key(?:_id)?|secret(?:_access)?(?:_key)?|password|passwd|token"
    r"|session_token|api_key|credentials?)$"
)
_ENDPOINT_KEY = re.compile(r"(?:^|_)endpoint(?:_url|_override)?(?:_s3)?$")
_BUCKET_KEY = re.compile(r"(?:^|_)bucket(?:_name)?$")
_SEED_KEY = re.compile(r"(?:^|_)seeds?$")
_SEED_ARG = re.compile(r"(?i)(?<![a-z0-9_])(?:--)?seed[=: ]\s*(\d+)")

#: Leaf keys whose values are evidence: a rewrite there is refused unless the
#: key is also an endpoint, bucket or credential key.
_EVIDENCE_KEYS = frozenset({"job_type", "job_name", "name", "status", "digest", "query_set_id"})

_IPV4 = re.compile(r"(?<![\d.])(\d{1,3}(?:\.\d{1,3}){3})(?![\d.])")
_URL = re.compile(
    r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.-]*://)(?P<userinfo>[^/@\s\"']*@)?"
    r"(?P<host>\[[^\]]*\]|[^/:?#\s\"'@]+)"
)

#: Value patterns that are credentials wherever they appear (the formats
#: .gitleaks.toml adds, plus the AWS key id and PEM private keys).
_CREDENTIAL_VALUES = (
    re.compile(r"\bPSFB[A-Z]{38}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
)

_CGNAT = ipaddress.IPv4Network("100.64.0.0/10")

_TOKEN_BEFORE = r"(?<![A-Za-z0-9_.-])"
_TOKEN_AFTER = r"(?![A-Za-z0-9_-])"


class ScrubError(ValueError):
    """A record that cannot become a fixture."""


def norm_key(key: str | None) -> str:
    """``endpointOverride``, ``fs.s3a.endpoint`` and ``AWS_ENDPOINT_URL_S3``
    as ``endpoint_override``, ``fs_s3a_endpoint``, ``aws_endpoint_url_s3``."""
    if not key:
        return ""
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key)
    return re.sub(r"[.\-\s]+", "_", snake).lower()


def _is_cred_key(key: str | None) -> bool:
    return bool(_CREDENTIAL_KEY.search(norm_key(key)))


def _is_endpoint_key(key: str | None) -> bool:
    return bool(_ENDPOINT_KEY.search(norm_key(key)))


def _is_bucket_key(key: str | None) -> bool:
    return bool(_BUCKET_KEY.search(norm_key(key)))


# ---------------------------------------------------------------------------
# Walking: every leaf with its effective key and credential context
# ---------------------------------------------------------------------------


def _children(obj: Mapping[str, Any]) -> Iterator[tuple[str, str, Any]]:
    """(child key, effective key, value). A ``{name, value}`` entry gives its
    ``value`` the effective key ``name``."""
    pair_name = obj.get("name") if isinstance(obj.get("name"), str) and "value" in obj else None
    for k, v in obj.items():
        yield str(k), (pair_name if (k == "value" and pair_name) else str(k)), v


def _walk(
    obj: Any, path: str = "", key: str | None = None, cred: bool = False
) -> Iterator[tuple[str, str | None, Any, bool]]:
    """(path, effective key, leaf value, in credential context)."""
    if isinstance(obj, Mapping):
        for k, ek, v in _children(obj):
            yield from _walk(v, f"{path}.{k}", ek, cred or _is_cred_key(ek))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _walk(v, f"{path}[{i}]", key, cred)
    else:
        yield path, key, obj, cred


def _keys(obj: Any, path: str = "") -> Iterator[tuple[str, str]]:
    """(path, dict key) for every dict key."""
    if isinstance(obj, Mapping):
        for k, v in obj.items():
            yield path, str(k)
            yield from _keys(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _keys(v, f"{path}[{i}]")


def _rewrite(
    obj: Any,
    leaf: Callable[[str | None, str, bool], str],
    rekey: Callable[[str], str],
    renames: list[str],
    path: str = "",
    key: str | None = None,
    cred: bool = False,
) -> Any:
    """Copy of *obj* with leaves through *leaf* and dict keys through
    *rekey*. Each renamed key's full path is appended to *renames*; a
    rename that would merge two keys, or that falls in a guarded path,
    raises ScrubError (it would drop or relabel evidence)."""
    if isinstance(obj, Mapping):
        out: dict[str, Any] = {}
        for k, ek, v in _children(obj):
            new = rekey(k)
            if new != k:
                where = f"{path}.{k}"
                if new in obj or new in out:
                    raise ScrubError(f"scrubbing key {where} would merge it into {new!r}")
                if where.startswith((".experiment", ".verdict")):
                    raise ScrubError(f"scrubbing would rename evidence key {where}")
                renames.append(where)
            out[new] = _rewrite(
                v, leaf, rekey, renames, f"{path}.{k}", ek, cred or _is_cred_key(ek)
            )
        return out
    if isinstance(obj, list):
        return [
            _rewrite(v, leaf, rekey, renames, f"{path}[{i}]", key, cred) for i, v in enumerate(obj)
        ]
    if isinstance(obj, str):
        return leaf(key, obj, cred)
    return obj


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------


def _private_ip(text: str) -> bool:
    try:
        ip = ipaddress.IPv4Address(text)
    except ValueError:
        return False  # 999.1.2.3 is not an address
    return text not in ALLOWED_HOSTS and (
        ip.is_private or ip.is_reserved or ip.is_link_local or ip in _CGNAT
    )


def _host_of(value: str) -> str | None:
    """The host of a URL or a bare ``host[:port]``; None for a path."""
    m = _URL.match(value.strip())
    if m:
        return m.group("host").lower()
    v = value.strip()
    if not v or v.startswith(("/", ".", "$")):
        return None
    return v.partition("/")[0].rpartition("@")[2].partition(":")[0].lower() or None


def _token_re(names: list[str], flags: int = 0) -> re.Pattern[str] | None:
    if not names:
        return None
    alt = "|".join(re.escape(n) for n in sorted(names, key=len, reverse=True))
    return re.compile(f"{_TOKEN_BEFORE}(?:{alt}){_TOKEN_AFTER}", flags)


def _credential_placeholder(key: str | None) -> str:
    k = norm_key(key)
    if "access_key" in k:
        return "${LAKEBENCH_S3_ACCESS_KEY}"
    if "secret" in k:
        return "${LAKEBENCH_S3_SECRET_KEY}"
    return "${LAKEBENCH_CREDENTIAL}"


class _Sensitive:
    """The bucket names and endpoint hosts one record carries."""

    def __init__(self, record: Any) -> None:
        self.buckets: dict[str, str] = {}
        hosts: set[str] = set()
        for path, key, value, _cred in _walk(record):
            if not isinstance(value, str) or not value:
                continue
            parts = path.split(".")
            if len(parts) >= 3 and parts[-2] == "buckets" and parts[-3].startswith("s3"):
                self._add_bucket(value, key)
            elif _is_bucket_key(key):
                self._add_bucket(value, None)
            if _is_endpoint_key(key):
                host = _host_of(value)
                # A dotless name (minio, prometheus) is a service name that
                # also appears as ordinary words: rewritten only where it is
                # the endpoint value itself, never as a token elsewhere.
                if host and host not in ALLOWED_HOSTS and "." in host:
                    hosts.add(host)
        taken = {v for v in self.buckets.values() if v}
        n = 0
        for name, placeholder in self.buckets.items():
            if not placeholder:
                n += 1
                while f"scrubbed-bucket-{n}" in taken:
                    n += 1
                self.buckets[name] = f"scrubbed-bucket-{n}"
        self.hosts = sorted(hosts)
        self._bucket_re = _token_re(list(self.buckets))
        self._host_re = _token_re(self.hosts, re.IGNORECASE)

    def _add_bucket(self, name: str, layer: str | None) -> None:
        if name.startswith("scrubbed-") or name in self.buckets:
            return
        if len(name) < 3:
            raise ScrubError(f"bucket name {name!r} is shorter than any valid S3 bucket name")
        self.buckets[name] = f"scrubbed-{layer}" if layer else ""

    def text(self, value: str, endpoint: bool = False) -> str:
        """*value* with addresses, endpoint hosts and buckets rewritten."""

        def url(m: re.Match[str]) -> str:
            host = m.group("host").lower()
            if endpoint or _private_ip(host) or host in self.hosts:
                host = PLACEHOLDER_HOST
            else:
                host = m.group("host")
            return f"{m.group('scheme')}{host}"  # user-info dropped

        out = _URL.sub(url, value)
        if endpoint and not _URL.match(value.strip()):
            host = _host_of(value)
            if host and host not in ALLOWED_HOSTS:
                out = re.sub(
                    f"{_TOKEN_BEFORE}{re.escape(host)}{_TOKEN_AFTER}",
                    PLACEHOLDER_HOST,
                    out,
                    flags=re.IGNORECASE,
                )
        out = _IPV4.sub(lambda m: PLACEHOLDER_HOST if _private_ip(m.group(1)) else m.group(1), out)
        if self._host_re is not None:
            out = self._host_re.sub(PLACEHOLDER_HOST, out)
        if self._bucket_re is not None:
            out = self._bucket_re.sub(lambda m: self.buckets[m.group(0)], out)
        return out


def _seed_values(path: str, key: str | None, value: Any) -> Iterator[tuple[str, int]]:
    if isinstance(value, bool):
        return
    if _SEED_KEY.search(norm_key(key)):
        if isinstance(value, int):
            yield path, value
        elif isinstance(value, str) and value.strip().isdigit():
            yield path, int(value.strip())
    if isinstance(value, str):
        for m in _SEED_ARG.finditer(value):
            yield path, int(m.group(1))


def _argv_seeds(obj: Any, path: str = "") -> Iterator[tuple[str, int]]:
    """``["--seed", "N"]`` (or ``"seed", N``) inside any list."""
    if isinstance(obj, Mapping):
        for k, v in obj.items():
            yield from _argv_seeds(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            nxt = obj[i + 1] if i + 1 < len(obj) else None
            if isinstance(v, str) and v.lstrip("-").lower() == "seed" and nxt is not None:
                if isinstance(nxt, int) and not isinstance(nxt, bool):
                    yield f"{path}[{i + 1}]", nxt
                elif isinstance(nxt, str) and nxt.strip().isdigit():
                    yield f"{path}[{i + 1}]", int(nxt.strip())
            yield from _argv_seeds(v, f"{path}[{i}]")


def _seed_problems(record: Any) -> list[str]:
    """Paths holding a seed the AML protocol protects. Never the value."""
    seeds = [s for p, k, v, _c in _walk(record) for s in _seed_values(p, k, v)]
    seeds += list(_argv_seeds(record))
    if not seeds:
        return []
    from lakebench.config import datagen_seed

    # Only the live held-out roles refuse. Spent seeds are retired, and the
    # calibration seed is the public development seed 43.
    protected = datagen_seed.protected_seeds()
    return [f"{path} holds the {protected[seed]} seed" for path, seed in seeds if seed in protected]


def _text_problems(where: str, value: str) -> list[str]:
    out = []
    for m in _IPV4.finditer(value):
        if _private_ip(m.group(1)):
            out.append(f"{where}: private address outside the placeholder set")
    for m in _URL.finditer(value):
        if m.group("userinfo"):
            out.append(f"{where}: URL carries user-info")
    for pattern in _CREDENTIAL_VALUES:
        if pattern.search(value):
            out.append(f"{where}: credential format")
    return out


def check_clean(obj: Any) -> list[str]:
    """Problems that make *obj* (a parsed record) unfit for a tracked file:
    a private address, URL user-info, an unscrubbed endpoint host, credential
    or bucket name, a credential format in a value or key, or a protected
    seed. Empty when clean."""
    problems: list[str] = []
    for path, key in _keys(obj):
        problems.extend(_text_problems(f"{path} key {key[:40]!r}", key))
    for path, key, value, cred in _walk(obj):
        if not isinstance(value, str):
            continue
        problems.extend(_text_problems(path, value))
        if cred and value and not value.startswith("${"):
            problems.append(f"{path}: credential-named key with a literal value")
        if _is_endpoint_key(key) and value:
            host = _host_of(value)
            if host and host not in ALLOWED_HOSTS:
                problems.append(f"{path}: endpoint host is not the placeholder")
    try:
        sensitive = _Sensitive(obj)
    except ScrubError as exc:
        problems.append(str(exc))
    else:
        problems.extend(f"bucket name {n!r} is not scrubbed" for n in sensitive.buckets)
    problems.extend(_seed_problems(obj))
    return problems


# ---------------------------------------------------------------------------
# Identity guard
# ---------------------------------------------------------------------------


def identity_view(record: Mapping[str, Any]) -> dict[str, Any]:
    """What the scrubber must not change, for the stored block and the block
    ``experiment_block()`` rebuilds on load."""
    from lakebench.metrics.experiment import experiment_of, identity, result_fingerprints
    from lakebench.metrics.storage import MetricsStorage

    def view(exp: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if not exp:
            return None
        limits = exp.get("limits") or {}
        return {
            "identity": identity(exp),
            "corpus_id": (exp.get("corpus") or {}).get("id"),
            "fingerprints": result_fingerprints(exp),
            "query_set_id": (exp.get("results") or {}).get("query_set_id"),
            "stages": exp.get("stages"),
            "bound_kinds": limits.get("bound_kinds"),
            "bound": limits.get("bound"),
        }

    with tempfile.TemporaryDirectory() as tmp:
        rebuilt = MetricsStorage(tmp)._dict_to_metrics(copy.deepcopy(dict(record)))
        rebuilt_block = rebuilt.experiment_block()
    rebuilt_view = view(rebuilt_block)
    return {
        "stored": view(experiment_of(record)),
        "rebuilt": None if rebuilt_view is None else rebuilt_view["identity"],
        "verdict": record.get("verdict"),
    }


def _guarded(path: str, key: str | None) -> bool:
    """A rewrite at *path* would change evidence."""
    if _is_endpoint_key(key) or _is_bucket_key(key) or _is_cred_key(key):
        return False
    if path.startswith((".experiment.", ".verdict.")):
        return True
    return norm_key(key) in _EVIDENCE_KEYS


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def scrub_text(text: str, record: Mapping[str, Any] | None = None) -> str:
    """*text* (a driver log, say) with private addresses, URL user-info and
    the endpoint hosts and bucket names of *record* rewritten. Raises
    ScrubError when it is still not clean."""
    sensitive = _Sensitive(record or {})
    out = sensitive.text(text)
    problems = _text_problems("text", out)
    seeds = [s for _p, s in _seed_values("text", None, out)]
    if seeds:
        from lakebench.config import datagen_seed

        protected = datagen_seed.protected_seeds()
        problems.extend(f"text holds the {protected[s]} seed" for s in seeds if s in protected)
    if problems:
        raise ScrubError("text is not clean after scrubbing: " + "; ".join(problems))
    return out


def scrub_record(record: Any) -> tuple[dict[str, Any], list[str]]:
    """(scrubbed copy, sorted list of rewritten paths). Raises ScrubError
    when the record cannot become a fixture (see the module docstring)."""
    if not isinstance(record, Mapping):
        raise ScrubError(f"a run record is a JSON object, not {type(record).__name__}")
    seeds = _seed_problems(record)
    if seeds:
        raise ScrubError("record holds a protected AML seed: " + "; ".join(seeds))
    sensitive = _Sensitive(record)

    def leaf(key: str | None, value: str, cred: bool) -> str:
        if cred and value and not value.startswith("${"):
            return _credential_placeholder(key)
        return sensitive.text(value, endpoint=_is_endpoint_key(key))

    renames: list[str] = []
    scrubbed = _rewrite(record, leaf, sensitive.text, renames)
    # _rewrite keeps structure and order, so the two walks pair leaf by leaf
    # even under a renamed key; paths are reported as in the source.
    changed_leaves = [
        (p, k)
        for (p, k, v, _c), (_p2, _k2, v2, _c2) in zip(_walk(record), _walk(scrubbed), strict=True)
        if v != v2
    ]
    changed = sorted(p for p, _k in changed_leaves)
    renamed = sorted(renames)

    guarded = [p for p, k in changed_leaves if _guarded(p, k)]
    if guarded:
        raise ScrubError(
            "scrubbing would rewrite evidence (not an endpoint, bucket or credential): "
            + ", ".join(guarded)
        )
    problems = check_clean(scrubbed)
    if problems:
        raise ScrubError("record is not clean after scrubbing: " + "; ".join(problems))
    if identity_view(record) != identity_view(scrubbed):
        raise ScrubError(
            "scrubbing would change the record's identity, results or verdict "
            f"(rewritten paths: {', '.join(changed)})"
        )
    return scrubbed, sorted(set(changed) | set(renamed))


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dump(record: Mapping[str, Any]) -> str:
    """The fixture's on-disk form (stable JSON)."""
    return json.dumps(record, indent=2) + "\n"


def write_fixture(record: Mapping[str, Any], dest: Path) -> list[str]:
    """Scrub *record* and write it to *dest*. Returns the rewritten paths."""
    scrubbed, changed = scrub_record(record)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(dump(scrubbed))
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
