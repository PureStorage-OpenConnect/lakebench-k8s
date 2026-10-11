"""The fixture scrubber: nothing lab-specific enters ``tests/fixtures/``.

A run record goes through ``scrub_record``; driver logs and other text go
through ``scrub_text``. Both rewrite lab endpoints to ``10.0.1.50``, bucket
names to ``scrubbed-<layer>`` and credentials to ``${LAKEBENCH_...}``
placeholders. They refuse, rather than rewrite: a live held-out seed anywhere
(the message names the path and role, never the value), anything that still
looks like a credential or lab address (``check_clean``), and any rewrite that
would change the identity of the record (identity dict, fingerprints, stages,
experiment and verdict blocks). ``tests/test_stored_records.py`` pins each rule.

Known gaps: IPv6 addresses, hostnames outside URLs and endpoint values, and
free-text credentials in forms the key rule does not name. A report.html is
not scrubbed; re-render it from the scrubbed record.

Usage::

    python -m tests.fixtures.scrub SRC_METRICS_JSON DEST_METRICS_JSON
    python -m tests.fixtures.scrub --check PATH [PATH ...]
"""

from __future__ import annotations

import argparse
import bisect
import copy
import ipaddress
import json
import re
import sys
import tempfile
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

#: The documented placeholder host for the lab S3 address.
PLACEHOLDER_HOST = "10.0.1.50"

#: Hosts that may stay in a fixture.
ALLOWED_HOSTS = frozenset({PLACEHOLDER_HOST, "127.0.0.1", "0.0.0.0", "localhost"})

_CREDENTIAL_KEY = re.compile(
    r"(?:^|_)(?:access_key(?:_id)?|secret(?:_access)?(?:_key)?|password|passwd|token"
    r"|session_token|api_key|credentials?)$"
)
_ENDPOINT_KEY = re.compile(r"(?:^|_)(?:endpoints?(?:_url|_override)?(?:_s3)?|s3_url)$")
_BUCKET_KEY = re.compile(r"(?:^|_)bucket(?:_name)?$")
_DIGITS = re.compile(r"(?<!\d)\d+(?!\d)")

#: Leaf keys whose values are evidence: a rewrite there is refused unless the
#: key is also an endpoint, bucket or credential key.
_EVIDENCE_KEYS = frozenset(
    {"job_type", "job_name", "name", "status", "digest", "query_set_id", "stage", "stage_name"}
)

# Not part of a longer dotted run (a five-part version), but a sentence's
# full stop after an address does not hide it.
_IPV4 = re.compile(r"(?<!\d)(?<!\d\.)(\d{1,3}(?:\.\d{1,3}){3})(?!\d|\.\d)")
_URL = re.compile(
    # A bounded scheme keeps a long run of word characters linear, and still
    # finds a URL glued to a timestamp or a dash (``...Z-http://u:p@h``).
    r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.-]{0,31}://)(?P<userinfo>[^/@\s\"']*@)?"
    r"(?P<host>\[[^\]]*\]|[^/:?#\s\"'@]+)"
)

#: Credentials assigned inside a string (a log line, a dumped config). The
#: key is classified by the same rule as a JSON key (``_is_cred_key``), so
#: camel, dotted and prefixed names (``trustStorePassword``,
#: ``fs.s3a.secret.key``, ``s3SecretKey``) are credentials and
#: ``password_policy`` is not. Keys may be quoted, JSON-escaped quoted
#: (``\"password\":``) or YAML. The key pattern does not consume the value,
#: so ``config: secretKey: X`` still finds ``secretKey``.
#:
#: The rewrite (``_text_credentials``) is best effort: a quoted value runs
#: to its closing quote (escapes honoured); an unquoted one to the next
#: whitespace, quoted segments included, so ``abc,def`` and
#: ``AKID,secretKey="..."`` are replaced whole; a YAML value on the next
#: indented line(s), or a ``|`` / ``>`` block, is the value. The check
#: (``_unreplaced_text_credentials``) is the backstop and fails closed: after
#: any credential-named key and separator, the next token must be a
#: placeholder, a YAML key or nothing, or the text is refused.
#: Horizontal whitespace (Unicode spaces included) and a line break as
#: str.splitlines() sees one, except that a vertical tab or form feed
#: separates like a space (a value after one is on the same line).
_WS = "[^\\S\r\n\x1c\x1d\x1e\x85\u2028\u2029]"  # vertical tab and form feed count as spaces
_BR_CHARS = "\r\n\x1c\x1d\x1e\x85\u2028\u2029"
_NL = f"(?:\r\n|[{_BR_CHARS}])"
_ASSIGN_KEY = re.compile(
    rf"""(?<![A-Za-z0-9_.-])([A-Za-z0-9_.-]+)\\*["']?{_WS}*(?::=|=>|[:=]){_WS}*"""
)
#: ``--password X`` style arguments.
_FLAG_KEY = re.compile(rf"(?<![A-Za-z0-9_.-])--?([A-Za-z0-9_.-]+){_WS}+(?=\S)")
#: YAML tags, anchors and aliases before a value (``!!str``, ``!<tag>``,
#: ``&a``, ``*a``).
_YAML_PROPS = re.compile(rf"(?:(?:!<[^>\s]*>|!!?[A-Za-z0-9_-]*|[&*][A-Za-z0-9_-]+){_WS}+)*")
_QUOTE = re.compile(r"""\\*["']""")
_PLACEHOLDER = re.compile(r"\$\{[A-Za-z0-9_]+\}")
#: Nothing left on the line but an optional ``# comment`` (or a shell
#: continuation backslash): the value, if any, is on the next line.
_LINE_END = re.compile(rf"{_WS}*(?:#(?:{_WS}[^{_BR_CHARS}]*)?|\\)?{_WS}*(?={_NL}|$)")
_BREAK = re.compile(_NL)
_NEXT_LINE = re.compile(rf"(?:{_WS}*{_NL})+({_WS}+)(\S[^{_BR_CHARS}]*)")
_BLOCK_INDICATOR = re.compile(rf"[|>][0-9]?[-+]?[0-9]?{_WS}*(?:#[^{_BR_CHARS}]*)?(?={_NL})")
_YAML_KEY = re.compile(rf"""["']?[A-Za-z0-9_.-]+["']?:(?={_WS}|{_NL}|$)""")
_EMPTY_QUOTED = re.compile(r"""\\*(["'])\\*\1[,;})\]]*""")
#: Up to eight tokens after a separator, on one line: bounded, so many keys
#: on one long line stay linear.
_TOKENS = re.compile(rf"(?:{_WS}*[^\s]+){{0,8}}")
_AT_LINE_END = re.compile(f"{_WS}*(?={_NL}|$)")
_ALNUM = re.compile(r"[A-Za-z0-9]")

#: Value patterns that are credentials wherever they appear (the formats
#: .gitleaks.toml adds, including its k8s-inline-env rule, plus the AWS key
#: id and PEM private keys). These refuse; they are not rewritten.
_CREDENTIAL_VALUES = (
    re.compile(r"\bPSFB[A-Z]{38}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"""value:\s*["']?[A-Za-z0-9+/]{40}["']?(?:\s|$)"""),
    re.compile(r"(?i)\bauthorization[\"']?\s*[:=]\s*[\"']?(?:basic|bearer|token)\s+(?!\$\{)\S"),
)

#: user:secret@ after a scheme, where the secret may hold ``/`` (AWS and
#: FlashBlade secret keys do), which the URL host parser cannot split.
_USERINFO = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]{0,31}://[^\s/@\"':]*:[^\s@\"']*@")

_CGNAT = ipaddress.IPv4Network("100.64.0.0/10")

_TOKEN_BEFORE = r"(?<![A-Za-z0-9_.-])"
_TOKEN_AFTER = r"(?![A-Za-z0-9_-])"


class ScrubError(ValueError):
    """A record that cannot become a fixture. The message never carries a
    held-out seed, wherever it was built (paths come from dict keys)."""

    def __init__(self, message: str) -> None:
        super().__init__(_redact_seeds(message))


def _protected_role(n: int) -> str | None:
    """The live held-out role of *n* (``evaluation`` or ``robustness``), or
    None. Checked against the salted hashes (``datagen_seed.heldout_role``),
    never a plaintext list; a spent seed is retired and public. Raises when
    the held-out record cannot be read, so callers fail closed."""
    from lakebench.config import datagen_seed

    role = datagen_seed.heldout_role(n)
    if role is None or datagen_seed.is_spent(n):
        return None
    return role


def _redact_seeds(text: str) -> str:
    """*text* with every digit run equal to a held-out seed replaced. When
    the held-out record cannot be read, every digit run is replaced."""
    try:
        return _DIGITS.sub(
            lambda m: "<seed>" if _protected_role(int(m.group(0))) else m.group(0), text
        )
    except Exception:  # noqa: BLE001 -- fail closed: no number survives
        return _DIGITS.sub("<n>", text)


def norm_key(key: str | None) -> str:
    """``endpointOverride``, ``fs.s3a.endpoint`` and ``AWS_ENDPOINT_URL_S3``
    as ``endpoint_override``, ``fs_s3a_endpoint``, ``aws_endpoint_url_s3``."""
    if not key:
        return ""
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key)
    return re.sub(r"[.\-\s]+", "_", snake).lower()


def _is_cred_key(key: str | None) -> bool:
    return bool(_CREDENTIAL_KEY.search(norm_key(key)))


def _quoted_end(text: str, i: int, quote: str) -> int:
    """End (exclusive, before the closing quote) of a quoted body that starts
    at *i*. *quote* is ``"``, ``'`` or a backslash-escaped form; inside a
    plain quote a backslash escape and YAML's doubled ``''`` are stepped
    over. Stops at a line end."""
    n = len(text)
    while i < n and text[i] not in _BR_CHARS:
        if len(quote) == 1 and text.startswith(quote * 2, i) and quote == "'":
            i += 2
        elif text.startswith(quote, i):
            return i
        else:
            i += 2 if (text[i] == "\\" and len(quote) == 1) else 1
    return min(i, n)


def _unquoted_end(text: str, i: int) -> int:
    """End of an unquoted value at *i*: the next whitespace, stepping over
    quoted segments."""
    n = len(text)
    while i < n and not text[i].isspace():
        if text[i] in "'\"":
            i = _quoted_end(text, i + 1, text[i]) + 1
        else:
            i += 1
    return min(i, n)


def _following_lines(text: str, pos: int, block: bool) -> tuple[int, int] | None:
    """(start, end) of a YAML value on the indented line(s) after *pos*, or
    None when there is none or (for a plain value) that line is a key."""
    first = _NEXT_LINE.match(text, pos)
    if not first or (not block and _YAML_KEY.match(first.group(2))):
        return None
    indent = first.group(1)
    start, end = first.start(2), first.end(2)
    while True:
        more = _NEXT_LINE.match(text, end)
        if not more or not more.group(1).startswith(indent):
            return start, end
        if not block and _YAML_KEY.match(more.group(2)):
            return start, end
        end = more.end(2)


class _Lines:
    """Line ends of one text, found once, so asking for the end of the line
    at any position costs a bisection, not a scan."""

    def __init__(self, text: str) -> None:
        self.breaks = [m.start() for m in _BREAK.finditer(text)]
        self.n = len(text)

    def end(self, pos: int) -> int:
        i = bisect.bisect_left(self.breaks, pos)
        return self.breaks[i] if i < len(self.breaks) else self.n


def _junk(token: str) -> bool:
    """A token that is not the value itself: no letter or digit (``[``,
    ``-``, ``{``), or a YAML key inside a flow mapping (``{ value: x }``)."""
    return not _ALNUM.search(token) or bool(_YAML_KEY.fullmatch(token))


def _value_span(text: str, pos: int, lines: _Lines) -> tuple[int, int] | None:
    """The value after a credential key's separator at *pos*."""
    pos = _YAML_PROPS.match(text, pos).end()  # type: ignore[union-attr]
    quote = _QUOTE.match(text, pos)
    if quote:
        return quote.end(), _quoted_end(text, quote.end(), quote.group(0))
    block = _BLOCK_INDICATOR.match(text, pos)
    if block:
        return _following_lines(text, block.end(), block=True)
    eol = _LINE_END.match(text, pos)
    if eol:
        return _following_lines(text, eol.end(), block=False)
    if text.startswith("#", pos):
        # "#c" is a comment when an indented value line follows it.
        nxt = _following_lines(text, lines.end(pos), block=False)
        if nxt:
            return nxt
    while True:
        end = _unquoted_end(text, pos)
        token = text[pos:end]
        if not _junk(token):
            return pos, end
        rest = re.compile(f"{_WS}*").match(text, end).end()  # type: ignore[union-attr]
        if rest >= len(text) or text[rest] in _BR_CHARS:
            return pos, end  # only junk on the line: replace it, fails safe
        pos = rest


def _text_credentials(text: str) -> list[tuple[int, int, str]]:
    """(start, end, key) of each credential value assigned inside *text*
    that is not already a placeholder; spans never overlap."""
    found: list[tuple[int, int, str]] = []
    lines = _Lines(text)
    taken = 0  # end of the last value: a key inside a value is not a key
    keys = [(m, m.group(1)) for m in _ASSIGN_KEY.finditer(text)]
    keys += [(m, m.group(1)) for m in _FLAG_KEY.finditer(text)]
    for m, key in sorted(keys, key=lambda mk: mk[0].start()):
        if m.start() < taken or not _is_cred_key(key):
            continue
        span = _value_span(text, m.end(), lines)
        if span is None:
            continue
        value = text[span[0] : span[1]]
        if value and not _PLACEHOLDER.fullmatch(value):
            found.append((span[0], span[1], key))
            taken = span[1]
    return found


def _replace_text_credentials(text: str) -> str:
    out, pos = [], 0
    for start, end, key in _text_credentials(text):
        out += [text[pos:start], _credential_placeholder(key)]
        pos = end
    out.append(text[pos:])
    return "".join(out)


def _unreplaced_text_credentials(text: str) -> bool:
    """The fail-closed backstop, run after the rewrite. After a
    credential-named key and separator, the tokens on the rest of the line
    are read in order: junk (``[``, ``-``, a flow-mapping key) is skipped,
    an empty quoted value is noted, a ``# comment`` ends the line, and the
    first other token must hold a placeholder and nothing else. When the
    line holds no value at all, the first token of an indented next line
    must be a placeholder or a YAML key. Anything else refuses.

    Not covered (best effort only): a secret after a placeholder on the same
    line (``password: ${X} hunter2``), and separators outside ``: = := =>``
    (``conf.set("k", "v")``, ``Map(k -> v)``, XML, HTML entities)."""
    if _text_credentials(text):
        return True
    lines = _Lines(text)
    keys = [(m, m.group(1)) for m in _ASSIGN_KEY.finditer(text)]
    keys += [(m, m.group(1)) for m in _FLAG_KEY.finditer(text)]
    for m, key in keys:
        if not _is_cred_key(key):
            continue
        pos = _YAML_PROPS.match(text, m.end()).end()  # type: ignore[union-attr]
        span = _TOKENS.match(text, pos)
        tokens = span.group(0).split() if span else []
        after = span.end() if span else pos
        empty = settled = ended = False
        for tok in tokens:
            if _PLACEHOLDER.search(tok):
                if _ALNUM.search(_PLACEHOLDER.sub("", tok).strip("\\\"'")):
                    return True
                settled = True
                break
            if _EMPTY_QUOTED.fullmatch(tok):
                empty = True
                continue
            if tok.startswith("#"):
                ended = True
                break
            if _junk(tok):
                continue
            return True
        if settled or empty:
            continue
        if ended:
            after = lines.end(after)
        elif not _AT_LINE_END.match(text, after):
            return True  # eight tokens of junk: refuse rather than read on
        nxt = _NEXT_LINE.match(text, after)
        if nxt and not _YAML_KEY.match(nxt.group(2)):
            first = nxt.group(2).split()[0]
            if _ALNUM.search(_PLACEHOLDER.sub("", first)):
                return True
    return False


def _is_endpoint_key(key: str | None) -> bool:
    return bool(_ENDPOINT_KEY.search(norm_key(key)))


def _is_bucket_key(key: str | None) -> bool:
    return bool(_BUCKET_KEY.search(norm_key(key)))


# ---------------------------------------------------------------------------
# Walking: every leaf with its effective key and credential context
# ---------------------------------------------------------------------------


def _children(obj: Mapping[str, Any]) -> Iterator[tuple[str, str, Any]]:
    """(child key, effective key, value). A ``{name, value}`` or ``{key,
    value}`` entry gives its ``value`` the effective key ``name`` (``key``)."""
    pair_name = None
    if "value" in obj:
        for field in ("name", "key"):
            if isinstance(obj.get(field), str):
                pair_name = obj[field]
                break
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
                if where.startswith((".experiment.", ".verdict.")):
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
    if text in ALLOWED_HOSTS:
        return False
    try:
        # 010.099.007.005 is still an address to a resolver; ipaddress
        # rejects leading zeros, so normalise the octets first.
        octets = [int(o) for o in text.split(".")]
        ip = ipaddress.IPv4Address(".".join(str(o) for o in octets))
    except ValueError:
        return False  # 999.1.2.3 is not an address
    return str(ip) not in ALLOWED_HOSTS and (
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
    # Secret first: aws_secret_access_key holds "access_key" too.
    if "secret" in k:
        return "${LAKEBENCH_S3_SECRET_KEY}"
    if "access_key" in k:
        return "${LAKEBENCH_S3_ACCESS_KEY}"
    return "${LAKEBENCH_CREDENTIAL}"


class _Sensitive:
    """The bucket names and endpoint hosts one record carries."""

    def __init__(self, record: Any) -> None:
        self.buckets: dict[str, str] = {}
        hosts: set[str] = set()
        other_values: set[str] = set()
        for path, key, value, _cred in _walk(record):
            if not isinstance(value, str) or not value:
                continue
            parts = path.split(".")
            if len(parts) >= 3 and parts[-2] == "buckets" and parts[-3].startswith("s3"):
                self._add_bucket(value, key)
            elif _is_bucket_key(key):
                self._add_bucket(value, None)
            else:
                other_values.add(value)
            if _is_endpoint_key(key):
                host = _host_of(value)
                if host and host not in ALLOWED_HOSTS:
                    hosts.add(host)
        # A bucket name that the record also uses as a word (a key such as
        # stage_matrix.silver, or a value such as table_format "iceberg" or
        # pipeline_mode "batch") cannot be rewritten as a token without
        # rewriting identity fields, and a legacy record has no experiment
        # block for the identity guard to compare. Refuse instead.
        keys = {k for _p, k in _keys(record)}
        for name in self.buckets:
            if name.isalpha():
                raise ScrubError(
                    f"bucket name {name!r} is a single word; rewriting it as a token "
                    "would rewrite other fields"
                )
            if name in keys:
                raise ScrubError(
                    f"bucket name {name!r} is also a key in the record; rewriting it "
                    "would rename structure"
                )
            if name in other_values:
                raise ScrubError(
                    f"bucket name {name!r} is also a value outside the bucket settings; "
                    "rewriting it would rewrite that field"
                )
        taken = {v for v in self.buckets.values() if v}
        n = 0
        for name, placeholder in self.buckets.items():
            if not placeholder:
                n += 1
                while f"scrubbed-bucket-{n}" in taken:
                    n += 1
                self.buckets[name] = f"scrubbed-bucket-{n}"
        #: Every endpoint host: rewritten as the host of any URL.
        self.hosts = sorted(hosts)
        self._bucket_re = _token_re(list(self.buckets))
        # A dotless name (minio, prometheus) is a service name that also
        # appears as an ordinary word: rewritten in URLs and endpoint values,
        # never as a bare token elsewhere.
        # A subdomain of an endpoint host (virtual-hosted bucket.fb.lab) is
        # the same host.
        dotted = [h for h in self.hosts if "." in h]
        self._host_re = (
            re.compile(
                f"{_TOKEN_BEFORE}(?:[A-Za-z0-9-]+\\.)*(?:"
                + "|".join(re.escape(h) for h in sorted(dotted, key=len, reverse=True))
                + f"){_TOKEN_AFTER}",
                re.IGNORECASE,
            )
            if dotted
            else None
        )

    def _add_bucket(self, name: str, layer: str | None) -> None:
        if name.startswith("scrubbed-") or name in self.buckets:
            return
        if len(name) < 3:
            raise ScrubError(f"bucket name {name!r} is shorter than any valid S3 bucket name")
        self.buckets[name] = f"scrubbed-{layer}" if layer else ""

    def _is_endpoint_host(self, host: str) -> bool:
        return any(host == h or host.endswith("." + h) for h in self.hosts)

    def text(self, value: str, endpoint: bool = False) -> str:
        """*value* with addresses, endpoint hosts and buckets rewritten."""

        def url(m: re.Match[str]) -> str:
            host = m.group("host").lower()
            if endpoint or _private_ip(host) or self._is_endpoint_host(host):
                host = PLACEHOLDER_HOST
            else:
                host = m.group("host")
            return f"{m.group('scheme')}{host}"  # user-info dropped

        out = _URL.sub(url, value)
        out = _replace_text_credentials(out)
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


def _numbers(obj: Any, path: str = "") -> Iterator[tuple[str, int]]:
    """(path, n) for every integer the record holds: int and integral float
    values, and every whole digit run in a string or a dict key."""
    if isinstance(obj, Mapping):
        for k, v in obj.items():
            for m in _DIGITS.finditer(str(k)):
                yield f"{path} key", int(m.group(0))
            yield from _numbers(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _numbers(v, f"{path}[{i}]")
    elif isinstance(obj, bool):
        return
    elif isinstance(obj, int):
        yield path, obj
    elif isinstance(obj, float) and obj.is_integer():
        yield path, int(obj)
    elif isinstance(obj, str):
        for m in _DIGITS.finditer(obj):
            yield path, int(m.group(0))


def _seed_problems(record: Any) -> list[str]:
    """Paths holding a seed the AML protocol protects. Never the value."""
    # Only the live held-out roles refuse. Spent seeds are retired, and the
    # calibration seed is the public development seed 43. An unreadable
    # held-out record refuses the whole record (fail closed).
    hits = []
    try:
        for path, n in _numbers(record):
            role = _protected_role(n)
            if role:
                hits.append(f"{path} holds the {role} seed")
    except Exception:  # noqa: BLE001
        return ["the held-out seed record cannot be read; refusing every record"]
    return sorted(set(hits))


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
    if _USERINFO.search(value):
        out.append(f"{where}: URL carries user-info")
    if _unreplaced_text_credentials(value):
        out.append(f"{where}: credential assigned in text")
    return out


def check_clean(obj: Any) -> list[str]:
    """Problems that make *obj* (a parsed record) unfit for a tracked file:
    a private address, URL user-info, an unscrubbed endpoint host, credential
    or bucket name, a credential format in a value or key, or a protected
    seed. Empty when clean. No message carries a held-out seed."""
    problems: list[str] = []
    for path, key in _keys(obj):
        problems.extend(_text_problems(f"{path} key {_redact_seeds(key)[:40]!r}", key))
    for path, key, value, cred in _walk(obj):
        if not isinstance(value, str):
            continue
        problems.extend(_text_problems(path, value))
        if cred and value and not _PLACEHOLDER.fullmatch(value):
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
    # Paths and keys can hold a held-out seed: redact every message.
    return [_redact_seeds(p) for p in problems]


# ---------------------------------------------------------------------------
# Identity guard
# ---------------------------------------------------------------------------


def identity_view(record: Mapping[str, Any]) -> dict[str, Any]:
    """What the scrubber must not change, for the stored block and the block
    ``build_experiment`` makes from the record's ``experiment_inputs`` (what
    a record without a stored block gets on load; a stored block is never
    rebuilt, so this is the stricter of the two)."""
    from lakebench.metrics.experiment import (
        build_experiment,
        experiment_of,
        identity,
        result_fingerprints,
    )
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
        rebuilt_block = build_experiment(rebuilt) or rebuilt.experiment
    rebuilt_view = view(rebuilt_block)
    return {
        "stored": view(experiment_of(record)),
        "rebuilt": None if rebuilt_view is None else rebuilt_view["identity"],
        "verdict": record.get("verdict"),
    }


def _guarded(path: str, key: str | None) -> bool:
    """A rewrite at *path* would change evidence."""
    if path.startswith((".experiment.", ".verdict.")):
        # A credential-looking key there (max_token) is still evidence.
        return not (_is_endpoint_key(key) or _is_bucket_key(key))
    if _is_endpoint_key(key) or _is_bucket_key(key) or _is_cred_key(key):
        return False
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
    problems.extend(_seed_problems(out))
    if problems:
        raise ScrubError("text is not clean after scrubbing: " + "; ".join(problems))
    return out


#: The parts the scrubber may rewrite (an endpoint host); any other part
#: changing is refused.
_SCRUBBABLE_PARTS = frozenset({"storage_endpoint"})


def _system_identity_paths(obj: Any, path: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    """Every ``system_identity`` mapping with ``parts`` in a record, at any
    depth: the experiment block, the run-start inputs, and the copies a
    snapshot carries (``pipeline_benchmark.config_snapshot``)."""
    out: list[tuple[str, ...]] = []
    if isinstance(obj, Mapping):
        for key, value in obj.items():
            here = (*path, str(key))
            if key == "system_identity" and isinstance(value, Mapping) and "parts" in value:
                out.append(here)
            else:
                out += _system_identity_paths(value, here)
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            out += _system_identity_paths(value, (*path, str(i)))
    return out


def _at(obj: Any, path: tuple[str, ...]) -> Any:
    for key in path:
        if isinstance(obj, list) and key.isdigit() and int(key) < len(obj):
            obj = obj[int(key)]
        elif isinstance(obj, Mapping):
            obj = obj.get(key)
        else:
            return None
    return obj


def _recompute_system_fingerprints(
    record: Mapping[str, Any], scrubbed: dict[str, Any]
) -> list[tuple[tuple[str, ...], str]]:
    """Recompute the fingerprint of each system identity whose parts the
    scrub rewrote (the storage endpoint host), so a fixture's fingerprint is
    the hash of the parts it shows. Refuses a source whose fingerprint was
    not the hash of its own parts, or a rewrite of any other part. Returns
    ``(path, source fingerprint)`` for each recomputed one."""
    from lakebench.metrics.system_identity import fingerprint_of

    out = []
    for path in _system_identity_paths(record):
        src, dst = _at(record, path), _at(scrubbed, path)
        if not isinstance(src, Mapping) or not isinstance(dst, dict):
            continue
        sp, dp = src.get("parts") or {}, dst.get("parts") or {}
        if sp == dp:
            continue
        moved = sorted(k for k in set(sp) | set(dp) if sp.get(k) != dp.get(k))
        where = ".".join(path)
        if set(moved) - _SCRUBBABLE_PARTS:
            raise ScrubError(
                f"scrubbing would rewrite {where} parts other than an endpoint: {moved}"
            )
        version = src.get("version")
        kind = str(src.get("type") or "cluster")
        if not isinstance(version, int) or fingerprint_of(sp, None, kind, version) != src.get(
            "fingerprint"
        ):
            raise ScrubError(f"{where}.fingerprint is not the hash of its parts in the source")
        dst["fingerprint"] = fingerprint_of(dp, None, kind, version)
        out.append((path, str(src.get("fingerprint"))))
    return out


def _with_source_fingerprints(
    scrubbed: Mapping[str, Any], fixed: list[tuple[tuple[str, ...], str]]
) -> dict[str, Any]:
    """*scrubbed* with each recomputed fingerprint put back to its source
    value, for the identity guard: the recompute is the one identity change
    the scrubber makes, and it is checked on its own."""
    out = copy.deepcopy(dict(scrubbed))
    for path, fingerprint in fixed:
        node = _at(out, path)
        if isinstance(node, dict):
            node["fingerprint"] = fingerprint
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
        if cred and value and not _PLACEHOLDER.fullmatch(value):
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
    fixed = _recompute_system_fingerprints(record, scrubbed)
    changed += [".".join(("", *path, "fingerprint")) for path, _ in fixed]
    problems = check_clean(scrubbed)
    if problems:
        raise ScrubError("record is not clean after scrubbing: " + "; ".join(problems))
    if identity_view(record) != identity_view(_with_source_fingerprints(scrubbed, fixed)):
        raise ScrubError(
            "scrubbing would change the record's identity, results or verdict "
            f"(rewritten paths: {', '.join(changed)})"
        )
    return scrubbed, sorted(set(changed) | set(renamed))


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
