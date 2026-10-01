#!/usr/bin/env python3
"""Every tracked doc has a reader, and the docs' links resolve.

A file in the universe (every tracked ``*.md``, every tracked file under
``docs/`` and ``scripts/``, and the top-level ``*.py`` and ``*.sh``) passes
when it is

- **linked**: reachable by markdown links from ``README.md`` or
  ``docs/README.md`` (relative links, directory links to their
  ``README.md``, and absolute ``github.com/<repo>/(blob|tree)/main/<path>``
  links; image links count);
- **read**: its repo-relative path (or, for a file under ``docs/`` in a
  subdirectory, its docs-relative path) appears in a Python string constant
  that is not a docstring under ``src/``, ``tests/`` or ``scripts/``; a
  string constant equals its basename and that basename is unique among
  tracked files; its path appears outside comments in a workflow, the
  ``Makefile``, ``pyproject.toml`` (string values, not under an
  ``exclude``, ``extend-exclude``, ``force-exclude`` or ``omit`` key) or
  ``.pre-commit-config.yaml``, alone or after ``./``, ``../``, ``$VAR/`` or
  ``${{ expr }}/``; or it
  matches a ``PATTERN_READERS`` entry whose literal is still in the reader;
- a **platform file** GitHub or the release reads (it only has to exist, and
  must, unless it is in ``PENDING_PLATFORM_FILES``);
- a **published stub**: a path a published PyPI README links, kept for one
  release as a page of at most five lines.

A citation in a comment or a docstring is not a reader; a path in any
other string constant, an error message included, is one.

Usage:
    python scripts/check_doc_readers.py                  # the reader check
    python scripts/check_doc_readers.py --resolve-paths NOTES.md [--base [PREFIX=]DIR ...]

``--resolve-paths`` checks that every backticked path and every relative
link in a local file (agent instructions, a memory index, a brief) exists: a
link relative to the file's directory, a backticked path absolute or
relative to one of the ``--base`` directories (default: the repository
root). A base given as ``PREFIX=DIR`` takes only the tokens that start with
PREFIX, and those resolve under DIR alone, so a path that is gone from one
tree cannot resolve in another. ``--ignore REGEX`` (repeatable) skips
matching tokens.
"""

from __future__ import annotations

import argparse
import ast
import bisect
import fnmatch
import html
import os
import posixpath
import re
import subprocess
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ImportError:  # Python 3.10; tomli is in [dev] there
    import tomli as tomllib  # type: ignore[no-redef]

ROOT = Path(__file__).resolve().parents[1]
REPO = "PureStorage-OpenConnect/lakebench-k8s"
INDEXES = ("README.md", "docs/README.md")

#: Files GitHub or the release process reads; they only have to exist.
PLATFORM_FILES = (
    "README.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "CODE_OF_CONDUCT.md",
    "LICENSE",
    "CHANGELOG.md",
    "RELEASING.md",
    "UPGRADING-*.md",
    ".github/pull_request_template.md",
    # Fetched by the documented `curl ... install.sh | sh` (getting-started.md).
    "install.sh",
    # pytest loads the rootdir conftest.py itself.
    "conftest.py",
)
#: Platform files that do not exist yet; every other non-glob PLATFORM_FILES
#: entry must exist. The list may only shrink (the test fails once one exists).
PENDING_PLATFORM_FILES = ("RELEASING.md",)

#: Paths a published PyPI README links that v1.7 moved; each stays one
#: release as a stub of at most STUB_MAX_LINES lines. The benchmark-spec move
#: adds docs/aml-scoring.md here when it turns that page into a stub.
PUBLISHED_STUBS: tuple[str, ...] = ()
STUB_MAX_LINES = 5

#: (glob, reader file, literal): files read through a computed path. An entry
#: counts only while the literal is still in the reader file.
PATTERN_READERS = (("uat/results-*.md", "scripts/release_gate.py", "uat/results-"),)

#: Unread files on the day this check landed. The list may only shrink: the
#: test fails when an entry gains a reader or disappears, so remove it then.
KNOWN_ORPHANS = (
    "docs/deep-dive/datagen-metrics.md",
    "docs/design/README.md",
    # Cited only in docstrings and comments; linked once docs/design/README.md is.
    "docs/design/namespace-isolation.md",
    "docs/internal/design-contradictions.md",
    # Cited only in comments and docstrings, and from the Rust datagen.
    "docs/internal/observability-pushgateway.md",
    "docs/reproductions/README.md",
    "docs/reproductions/c360-scale-0-1.yaml",
)

_CONFIG_READERS = ("Makefile", "pyproject.toml", ".pre-commit-config.yaml")
#: The checker and its tests name paths to check them, which is not reading them.
_NOT_READERS = (
    "scripts/check_doc_readers.py",
    "tests/test_doc_readers.py",
    "tests/test_doc_links.py",
)


# -- tracked files -------------------------------------------------------------


def _git_env() -> dict[str, str]:
    # Under a git hook GIT_DIR and GIT_INDEX_FILE name the hook's repository;
    # inherited, `git -C <root>` would list that index instead of root's.
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def tracked_files(root: Path = ROOT) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z"],
        capture_output=True,
        check=True,
        env=_git_env(),
    ).stdout.decode()
    return sorted(p for p in out.split("\0") if p and (root / p).is_file())


def universe(files: Iterable[str]) -> list[str]:
    out = []
    for p in files:
        top_level = "/" not in p
        if (
            p.endswith(".md")
            or p.startswith(("docs/", "scripts/"))
            or (top_level and p.endswith((".py", ".sh")))
        ):
            out.append(p)
    return sorted(out)


# -- markdown parsing ----------------------------------------------------------

_FENCE = re.compile(r"^\s{0,3}(`{3,}|~{3,})")
_INLINE_CODE = re.compile(r"(`+)(.+?)\1")
_LINK = re.compile(
    r"!?\[(?:[^\[\]]|\[[^\[\]]*\])*\]\(\s*<?([^()\s<>]+(?:\([^()\s]*\))?)>?(?:\s+[\"'(][^)]*)?\)"
)
_REF_DEF = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*<?(\S+?)>?(?:\s+.*)?$")
_HTML_LINK = re.compile(r"""\b(?:href|src)\s*=\s*["']([^"']+)["']""")
_HTML_ANCHOR = re.compile(
    r"""<a\s[^>]*\b(?:name|id)\s*=\s*["']([^"']+)["']|\bid\s*=\s*["']([^"']+)["']"""
)
_ATX = re.compile(r"^\s{0,3}(#{1,6})\s+(.*?)\s*#*\s*$")
_REPO_URL = re.compile(
    rf"^https?://github\.com/{re.escape(REPO)}/(?:blob|tree)/main/?(?P<path>[^#?]*)(?:\?[^#]*)?(?:#(?P<anchor>.*))?$",
    re.I,
)
_FOOTNOTE = re.compile(r"^\s{0,3}\[\^")


def prose_lines(text: str) -> list[tuple[int, str]]:
    """(line number, text) outside fenced code blocks, inline code blanked."""
    out = []
    fence: str | None = None
    for n, line in enumerate(text.splitlines(), 1):
        m = _FENCE.match(line)
        if fence is not None:
            if m and m.group(1)[0] == fence[0] and len(m.group(1)) >= len(fence):
                fence = None
            continue
        if m:
            fence = m.group(1)
            continue
        out.append((n, _INLINE_CODE.sub(lambda c: " " * len(c.group(0)), line)))
    return out


def links(text: str) -> list[tuple[int, str]]:
    """(line number, target) for every markdown, reference and HTML link,
    including inline links whose text wraps onto the next line."""
    kept = dict(prose_lines(text))
    lines = [kept.get(n, "") for n in range(1, len(text.splitlines()) + 1)]
    starts = [0]
    for line in lines:
        starts.append(starts[-1] + len(line) + 1)
    out = []
    for m in _LINK.finditer("\n".join(lines)):
        out.append((bisect.bisect_right(starts, m.start()), m.group(1)))
    for n, line in enumerate(lines, 1):
        ref = _REF_DEF.match(line)
        if ref and not _FOOTNOTE.match(line):
            out.append((n, ref.group(1)))
        out += [(n, h.group(1)) for h in _HTML_LINK.finditer(line)]
    return sorted(out)


def slug(heading: str) -> str:
    """GitHub's heading anchor: rendered text, lowercased, punctuation other
    than '-' and '_' dropped, spaces to hyphens."""
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", heading)  # links -> text
    text = re.sub(r"<[^>]+>", "", text)  # inline HTML
    text = html.unescape(text)
    # Code spans render verbatim; emphasis markers outside them do not render.
    parts = re.split(r"(`+[^`]*?`+)", text)
    out = []
    for part in parts:
        if part.startswith("`"):
            out.append(part.strip("`"))
            continue
        part = part.replace("*", "").replace("~", "")
        # Matched _emphasis_ / __strong__ pairs only; snake_case and a lone
        # underscore stay, as GitHub keeps them.
        part = re.sub(r"(?<![\w\\])(_{1,3})(?=\S)(.+?)(?<=\S)\1(?!\w)", r"\2", part)
        out.append(part)
    text = "".join(out).strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def anchors(text: str) -> set[str]:
    """Every anchor a page defines: heading slugs (with -1, -2 for repeats)
    and explicit HTML name or id attributes."""
    seen: dict[str, int] = {}
    out: set[str] = set()
    prev = ""
    for _, line in _heading_candidates(text):
        m = _ATX.match(line)
        title = m.group(2) if m else None
        if (
            title is None
            and re.fullmatch(r"\s{0,3}(=+|-+)\s*", line)
            and prev.strip()
            and not _ATX.match(prev)
            and not re.match(r"\s*([-*+]|\d+[.)])\s|\s*\||\s*>", prev)
        ):
            title = prev.strip()
        if title is not None:
            base = slug(title)
            k = seen.get(base, 0)
            out.add(base if k == 0 else f"{base}-{k}")
            seen[base] = k + 1
        prev = line
    for _, line in prose_lines(text):
        for m in _HTML_ANCHOR.finditer(line):
            out.add((m.group(1) or m.group(2)).lower())
    return out


def _heading_candidates(text: str) -> list[tuple[int, str]]:
    # Headings keep their inline code (it is part of the anchor text).
    out = []
    fence: str | None = None
    for n, line in enumerate(text.splitlines(), 1):
        m = _FENCE.match(line)
        if fence is not None:
            if m and m.group(1)[0] == fence[0] and len(m.group(1)) >= len(fence):
                fence = None
            continue
        if m:
            fence = m.group(1)
            continue
        out.append((n, line))
    return out


@dataclass(frozen=True)
class Target:
    path: str  # repo-relative; "" for the repository root
    anchor: str | None


def resolve(src: str, target: str) -> Target | None:
    """Where a link in *src* points inside the repository; None for an
    external link or a mail address."""
    m = _REPO_URL.match(target)
    if m:
        norm = posixpath.normpath(m.group("path") or ".")
        return Target("" if norm == "." else norm, m.group("anchor") or None)
    if re.match(r"^[a-z][a-z0-9+.-]*:", target, re.I):
        return None
    path, _, anchor = target.partition("#")
    path = path.split("?", 1)[0]
    if not path:
        return Target(src, anchor or None)
    if path.startswith("/"):
        joined = path.lstrip("/")
    else:
        joined = posixpath.join(posixpath.dirname(src), path)
    norm = posixpath.normpath(joined)
    return Target("" if norm == "." else norm, anchor or None)


def _is_dir(path: str, files: set[str]) -> bool:
    return path == "" or any(f.startswith(path.rstrip("/") + "/") for f in files)


def linked_files(files: Sequence[str], root: Path = ROOT) -> set[str]:
    """Tracked files reachable by links from the two indexes."""
    tracked = set(files)
    seen: set[str] = set()
    todo = [i for i in INDEXES if i in tracked]
    while todo:
        page = todo.pop()
        if page in seen:
            continue
        seen.add(page)
        if not page.endswith(".md"):
            continue
        for _, raw in links((root / page).read_text(encoding="utf-8")):
            t = resolve(page, raw)
            if t is None:
                continue
            path = t.path
            if path not in tracked and _is_dir(path, tracked):
                path = posixpath.join(path, "README.md") if path else "README.md"
            if path in tracked and path not in seen:
                todo.append(path)
    return seen


# -- readers ---------------------------------------------------------------------


def _docstring_nodes(tree: ast.AST) -> set[int]:
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                if isinstance(body[0].value.value, str):
                    out.add(id(body[0].value))
    return out


def python_strings(files: Sequence[str], root: Path = ROOT) -> list[str]:
    """Every non-docstring string constant in tracked Python under src/,
    tests/ and scripts/."""
    out: list[str] = []
    for rel in files:
        if not rel.endswith(".py") or not rel.startswith(("src/", "tests/", "scripts/")):
            continue
        if rel in _NOT_READERS:
            continue
        try:
            tree = ast.parse((root / rel).read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        skip = _docstring_nodes(tree)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in skip
            ):
                out.append(node.value)
    return out


def _strip_comment(line: str) -> str:
    if line.lstrip().startswith("#"):
        return ""
    return re.split(r"\s#", line, maxsplit=1)[0]


#: pyproject.toml keys whose values leave files out rather than read them.
_EXCLUDE_KEYS = frozenset({"exclude", "extend-exclude", "force-exclude", "omit"})


def _pyproject_strings(text: str) -> str:
    """Every string value in pyproject.toml, one per line, except those under
    an exclude-type key: leaving a file out of a package is not reading it."""
    out: list[str] = []

    def walk(value: object, key: str = "") -> None:
        if key in _EXCLUDE_KEYS:
            return
        if isinstance(value, str):
            out.append(value)
        elif isinstance(value, dict):
            for k, v in value.items():
                walk(v, k)
        elif isinstance(value, list):
            for v in value:
                walk(v, key)

    walk(tomllib.loads(text))
    return "\n".join(out)


def config_text(files: Sequence[str], root: Path = ROOT) -> str:
    """Workflows, Makefile, pyproject.toml (without exclude lists) and
    .pre-commit-config.yaml, comments removed."""
    readers = [f for f in files if f.startswith(".github/workflows/") or f in _CONFIG_READERS]
    lines = []
    for rel in readers:
        text = (root / rel).read_text(encoding="utf-8")
        if rel == "pyproject.toml":
            lines += _pyproject_strings(text).splitlines()
            continue
        lines += [_strip_comment(ln) for ln in text.splitlines()]
    return "\n".join(lines)


#: What may sit before a path and still leave it a whole path: `./`, `../`,
#: `$VAR/`, `${VAR}/`, `${{ expr }}/` or `$(cmd)/`.
_PATH_PREFIX = re.compile(r"(?:(?<![\w.])\.{1,2}|\$\w+|\}|\))/\Z")


def _mentions(haystack: str, path: str) -> bool:
    """*path* appears in *haystack* as a whole path, not inside a longer one."""
    for m in re.finditer(rf"{re.escape(path)}(?![\w-]|\.\w)", haystack):
        before = haystack[max(0, m.start() - 64) : m.start()]
        if not before or not re.search(r"[\w./-]\Z", before):
            return True
        if _PATH_PREFIX.search(before):
            return True
    return False


def _matches_platform(path: str) -> bool:
    return any(fnmatch.fnmatch(path, pat) for pat in PLATFORM_FILES)


def pattern_reader(path: str, root: Path = ROOT) -> bool:
    for glob, reader, literal in PATTERN_READERS:
        if fnmatch.fnmatch(path, glob):
            reader_path = root / reader
            if reader_path.is_file() and literal in reader_path.read_text(encoding="utf-8"):
                return True
    return False


def unread_files(root: Path = ROOT) -> list[str]:
    """Universe files that are neither linked, read, platform files nor stubs."""
    files = tracked_files(root)
    linked = linked_files(files, root)
    strings = python_strings(files, root)
    joined = "\n".join(strings)
    string_set = set(strings)
    config = config_text(files, root)
    basenames: dict[str, int] = {}
    for f in files:
        b = posixpath.basename(f)
        basenames[b] = basenames.get(b, 0) + 1

    out = []
    for path in universe(files):
        if path in linked or _matches_platform(path) or path in PUBLISHED_STUBS:
            continue
        names = {path}
        if path.startswith("docs/") and path.count("/") >= 2:
            names.add(path[len("docs/") :])
        if any(_mentions(joined, n) for n in names):
            continue
        base = posixpath.basename(path)
        if basenames.get(base) == 1 and base in string_set:
            continue
        if _mentions(config, path):
            continue
        if pattern_reader(path, root):
            continue
        out.append(path)
    return out


def stub_problems(root: Path = ROOT) -> list[str]:
    problems = []
    for path in PUBLISHED_STUBS:
        p = root / path
        if not p.is_file():
            problems.append(f"{path}: published stub is missing")
        elif len(p.read_text(encoding="utf-8").splitlines()) > STUB_MAX_LINES:
            problems.append(f"{path}: published stub is longer than {STUB_MAX_LINES} lines")
        else:
            files = set(tracked_files(root))
            targets = [resolve(path, raw) for _, raw in links(p.read_text(encoding="utf-8"))]
            if not any(t and t.path != path and t.path in files for t in targets):
                problems.append(f"{path}: published stub links no tracked page")
    return problems


# -- link, anchor and code-reference checks --------------------------------------


def markdown_files(files: Sequence[str]) -> list[str]:
    return [f for f in files if f.endswith(".md")]


def link_problems(root: Path = ROOT) -> list[str]:
    """Relative and absolute repository links that name no tracked path, and
    #anchors that name no heading or HTML anchor in their target page."""
    files = tracked_files(root)
    tracked = set(files)
    cache: dict[str, set[str]] = {}
    problems = []
    for page in markdown_files(files):
        for n, raw in links((root / page).read_text(encoding="utf-8")):
            t = resolve(page, raw)
            if t is None:
                continue
            where = f"{page}:{n}: {raw}"
            if t.path not in tracked and not _is_dir(t.path, tracked):
                problems.append(f"{where}: no tracked file or directory {t.path or '.'}")
                continue
            if t.anchor is None:
                continue
            target = t.path
            if target not in tracked:  # a directory: GitHub shows its README
                target = posixpath.join(target, "README.md") if target else "README.md"
            if not target.endswith(".md") or target not in tracked:
                continue  # anchors into code files are line links (#L10)
            if target not in cache:
                cache[target] = anchors((root / target).read_text(encoding="utf-8"))
            if t.anchor.lower() not in cache[target]:
                problems.append(f"{where}: no anchor #{t.anchor} in {target}")
    return problems


_CODE_REF = re.compile(r"`((?:src|tests|scripts|datagen_rs)/[^`\s]+)`")


def code_ref_problems(root: Path = ROOT) -> list[str]:
    """Backticked src/, tests/, scripts/ or datagen_rs/ references in tracked
    markdown that name a missing path, or a symbol (`path:name`) or test
    (`path::name`) the file does not define. `path:123` is a line reference;
    only the path is checked. Globs and placeholders are skipped."""
    files = tracked_files(root)
    tracked = set(files)
    problems = []
    for page in markdown_files(files):
        text = (root / page).read_text(encoding="utf-8")
        for n, line in enumerate(text.splitlines(), 1):
            for m in _CODE_REF.finditer(line):
                tok = m.group(1).rstrip(".,;")
                if _PLACEHOLDER.search(tok):
                    continue
                path, sep, rest = tok.partition(":")
                where = f"{page}:{n}: `{tok}`"
                if path not in tracked and not _is_dir(path, tracked):
                    problems.append(f"{where}: no such path {path}")
                    continue
                if not sep or not rest:
                    continue
                if rest.startswith(":"):
                    # path::test, path::Class, path::Class::test or path::Class.attr
                    name = re.split(r"::|\.|\[", rest[1:], maxsplit=1)[0]
                    if not (root / path).is_file():
                        problems.append(f"{where}: {path} is not a file")
                        continue
                    body = (root / path).read_text(encoding="utf-8")
                    if not re.search(
                        rf"^\s*(?:(?:async\s+)?def|class) {re.escape(name)}\b", body, re.M
                    ):
                        problems.append(f"{where}: no test or class {name} in {path}")
                    continue
                if re.fullmatch(r"\d+(?:[-,]\d+)*", rest):
                    continue
                name = rest.split(".", 1)[0].split("(", 1)[0]
                if not re.fullmatch(r"\w+", name) or not (root / path).is_file():
                    continue
                body = (root / path).read_text(encoding="utf-8", errors="replace")
                defined = rf"^\s*(?:(?:async\s+)?def\s+{re.escape(name)}\b|class\s+{re.escape(name)}\b|{re.escape(name)}\s*[:=]|(?:pub(?:\([^)]*\))?\s+)?(?:fn|struct|enum|const|static|type|trait)\s+{re.escape(name)}\b)"
                if not re.search(defined, body, re.M):
                    problems.append(f"{where}: {path} defines no {name}")
    return problems


# -- --resolve-paths -------------------------------------------------------------

_BACKTICK = re.compile(r"`([^`\s]+)`")
_PLACEHOLDER = re.compile(r"[*?<>{}$\[\]=~]|\.\.\.")
#: A backticked token is taken as a path only when it is absolute, ends in
#: "/", or its last part has one of these extensions; branch names
#: (integrate/v1.5.0) and image names (docker.io/org) are not paths.
_PATH_SUFFIXES = (
    ".md",
    ".py",
    ".sh",
    ".yaml",
    ".yml",
    ".json",
    ".toml",
    ".rs",
    ".txt",
    ".j2",
    ".lock",
    ".cfg",
    ".ini",
    ".html",
    ".csv",
    ".diff",
    ".patch",
)


def _clean(token: str) -> str:
    token = token.split("#", 1)[0].rstrip(".,;")
    # file:line, file:line-line, file:symbol, file:symbol() and file::test
    return re.sub(r"(?<=\w):{1,2}[\w.()\-]*$", "", token).rstrip(":")


def path_tokens(text: str) -> list[tuple[int, str, bool]]:
    """(line, token, is_link) for every backticked token that looks like a
    path and every relative link target; globs and placeholders are skipped."""
    out = []
    for n, line in enumerate(text.splitlines(), 1):
        for m in _BACKTICK.finditer(line):
            c = _clean(m.group(1))
            if not c or "://" in c or _PLACEHOLDER.search(c) or c.startswith("-"):
                continue
            if "/" in c and (c.startswith("/") or c.endswith("/") or c.endswith(_PATH_SUFFIXES)):
                out.append((n, c, False))
        for m in _LINK.finditer(line):
            c = _clean(m.group(1))
            if not c or re.match(r"^[a-z][a-z0-9+.-]*:", c, re.I) or _PLACEHOLDER.search(c):
                continue
            out.append((n, c, True))
    return out


def platform_problems(root: Path = ROOT) -> list[str]:
    """Every non-glob platform file exists, except the pending ones."""
    out = []
    for rel in PLATFORM_FILES:
        if any(c in rel for c in "*?[") or rel in PENDING_PLATFORM_FILES:
            continue
        if not (root / rel).is_file():
            out.append(f"{rel}: platform file missing")
    return out


def _split_bases(bases: Sequence[Path | str]) -> tuple[list[Path], list[tuple[str, Path]]]:
    plain: list[Path] = []
    mapped: list[tuple[str, Path]] = []
    for b in bases:
        text = str(b)
        if "=" in text and not Path(text).exists():
            prefix, _, d = text.partition("=")
            mapped.append((prefix, Path(d)))
        else:
            plain.append(Path(text))
    return plain, mapped


def unresolved_paths(
    path: Path, bases: Sequence[Path | str], ignore: Sequence[str] = ()
) -> list[str]:
    """Paths in a local file that do not exist. A link resolves against the
    file's own directory; a backticked path is absolute or relative to one of
    *bases*. A base written ``PREFIX=DIR`` takes only the tokens starting
    with PREFIX, and those resolve under DIR alone, so a stale path cannot
    resolve in another tree."""
    plain, mapped = _split_bases(bases)
    out = []
    for n, tok, is_link in path_tokens(path.read_text(encoding="utf-8")):
        if any(re.search(pat, tok) for pat in ignore):
            continue
        p = Path(tok)
        if p.is_absolute():
            ok = p.exists()
        else:
            # The longest matching prefix owns the token.
            owner = [
                d
                for prefix, d in sorted(mapped, key=lambda pd: -len(pd[0]))
                if tok.startswith(prefix)
            ]
            roots = [path.parent] if is_link else (owner[:1] or plain)
            ok = any((b / tok).exists() for b in roots)
        if not ok:
            out.append(f"{path}:{n}: {tok}: no such path")
    return out


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--resolve-paths", metavar="FILE", type=Path, action="append")
    parser.add_argument(
        "--base",
        metavar="[PREFIX=]DIR",
        action="append",
        help="base for backticked paths; PREFIX=DIR takes only tokens starting with PREFIX",
    )
    parser.add_argument("--ignore", metavar="REGEX", action="append", default=[])
    args = parser.parse_args(argv)
    if args.resolve_paths:
        bases = args.base or [str(ROOT)]
        bad = [b for f in args.resolve_paths for b in unresolved_paths(f, bases, args.ignore)]
        for line in bad:
            print(line)
        return 1 if bad else 0
    problems = (
        [
            f"{p}: not linked from the docs index and not read by code, tests or CI"
            for p in unread_files(ROOT)
            if p not in KNOWN_ORPHANS
        ]
        + stub_problems(ROOT)
        + platform_problems(ROOT)
    )
    for line in problems:
        print(line)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
