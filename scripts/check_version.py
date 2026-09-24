#!/usr/bin/env python3
"""Check that the package version has one source and matches a release tag.

The version lives only in ``src/lakebench/__init__.py``; ``pyproject.toml``
declares it dynamic and hatch reads it from there. This script fails if
pyproject grows a static version again, and, given ``--tag``, if the tag
does not name exactly that version or names a ``.dev`` build.

Usage:
    python scripts/check_version.py              # consistency only
    python scripts/check_version.py --tag v1.6.0 # also check a release tag
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from packaging.version import InvalidVersion, Version

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10; tomli is in the dev extra there
    import tomli as tomllib  # type: ignore[no-redef]

ROOT = Path(__file__).resolve().parents[1]
INIT = ROOT / "src" / "lakebench" / "__init__.py"
PYPROJECT = ROOT / "pyproject.toml"

_VERSION_RE = re.compile(r'^__version__\s*=\s*["\']([^"\']+)["\']', re.MULTILINE)


def package_version(init_path: Path = INIT) -> str:
    """Return ``__version__`` from the package ``__init__`` without importing it."""
    match = _VERSION_RE.search(init_path.read_text())
    if not match:
        raise ValueError(f"no __version__ literal in {init_path}")
    return match.group(1)


def check(
    tag: str | None = None,
    init_path: Path = INIT,
    pyproject_path: Path = PYPROJECT,
) -> list[str]:
    """Return a list of problems; empty means consistent."""
    problems: list[str] = []
    try:
        version = package_version(init_path)
    except ValueError as exc:
        return [str(exc)]

    pyproject = tomllib.loads(pyproject_path.read_text())
    project = pyproject.get("project", {})
    if "version" in project:
        problems.append(
            f"pyproject.toml has a static version {project['version']!r}; "
            "the version must come only from src/lakebench/__init__.py"
        )
    if "version" not in project.get("dynamic", []):
        problems.append('pyproject.toml [project] must declare dynamic = ["version"]')
    hatch_rel = pyproject.get("tool", {}).get("hatch", {}).get("version", {}).get("path", "")
    if not hatch_rel or (pyproject_path.parent / hatch_rel).resolve() != init_path.resolve():
        problems.append("[tool.hatch.version] path must point at src/lakebench/__init__.py")

    try:
        parsed = Version(version)
    except InvalidVersion:
        problems.append(f"package version {version!r} is not a valid PEP 440 version")
        return problems

    if tag is not None:
        raw = tag.removeprefix("refs/tags/")
        if not raw.startswith("v") or raw.startswith("vv"):
            problems.append(f"tag {tag!r} must be 'v' followed by the version, e.g. v{parsed}")
            return problems
        try:
            tag_version = Version(raw[1:])
        except InvalidVersion:
            problems.append(f"tag {tag!r} is not 'v' plus a valid PEP 440 version")
            return problems
        # Versions compare equal after normalisation (1.6 == 1.6.0), so the
        # tag must also be spelt exactly as the normalised package version;
        # otherwise v1.6, v1.6.0 and v1.6.0.0 could all be cut for one release.
        if tag_version != parsed or raw[1:] != str(parsed):
            problems.append(
                f"tag {tag!r} does not match package version {version!r} (expected v{parsed})"
            )
        if parsed.is_devrelease or tag_version.is_devrelease:
            problems.append(f"version {version!r} is a dev release; bump it before tagging")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tag", help="release tag to check, e.g. v1.6.0")
    args = parser.parse_args(argv)
    problems = check(args.tag)
    if problems:
        for p in problems:
            print(f"FAIL: {p}", file=sys.stderr)
        return 1
    print(f"OK: version {package_version()}" + (f" matches tag {args.tag}" if args.tag else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
