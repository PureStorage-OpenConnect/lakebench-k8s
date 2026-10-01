#!/usr/bin/env python3
"""Check the runtime of every GitHub Action the workflows use.

``.github/action-runtimes.json`` maps each ``owner/repo[/path]@<sha>`` named in
a ``uses:`` of ``.github/workflows/*.yml`` to its ``runs.using`` value
(``node24``, ``composite``, ``docker``). GitHub retires old Node runtimes on
its own schedule, so a Node 20 action keeps working until the day it does
not; this makes the runtime visible and checked.

Without flags the check is offline: every action is pinned by a 40-hex SHA,
every one has a map entry, the map has no stale entries, and no entry is a
retired Node runtime (node12, node16, node20).

``--verify`` also reads ``action.yml`` (or ``action.yaml``) at each pinned SHA
through the GitHub contents API and fails when the map differs from what
GitHub serves. It needs ``GITHUB_TOKEN`` (or ``GH_TOKEN``). Without one it
prints SKIP for that part and exits on the offline result, except under
GitHub Actions (``GITHUB_ACTIONS=true``), where a missing token is a failure
so the CI step cannot pass without reading anything.

Only the actions named in the workflows are checked. An action that a
composite action calls internally (pypa/gh-action-pypi-publish calls
actions/setup-python) is not seen here.

Usage:
    python scripts/check_action_runtimes.py [--verify]
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
RUNTIMES = ROOT / ".github" / "action-runtimes.json"

RETIRED_RUNTIMES = frozenset({"node12", "node16", "node20"})
_SHA = re.compile(r"^[0-9a-f]{40}$")


def workflow_uses(workflows: Path = WORKFLOWS) -> dict[str, list[str]]:
    """Map each remote action ref used in the workflows to where it is used.

    Local actions and reusable workflows (``./...``) and ``docker://`` images
    are not GitHub Actions with an ``action.yml`` at a SHA, so they are left
    out.
    """
    found: dict[str, list[str]] = {}
    for wf_path in sorted(workflows.glob("*.y*ml")):
        wf = yaml.safe_load(wf_path.read_text()) or {}
        for job_name, job in (wf.get("jobs") or {}).items():
            refs = [job.get("uses")] + [s.get("uses") for s in job.get("steps") or []]
            for ref in refs:
                if not ref or ref.startswith(("./", "docker://")):
                    continue
                found.setdefault(ref, []).append(f"{wf_path.name}:{job_name}")
    return found


def split_ref(ref: str) -> tuple[str, str, str, str]:
    """``owner/repo[/path]@ref`` -> (owner, repo, path, ref)."""
    name, _, at = ref.partition("@")
    owner, repo, *path = name.split("/")
    return owner, repo, "/".join(path), at


def load_runtimes(path: Path = RUNTIMES) -> dict[str, str]:
    data: dict[str, str] = json.loads(path.read_text())
    return data


def offline_problems(uses: dict[str, list[str]], runtimes: dict[str, str]) -> list[str]:
    problems: list[str] = []
    for ref, where in sorted(uses.items()):
        at = split_ref(ref)[3]
        if not _SHA.match(at):
            problems.append(f"{ref} ({', '.join(where)}): not pinned by a 40-hex commit SHA")
        if ref not in runtimes:
            problems.append(f"{ref} ({', '.join(where)}): no entry in {RUNTIMES.name}")
    for ref in sorted(set(runtimes) - set(uses)):
        problems.append(f"{ref}: in {RUNTIMES.name} but not used by any workflow")
    for ref, using in sorted(runtimes.items()):
        if using in RETIRED_RUNTIMES:
            problems.append(f"{ref}: runs on {using}, which GitHub has retired or is retiring")
    return problems


def fetch_using(ref: str, token: str) -> str:
    """Read ``runs.using`` from the action's metadata file at the pinned ref."""
    owner, repo, path, at = split_ref(ref)
    for name in ("action.yml", "action.yaml"):
        file_path = f"{path}/{name}" if path else name
        url = f"https://api.github.com/repos/{owner}/{repo}/contents/{file_path}?ref={at}"
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 (fixed https host)
                body = json.load(resp)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                continue
            raise
        meta = yaml.safe_load(base64.b64decode(body["content"]))
        return str(meta["runs"]["using"])
    raise LookupError(f"{ref}: no action.yml or action.yaml at {at}")


def verify_problems(runtimes: dict[str, str], fetch: Callable[[str], str]) -> list[str]:
    problems: list[str] = []
    for ref, recorded in sorted(runtimes.items()):
        try:
            actual = fetch(ref)
        except (LookupError, urllib.error.URLError, KeyError, TypeError) as exc:
            problems.append(f"{ref}: could not read runs.using ({exc})")
            continue
        if actual != recorded:
            problems.append(f"{ref}: map says {recorded}, action.yml says {actual}")
        else:
            print(f"ok   {ref}  {actual}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--verify",
        action="store_true",
        help="also read action.yml at each pinned SHA from GitHub (needs GITHUB_TOKEN)",
    )
    args = parser.parse_args(argv)

    runtimes = load_runtimes()
    problems = offline_problems(workflow_uses(), runtimes)

    if args.verify:
        token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if not token and os.environ.get("GITHUB_ACTIONS") == "true":
            problems.append("--verify under GitHub Actions needs GITHUB_TOKEN in the step env")
        elif not token:
            print("SKIP: --verify needs GITHUB_TOKEN; runtimes not read from GitHub")
        else:
            problems += verify_problems(runtimes, lambda ref: fetch_using(ref, token))

    for problem in problems:
        print(f"FAIL {problem}")
    if problems:
        return 1
    print(f"{len(runtimes)} actions pinned by SHA, none on a retired Node runtime")
    return 0


if __name__ == "__main__":
    sys.exit(main())
