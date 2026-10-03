#!/usr/bin/env python3
"""The code side of the 1.7 breaking-changes list (docs/upgrading/breaking-1.7.yaml).

``missing_entries(list_path)`` returns a skeleton for every breaking change
the code shows that has no entry in the list:

- a removed config key (any model's ``_removed_keys``) not in the 1.6.0
  snapshot ``tests/fixtures/removed_keys_1.6.0.txt``;
- an alias, refused command or flag, or aliased flag in
  ``lakebench.cli._aliases``;
- an exit path whose code differs from 1.6 (``ExitPath.v16_code``);
- an ``ImagesConfig`` default that differs from the 1.6.0 snapshot
  ``tests/fixtures/images_defaults_1_6_0.json``;
- a workload identity version that differs from 1.6 (``WORKLOAD_VERSIONS``);
- the changes no table carries, asserted by ``id``: the Polaris default
  client secret and the System and access-path move of the comparability
  ladder.

An entry covers a code-side item when its ``kind`` matches and the item is
its ``subject`` (one string, or a list of related subjects).
The script writes neither UPGRADING-1.7.md nor the CHANGELOG: the fixes
need prose.

Usage:
    python3.11 scripts/upgrading.py missing     # YAML skeletons, one per gap
    python3.11 scripts/upgrading.py snapshot --tree /path/to/v1.6.0/checkout
        # rewrite the two 1.6.0 fixtures from a checkout of the v1.6.0 tag
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import typing
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
LIST = REPO_ROOT / "docs" / "upgrading" / "breaking-1.7.yaml"
REMOVED_SNAPSHOT = REPO_ROOT / "tests" / "fixtures" / "removed_keys_1.6.0.txt"
IMAGES_SNAPSHOT = REPO_ROOT / "tests" / "fixtures" / "images_defaults_1_6_0.json"

#: The commit the v1.6.0 tag names; the snapshots record it.
V160_COMMIT = "b774dad1"

#: 1.6's workload identity versions (metrics/experiment.py at v1.6.0).
WORKLOAD_VERSIONS_1_6 = {"customer360": "c360-1", "financial": "aml-1"}

#: Breaking changes no code table carries, by the ``id`` their entry must have.
TABLELESS = {
    "deploy-generates-the-polaris-client-secret": "deploy generates the Polaris client secret",
    "system-and-access-path-are-not-execution-conditions": (
        "the comparability ladder's System and access-path move"
    ),
}

KINDS = (
    "removed-key",
    "refused-command",
    "alias",
    "exit-code",
    "identity",
    "version-bump",
    "default-change",
)


# -- the schema, on whichever tree is importable ---------------------------------


def _models_in(annotation: Any) -> list[type]:
    from pydantic import BaseModel

    args = typing.get_args(annotation) or (annotation,)
    return [a for a in args if isinstance(a, type) and issubclass(a, BaseModel)]


def _is_block(annotation: Any) -> bool:
    return bool(_models_in(annotation)) and typing.get_origin(annotation) not in (dict, list)


def removed_keys() -> list[str]:
    """Every removed key of the importable schema, dotted from the root."""
    from lakebench.config.schema import LakebenchConfig

    out: set[str] = set()

    def walk(model: type, prefix: str) -> None:
        for key in getattr(model, "_removed_keys", {}):
            out.add(f"{prefix}{key}")
        for name, field in model.model_fields.items():
            if _is_block(field.annotation):
                walk(_models_in(field.annotation)[0], f"{prefix}{field.alias or name}.")

    walk(LakebenchConfig, "")
    return sorted(out)


def image_defaults() -> dict[str, str]:
    from lakebench.config.schema import ImagesConfig

    out: dict[str, str] = {}
    for name, field in ImagesConfig.model_fields.items():
        d = field.default
        d = getattr(d, "value", d)  # an enum default (pull_policy) by its value
        if isinstance(d, str):
            out[name] = d
    return out


# -- the snapshots ----------------------------------------------------------------


def _read_snapshot_lines(path: Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]


def snapshot_commit(path: Path) -> str | None:
    for line in path.read_text().splitlines():
        if line.startswith("# commit: "):
            return line.split(": ", 1)[1].strip()
    return None


def removed_keys_1_6() -> set[str]:
    return set(_read_snapshot_lines(REMOVED_SNAPSHOT))


def image_defaults_1_6() -> dict[str, str]:
    data = json.loads(IMAGES_SNAPSHOT.read_text())
    return dict(data["defaults"])


def write_snapshots(tree: Path) -> None:
    """Rewrite both fixtures from a checkout of the v1.6.0 tag at *tree*."""
    head = subprocess.run(
        ["git", "-C", str(tree), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()
    if not head.startswith(V160_COMMIT):
        raise SystemExit(f"{tree} is at {head[:12]}, not the v1.6.0 commit {V160_COMMIT}")
    code = (
        "import json, sys; sys.path.insert(0, sys.argv[1]); sys.path.insert(0, sys.argv[2]);"
        "import upgrading as u;"
        "print(json.dumps({'removed': u.removed_keys(), 'images': u.image_defaults()}))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code, str(tree / "src"), str(REPO_ROOT / "scripts")],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    data = json.loads(out)
    REMOVED_SNAPSHOT.write_text(
        f"# Removed config keys at v1.6.0, dotted from the root (scripts/upgrading.py snapshot).\n"
        f"# commit: {head[:8]}\n" + "".join(f"{k}\n" for k in data["removed"])
    )
    IMAGES_SNAPSHOT.write_text(
        json.dumps({"commit": head[:8], "defaults": data["images"]}, indent=2, sort_keys=True)
        + "\n"
    )


# -- the comparison ---------------------------------------------------------------


def load_list(path: Path = LIST) -> list[dict[str, Any]]:
    import yaml

    data = yaml.safe_load(path.read_text()) if path.exists() else None
    return list(data or [])


def code_items() -> list[dict[str, str]]:
    """Every breaking change the code shows, as ``{kind, subject, hint}``."""
    from lakebench.cli import _aliases as al
    from lakebench.exit_codes import PATHS
    from lakebench.metrics.experiment import WORKLOAD_VERSIONS

    items: list[dict[str, str]] = []
    old = removed_keys_1_6()
    for key in removed_keys():
        if key not in old:
            items.append({"kind": "removed-key", "subject": key, "hint": "removed config key"})
    for name, a in al.ALIASES.items():
        items.append({"kind": "alias", "subject": name, "hint": f"now `{a.target}`"})
    for command, flags in al.ALIASED_FLAGS.items():
        for flag, fa in flags.items():
            items.append({"kind": "alias", "subject": f"{command} {flag}", "hint": fa.note})
    for name, r in al.REFUSED.items():
        items.append({"kind": "refused-command", "subject": name, "hint": r.reason})
    for command, flags in al.REFUSED_FLAGS.items():
        for flag, r in flags.items():
            items.append(
                {"kind": "refused-command", "subject": f"{command} {flag}", "hint": r.reason}
            )
    for p in PATHS:
        if p.live and p.v16_code is not None and p.v16_code != int(p.code):
            items.append(
                {
                    "kind": "exit-code",
                    "subject": p.name,
                    "hint": f"{p.v16_code} -> {int(p.code)}: {p.when}",
                }
            )
    old_images = image_defaults_1_6()
    for name, value in image_defaults().items():
        if old_images.get(name) != value:
            items.append(
                {
                    "kind": "version-bump",
                    "subject": f"images.{name}",
                    "hint": f"{old_images.get(name)} -> {value}",
                }
            )
    for schema, version in WORKLOAD_VERSIONS.items():
        # A workload 1.6 did not have is new, not a change.
        if schema in WORKLOAD_VERSIONS_1_6 and WORKLOAD_VERSIONS_1_6[schema] != version:
            items.append(
                {
                    "kind": "identity",
                    "subject": f"workload {schema}",
                    "hint": f"{WORKLOAD_VERSIONS_1_6.get(schema)} -> {version}",
                }
            )
    return items


def subjects(entry: dict[str, Any]) -> list[str]:
    """An entry's subjects: ``subject`` is one, or a list of related ones."""
    s = entry.get("subject")
    return [str(x) for x in s] if isinstance(s, list) else [str(s)]


def missing_entries(list_path: Path = LIST) -> list[dict[str, str]]:
    """Code-side breaking changes with no entry in the list."""
    entries = load_list(list_path)
    covered = {(e.get("kind"), subject) for e in entries for subject in subjects(e)}
    ids = {e.get("id") for e in entries}
    out = [i for i in code_items() if (i["kind"], i["subject"]) not in covered]
    for entry_id, what in TABLELESS.items():
        if entry_id not in ids:
            out.append({"kind": "default-change", "subject": entry_id, "hint": what})
    return out


def slug(title: str) -> str:
    """GitHub's heading anchor: an entry's ``id`` is the slug of its ``title``."""
    import re

    return re.sub(r"[^a-z0-9 _-]", "", title.lower()).replace(" ", "-")


def skeleton(item: dict[str, str]) -> str:
    """A list entry to paste and finish: replace every TODO (and the title's
    words, then set ``id`` to the new title's slug)."""
    title = f"TODO {item['subject']}"
    return (
        f"- id: {slug(title)}\n"
        f"  title: {json.dumps(title)}\n"
        f"  kind: {item['kind']}\n"
        f"  subject: {json.dumps(item['subject'])}\n"
        f"  source: TODO  # path:symbol of the code\n"
        f"  text: TODO  # one line; also its CHANGELOG bullet ({item['hint']})\n"
        f"  fix: TODO\n"
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("missing", help="print a YAML skeleton for each missing entry")
    snap = sub.add_parser("snapshot", help="rewrite the 1.6.0 fixtures from a v1.6.0 checkout")
    snap.add_argument("--tree", type=Path, required=True)
    args = ap.parse_args(argv)
    if args.cmd == "snapshot":
        write_snapshots(args.tree)
        print(
            f"wrote {REMOVED_SNAPSHOT.relative_to(REPO_ROOT)}, {IMAGES_SNAPSHOT.relative_to(REPO_ROOT)}"
        )
        return 0
    sys.path.insert(0, str(REPO_ROOT / "src"))
    gaps = missing_entries()
    for item in gaps:
        print(skeleton(item))
    return 1 if gaps else 0


if __name__ == "__main__":
    raise SystemExit(main())
