"""Every reference to the Lakebench datagen image in a tracked file names the
default image (``ImagesConfig.datagen``), its tag or its digest, the v1.6
release image 1.6.0 (still in the registry, and the lineage root), or is on
the allowlist below with its reason. A pin to a deleted tag (``14c4eee`` in
the perf configs, ``:latest`` in manual Jobs) fails here instead of at
ImagePullBackOff."""

from __future__ import annotations

import fnmatch
import re
import subprocess
from pathlib import Path

import pytest

from lakebench.config.schema import ImagesConfig

ROOT = Path(__file__).resolve().parents[1]

# The image's name: bare (``lb-datagen:x``) or under the sillidata namespace on
# any registry host, with ``-rs`` for the retired repository; the tag or digest
# runs to the next space, quote or delimiter. A name under another namespace
# (``your-registry/lb-datagen:custom``, ``${REG}/lb-datagen:x``) is a
# placeholder for the user's own build, not a pin. A bare untagged
# ``lb-datagen`` is prose; ``sillidata/lb-datagen`` with no tag pulls
# ``:latest`` and is flagged.
_REF = re.compile(
    r"(?<![\w./${}-])((?:[\w.-]+/)?sillidata/)?(lb-datagen(?:-rs)?)"
    r"((?:[:@][^\s`'\"<>|,()\[\]]*)?)"
)

#: Path pattern -> why any datagen reference is allowed in that file.
ALLOWLIST = {
    "CHANGELOG.md": "release history names the images each release used",
    "tests/fixtures/records/*": "recorded run output names the image that ran",
    "tests/fixtures/reports/*": "reports rendered from those records",
    "tests/fixtures/verdict/*": "recorded run output names the image that ran",
    "uat/runs/*": "checked-in run output names the image that ran",
    "tests/expected/pairs.json": "expected compare output over the recorded runs",
    "tests/test_experiment.py": "a record fixture naming an old image on purpose",
    "tests/test_datagen_robustness.py": "comment on what an old image produced",
    "tests/test_aml_scale_invariance.py": "refusal test feeds a floating tag on purpose",
    "tests/test_root_lakebench_yaml.py": "docstring on the floating tag it guards against",
    "tests/test_datagen_image_refs.py": "this file plants references on purpose",
    "tests/test_aml_protocol_look_image.py": "builds patterns of the default image's tag",
    "tests/test_doc_overlap.py": "whole-token matching cases (1.6.01, 1.6.0.1) on purpose",
}

#: Path -> the exact references (registry prefix dropped) allowed in it, and why.
ALLOW_REFS = {
    "docs/perf-regression-gate.md": ({"lb-datagen:latest"}, "history of the earlier baselines"),
    "docs/benchmarks/C360.md": ({"lb-datagen:034f998"}, "the deleted image a recorded run used"),
    "docs/reproductions/c360-scale-0-1.yaml": (
        {"lb-datagen:latest"},
        "legacy package `reproduce` refuses; kept as history",
    ),
    "scripts/check_doc_overlap.py": ({"lb-datagen:1.6.0.1"}, "whole-token matching comment"),
}


def _allowed(namespaced: bool, name: str, suffix: str) -> bool:
    """The default image (tag, digest or both), 1.6.0 (still in the registry,
    the lineage root), an untagged prose mention, or code building a digest."""
    tag_ref, digest = ImagesConfig().datagen.split("@", 1)
    tag = tag_ref.rsplit(":", 1)[1]
    if name != "lb-datagen":
        return False  # the retired lb-datagen-rs repository
    suffix = suffix.rstrip(".,;)")
    if suffix == "":
        return not namespaced
    if suffix in {f":{tag}", f":{tag}@{digest}", f"@{digest}", ":1.6.0"}:
        return True
    if suffix in {"@sha256:", f":{tag}@sha256:"}:
        # ``... + "0" * 64`` or ``@sha256:<digest above>``: builds a digest.
        return True
    # A shortened digest of the default in prose (``@sha256:48e18a41...``).
    m = re.fullmatch(rf"(?::{tag})?@(sha256:[0-9a-f]{{8,}})\.*", suffix)
    return bool(m and digest.startswith(m.group(1)))


def stale_refs(root: Path, files: list[str]) -> list[str]:
    """``path:line: ref`` for every datagen image reference that is not
    allowed (see ``_allowed``, ALLOWLIST and ALLOW_REFS)."""
    out = []
    for rel in files:
        if any(fnmatch.fnmatch(rel, pat) for pat in ALLOWLIST):
            continue
        extra = ALLOW_REFS.get(rel, (set(), ""))[0]
        try:
            text = (root / rel).read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue  # binary
        for m in _REF.finditer(text):
            namespaced, name, suffix = bool(m.group(1)), m.group(2), m.group(3)
            if _allowed(namespaced, name, suffix) or f"{name}{suffix}".rstrip(".,;)") in extra:
                continue
            line = text.count("\n", 0, m.start()) + 1
            out.append(f"{rel}:{line}: {m.group(0)}")
    return out


def _tracked() -> list[str]:
    try:
        r = subprocess.run(
            ["git", "-C", str(ROOT), "ls-files"], capture_output=True, text=True, check=True
        )
    except (OSError, subprocess.CalledProcessError) as e:
        pytest.fail(f"the drift test needs a git checkout: {e}")
    return [f for f in r.stdout.splitlines() if (ROOT / f).is_file()]


def test_no_stale_datagen_image_reference():
    assert stale_refs(ROOT, _tracked()) == []


def test_allowlist_entries_still_match_a_file():
    files = _tracked()
    for pat in [*ALLOWLIST, *ALLOW_REFS]:
        assert any(fnmatch.fnmatch(f, pat) for f in files), f"stale allowlist entry {pat}"


def test_drift_fails_on_planted_reference(tmp_path):
    default = ImagesConfig().datagen
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "ok.md").write_text(
        f"Pin `{default}`, or `lb-datagen:1.6.0`; build `your-registry/lb-datagen:custom`.\n"
        f"Pulled as docker.io/sillidata/lb-datagen@{default.split('@')[1][:15]}...\n"
    )
    (tmp_path / "perf.yaml").write_text(
        "images:\n  datagen: docker.io/sillidata/lb-datagen:14c4eee\n"
    )
    (tmp_path / "job.yaml").write_text("image: docker.io/sillidata/lb-datagen-rs:latest\n")
    (tmp_path / "CHANGELOG.md").write_text("- was `lb-datagen:v3`\n")
    (tmp_path / "more.yaml").write_text(
        "a: docker.io/sillidata/lb-datagen\n"  # untagged: pulls :latest
        "b: index.docker.io/sillidata/lb-datagen:gone\n"
        "c,lb-datagen:gone\n"
        "|lb-datagen:gone|\n"
        "e:docker.io/sillidata/lb-datagen:gone@sha256:\n"
        f"f: docker.io/sillidata/lb-datagen:gone@{default.split('@')[1]}\n"
        "the lb-datagen image\n"  # prose, not a pin
    )
    files = ["docs/ok.md", "perf.yaml", "job.yaml", "CHANGELOG.md", "more.yaml"]
    found = [f.split(": ", 1)[0] for f in stale_refs(tmp_path, files)]
    assert found == [
        "perf.yaml:2",
        "job.yaml:1",
        "more.yaml:1",
        "more.yaml:2",
        "more.yaml:3",
        "more.yaml:4",
        "more.yaml:5",
        "more.yaml:6",
    ]
