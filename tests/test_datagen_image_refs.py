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

# The image's own name, bare (``lb-datagen:x``) or under docker.io/sillidata,
# with ``-rs`` for the retired repository. A reference under another registry
# (``your-registry/lb-datagen:custom``) is a placeholder, not a pin.
_REF = re.compile(
    r"(?:(?<=[\s`'\"(=])|^)(?:docker\.io/)?(?:sillidata/)?lb-datagen(?:-rs)?[:@][A-Za-z0-9._:@-]+",
    re.M,
)

#: Path pattern -> why an old or floating tag is allowed there.
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
    "docs/perf-regression-gate.md": "history: the earlier baselines ran a floating tag",
    "docs/benchmarks/C360.md": "names the deleted image a recorded run used",
    "docs/reproductions/c360-scale-0-1.yaml": "legacy package `reproduce` refuses; kept as history",
    "scripts/check_doc_overlap.py": "comments on whole-token matching of a versioned name",
    "tests/test_doc_overlap.py": "whole-token matching cases (1.6.01, 1.6.0.1) on purpose",
}


def _allowed_values() -> set[str]:
    default = ImagesConfig().datagen
    tag_ref, digest = default.split("@", 1)
    tag = tag_ref.rsplit(":", 1)[1]
    values = {default, tag_ref, f"docker.io/sillidata/lb-datagen@{digest}"}
    for name in ("lb-datagen", "sillidata/lb-datagen", "docker.io/sillidata/lb-datagen"):
        values |= {f"{name}:{tag}", f"{name}:1.6.0"}
    return values


def _allowed(ref: str, values: set[str], digest: str) -> bool:
    ref = ref.rstrip(".,;)")
    if ref in values:
        return True
    if ref.endswith("@sha256:"):
        # Code or prose that builds a digest reference (``... + "0" * 64``,
        # ``@sha256:<digest above>``), not a pin.
        return True
    # A shortened digest in prose (``@sha256:48e18a41...``).
    m = re.fullmatch(r"(?:docker\.io/)?(?:sillidata/)?lb-datagen@(sha256:[0-9a-f]{8,})", ref)
    return bool(m and digest.startswith(m.group(1)))


def stale_refs(root: Path, files: list[str]) -> list[str]:
    """``path:line: ref`` for every datagen image reference that is neither
    the default, 1.6.0, nor in an allowlisted file."""
    values = _allowed_values()
    digest = ImagesConfig().datagen.split("@", 1)[1]
    out = []
    for rel in files:
        if any(fnmatch.fnmatch(rel, pat) for pat in ALLOWLIST):
            continue
        try:
            text = (root / rel).read_text()
        except (UnicodeDecodeError, OSError):
            continue
        for m in _REF.finditer(text):
            if not _allowed(m.group(0), values, digest):
                line = text.count("\n", 0, m.start()) + 1
                out.append(f"{rel}:{line}: {m.group(0)}")
    return out


def _tracked() -> list[str]:
    try:
        r = subprocess.run(
            ["git", "-C", str(ROOT), "ls-files"], capture_output=True, text=True, check=True
        )
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("not a git checkout")
    return [f for f in r.stdout.splitlines() if (ROOT / f).is_file()]


def test_no_stale_datagen_image_reference():
    assert stale_refs(ROOT, _tracked()) == []


def test_allowlist_entries_still_match_a_file():
    files = _tracked()
    for pat in ALLOWLIST:
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
    found = stale_refs(tmp_path, ["docs/ok.md", "perf.yaml", "job.yaml", "CHANGELOG.md"])
    assert found == [
        "perf.yaml:2: docker.io/sillidata/lb-datagen:14c4eee",
        "job.yaml:1: docker.io/sillidata/lb-datagen-rs:latest",
    ]
