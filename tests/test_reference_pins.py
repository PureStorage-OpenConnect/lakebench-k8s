"""The AML reference libraries are pinned once (QA-8).

The cluster job installs ``REFERENCE_PY_DEPS`` (``job.py``) on the driver; the
``[aml]`` extra pins the same versions for local and CI runs, and
``scripts/aml_gate.py`` refuses a local gate run on any other version. These
tests fail when the two lists drift, and when the installed libraries are not
the pinned ones, so a CI run never reports AML numbers from a different
scikit-learn than the cluster uses.

The installed-version check fails in CI (``GITHUB_ACTIONS=true``) or with
``LB_REQUIRE_REFERENCE_PINS=1``. Elsewhere a drifted install is reported as a
skip naming every drifted or missing package, because a shared development host whose
system Python carries newer libraries would otherwise fail every local suite
run at this file.
"""

from __future__ import annotations

import ast
import importlib
import os
import sys
from importlib import metadata
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - 3.10 only
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]
JOB_PY = ROOT / "src/lakebench/modules/pipeline_engines/spark/job.py"
AML_GATE = ROOT / "scripts/aml_gate.py"

#: The packages whose version changes a reference-model number. The rest of
#: REFERENCE_PY_DEPS are pure-Python helpers of pandas.
CHECKED = {"numpy", "scipy", "pandas", "scikit-learn", "joblib", "threadpoolctl"}
_MODULE_OF = {"scikit-learn": "sklearn"}


def _module_assign(path: Path, name: str) -> ast.expr:
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return node.value
    raise AssertionError(f"{name} not found at module level in {path}")


def reference_py_deps() -> dict[str, str]:
    """REFERENCE_PY_DEPS read from job.py source, as {package: version}."""
    node = _module_assign(JOB_PY, "REFERENCE_PY_DEPS")
    pins = ast.literal_eval(node)
    out = {}
    for pin in pins:
        name, sep, version = pin.partition("==")
        assert sep, f"REFERENCE_PY_DEPS entry {pin!r} is not an exact pin"
        out[name] = version
    return out


def _extras() -> dict[str, list[str]]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return data["project"]["optional-dependencies"]


def _exact_pins(reqs: list[str]) -> dict[str, str]:
    out = {}
    for line in reqs:
        req = Requirement(line)
        specs = list(req.specifier)
        if len(specs) == 1 and specs[0].operator == "==":
            out[canonicalize_name(req.name)] = specs[0].version
    return out


def pin_drift(aml: list[str], reference: dict[str, str]) -> dict[str, tuple]:
    """{package: (in [aml], in REFERENCE_PY_DEPS)} for each checked package
    whose exact pin differs or is missing on either side."""
    extra = _exact_pins(aml)
    return {
        p: (extra.get(p), reference.get(p))
        for p in sorted(CHECKED)
        if extra.get(p) is None or extra.get(p) != reference.get(p)
    }


def test_aml_extra_equals_reference_py_deps():
    drift = pin_drift(_extras()["aml"], reference_py_deps())
    assert not drift, f"[aml] and REFERENCE_PY_DEPS disagree ([aml], job.py): {drift}"


def test_pin_drift_names_a_drifted_package():
    ref = reference_py_deps()
    aml = [f"{p}=={v}" for p, v in ref.items() if p in CHECKED]
    assert pin_drift(aml, ref) == {}
    drifted = [a if not a.startswith("scikit-learn") else "scikit-learn==1.9.1" for a in aml]
    assert pin_drift(drifted, ref) == {"scikit-learn": ("1.9.1", ref["scikit-learn"])}
    loose = [a if not a.startswith("numpy") else "numpy>=1.26" for a in aml]
    assert set(pin_drift(loose, ref)) == {"numpy"}


def test_dev_takes_the_aml_extra_and_does_not_repin():
    dev = _extras()["dev"]
    assert any(
        Requirement(r).name == "lakebench-k8s" and "aml" in Requirement(r).extras for r in dev
    )
    # A second, looser line for a checked package in [dev] would let the two
    # drift apart again without this file noticing.
    names = {canonicalize_name(Requirement(r).name) for r in dev}
    assert not names & CHECKED, sorted(names & CHECKED)


def test_aml_gate_checks_the_same_packages():
    node = _module_assign(AML_GATE, "_CHECKED")
    assert ast.literal_eval(node) == CHECKED


def installed_drift(reference: dict[str, str], version=metadata.version) -> dict[str, tuple]:
    """{package: (installed, pinned)} for each checked package not installed
    at its pin; a package that is not installed at all shows as None."""
    out: dict[str, tuple] = {}
    for p in sorted(CHECKED):
        try:
            have: str | None = version(p)
        except metadata.PackageNotFoundError:
            have = None
        if have != reference[p]:
            out[p] = (have, reference[p])
    return out


def _require_pins() -> bool:
    return os.environ.get("GITHUB_ACTIONS") == "true" or os.environ.get(
        "LB_REQUIRE_REFERENCE_PINS"
    ) in {"1", "true", "yes"}


def test_installed_reference_libraries_match():
    ref = reference_py_deps()
    drift = installed_drift(ref)
    if drift and not _require_pins():
        pytest.skip(
            "installed AML libraries differ from REFERENCE_PY_DEPS (installed, pinned): "
            f"{drift}; set LB_REQUIRE_REFERENCE_PINS=1 to fail on this"
        )
    assert not drift, f"installed AML libraries differ from REFERENCE_PY_DEPS: {drift}"
    # The distribution metadata could name one version while another copy on
    # sys.path is imported; check what the import gives too.
    imported = {}
    for p in sorted(CHECKED):
        mod = importlib.import_module(_MODULE_OF.get(p, p))
        if getattr(mod, "__version__", None) != ref[p]:
            imported[p] = (getattr(mod, "__version__", None), ref[p])
    assert not imported, f"imported AML libraries differ from REFERENCE_PY_DEPS: {imported}"


def test_installed_drift_detects_a_drifted_install():
    ref = reference_py_deps()
    fake = {**ref, "scikit-learn": "1.9.1"}

    def version(p):
        if p == "threadpoolctl":
            raise metadata.PackageNotFoundError(p)
        return fake[p]

    assert installed_drift(ref, version) == {
        "scikit-learn": ("1.9.1", ref["scikit-learn"]),
        "threadpoolctl": (None, ref["threadpoolctl"]),
    }
