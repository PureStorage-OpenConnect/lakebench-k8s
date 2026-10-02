"""scripts/frozen_guard.py on a temporary git repository shaped like the
real one (design 07 section 7.5, plus the pre-build review's cases).

The guard's hashes are defined on Python 3.11 only, so these tests skip on
any other interpreter; the CI frozen-guard job runs them on 3.11 and fails
on a skip.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from tests.conftest import exec_repo_script

REPO = Path(__file__).resolve().parents[1]


def _load_guard(name: str):
    return exec_repo_script(REPO / "scripts" / "frozen_guard.py", name)


pytestmark = pytest.mark.skipif(
    sys.version_info[:2] != (3, 11), reason="frozen guard hashes are defined on Python 3.11"
)

SCRIPTS = "src/lakebench/spark/scripts"
DDL = "src/lakebench/deploy/financial_ddl.py"
MAPS = "src/lakebench/modules/pipeline_engines/spark/scripts_maps.py"

FILES = {
    "src/lakebench/__init__.py": "",
    "src/lakebench/aml/__init__.py": "",
    "src/lakebench/aml/shards.py": """
        def check_plan(x):
            return x + 1
    """,
    f"{SCRIPTS}/common.py": '''
        """Fixture common."""
        import os

        _RUNS = set()


        def env(name, default=None):
            """Read an env var."""
            return os.getenv(name, default)


        def aml_opening_balance(x):
            """Opening balance."""
            return abs(x) % 200 + _OFFSET


        _OFFSET = 10


        def replay_possible(run):
            if run in _RUNS:
                return False
            return True


        def unrelated_helper():
            return 1
    ''',
    f"{SCRIPTS}/detection_rules.py": """
        _STRUCTURING_THRESHOLDS = {"USD": 10000}


        def reader():
            return _STRUCTURING_THRESHOLDS["USD"]
    """,
    f"{SCRIPTS}/frozen_a.py": '''
        """A frozen script."""
        from common import aml_opening_balance, env

        CATALOG = env("LB_CATALOG", "lh")


        def main():
            from detection_rules import _STRUCTURING_THRESHOLDS

            try:
                from lakebench.aml.shards import check_plan
            except ImportError:
                from shards import check_plan
            return aml_opening_balance(1) + _STRUCTURING_THRESHOLDS["USD"] + check_plan(1)
    ''',
    DDL: '''
        SILVER_X_DDL = """
        CREATE TABLE {catalog}.{table} (a INT)
        """.strip()
        GOLD_Y_DDL = """
        CREATE TABLE {catalog}.{table} (b INT)
        """.strip()


        def render_ddl(t, catalog, table):
            return t.format(catalog=catalog, table=table)
    ''',
    MAPS: """
        def _src(path):
            return path


        def _scripts(*names):
            return tuple(_src(f"spark/scripts/{n}") for n in names)


        SCRIPT_MAPS = {
            "common": _scripts("common.py"),
            "jobs": _scripts("detection_rules.py", "frozen_a.py"),
        }
    """,
    "datagen_rs/src/a.rs": "fn a() {}\n",
    "datagen_rs/src/b.rs": "fn b() {}\n",
    "datagen_rs/entrypoint.py": "print('datagen')\n",
    ".github/workflows/ci.yml": """
        jobs:
          aml-parity:
            runs-on: ubuntu-24.04
            steps:
              - run: pytest tests/spark -m aml_parity
    """,
    "src/lakebench/spark/data/aml/aml_level2_predictions.json": '{"predictions": []}\n',
    "src/lakebench/spark/data/aml/aml_registered_looks.json": '{"looks": []}\n',
    "src/lakebench/spark/data/aml/aml_preregistration.json": '{"corpora": {"spent_seeds": [1]}}\n',
}


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), "-c", "user.name=t", "-c", "user.email=t@t", *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


@pytest.fixture
def guard(monkeypatch):
    g = _load_guard("frozen_guard_under_test")
    monkeypatch.setattr(
        g,
        "REQUIRED_FILES",
        (
            f"{SCRIPTS}/frozen_a.py",
            "datagen_rs/entrypoint.py",
            "src/lakebench/spark/data/aml/aml_preregistration.json",
        ),
    )
    monkeypatch.setattr(g, "REQUIRED_GLOBS", ("datagen_rs/src/**",))
    monkeypatch.setattr(g, "REQUIRED_PYATTRS", ((DDL, ("SILVER_X_DDL",)),))
    monkeypatch.setattr(g, "REQUIRED_PYSYMS", ((DDL, "render_ddl"), (MAPS, "SCRIPT_MAPS")))
    monkeypatch.setattr(g, "REQUIRED_CI_JOBS", ("aml-parity",))
    return g


class Repo:
    def __init__(self, root: Path, g):
        self.root = root
        self.g = g

    def write(self, path: str, text: str) -> None:
        p = self.root / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(textwrap.dedent(text).lstrip("\n"))

    def edit(self, path: str, old: str, new: str) -> None:
        p = self.root / path
        text = p.read_text()
        assert old in text, (path, old)
        p.write_text(text.replace(old, new, 1))

    def regen(self) -> list[str]:
        return self.g.regen(self.root)

    def commit(self, msg: str, trailer: str | None = None) -> str:
        _git(self.root, "add", "-A")
        body = msg if trailer is None else f"{msg}\n\nFreeze-cost: {trailer}"
        _git(self.root, "commit", "-q", "--allow-empty", "-m", body)
        return _git(self.root, "rev-parse", "HEAD").strip()

    def tree_problems(self) -> list[str]:
        tree = self.g.WorkTree(self.root)
        return self.g.tree_problems(tree, self.g.load_list(tree))[0]

    def range_problems(self, parity="success", evidence=None, require_all=False):
        ev = evidence or (lambda c: (True, ""))
        return self.g.range_problems(
            self.root, "HEAD", parity, None, None, require_all=require_all, evidence=ev
        )[0]

    def history_problems(self) -> list[str]:
        return self.g.history_problems(self.root)

    def list_doc(self) -> dict:
        return json.loads((self.root / self.g.LIST_PATH).read_text())

    def save_list(self, doc: dict) -> None:
        path = self.root / self.g.LIST_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(doc, indent=1) + "\n")


@pytest.fixture
def frozen_repo(tmp_path, guard):
    root = tmp_path / "repo"
    root.mkdir()
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    repo = Repo(root, guard)
    for path, text in FILES.items():
        repo.write(path, text)
    repo.commit("base")
    doc = _empty_list()
    doc["dynamic_ok"] = [
        {
            "path": f"{SCRIPTS}/common.py",
            "what": "replay_possible:_RUNS",
            "occurrence": 1,
            "reason": "fixture run-time state",
        },
    ]
    repo.save_list(doc)
    assert repo.regen() == []
    repo.commit("frozen list", "None")
    assert repo.tree_problems() == []
    return repo


def _empty_list() -> dict:
    return {
        "schema": 2,
        "state": "open",
        "locked": None,
        "ast_python": "3.11",
        "dynamic_ok": [],
        "entries": [],
    }


def _mutation_fixture_problems(repo):
    return [p for p in repo.tree_problems() if "mutates pinned" in p]


# --- the closure -----------------------------------------------------------


def test_closure_pins_imported_symbols_and_their_module_names(frozen_repo, guard):
    keys = {(e["kind"], e.get("path"), e.get("name")) for e in frozen_repo.list_doc()["entries"]}
    common = f"{SCRIPTS}/common.py"
    assert ("pysym", common, "aml_opening_balance") in keys
    assert ("pysym", common, "_OFFSET") in keys  # referenced by a pinned function
    assert ("pysym", common, "env") in keys
    assert ("pysym", common, "unrelated_helper") not in keys
    assert ("pysym", f"{SCRIPTS}/detection_rules.py", "_STRUCTURING_THRESHOLDS") in keys
    assert ("pysym", "src/lakebench/aml/shards.py", "check_plan") in keys  # paired fallback
    assert ("prelude", common, None) in keys


def test_closure_follows_bare_fallback_import_and_refuses_an_unpaired_one(frozen_repo):
    assert frozen_repo.tree_problems() == []
    frozen_repo.edit(
        f"{SCRIPTS}/frozen_a.py",
        "    return aml_opening_balance(1)",
        "    from nowhere_local import thing\n    return aml_opening_balance(1)",
    )
    assert any("cannot resolve import nowhere_local" in p for p in frozen_repo.tree_problems())


def test_unpinned_closure_symbol_fails(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/frozen_a.py",
        "from common import aml_opening_balance, env",
        "from common import aml_opening_balance, env, unrelated_helper",
    )
    problems = frozen_repo.tree_problems()
    assert any("unrelated_helper" in p and "not in" in p for p in problems), problems


def test_star_import_and_escaping_module_object_refused(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/frozen_a.py",
        '"""A frozen script."""',
        '"""A frozen script."""\nfrom common import *',
    )
    assert any("import *" in p for p in frozen_repo.tree_problems())
    frozen_repo.edit(
        f"{SCRIPTS}/frozen_a.py", "from common import *", "import common\nX = [common]"
    )
    assert any("used other than by attribute access" in p for p in frozen_repo.tree_problems())


def test_module_attribute_access_is_pinned(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/frozen_a.py",
        '"""A frozen script."""',
        '"""A frozen script."""\nimport common as c\nY = c.unrelated_helper()',
    )
    problems = frozen_repo.tree_problems()
    assert any("unrelated_helper" in p and "not in" in p for p in problems), problems


# --- check-tree --------------------------------------------------------------


def test_edit_without_list_update_fails_check_tree(frozen_repo):
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    assert any("frozen_a.py" in p and "changed" in p for p in frozen_repo.tree_problems())


def test_entry_dropped_from_list_fails(frozen_repo):
    doc = frozen_repo.list_doc()
    doc["entries"] = [e for e in doc["entries"] if e.get("path") != f"{SCRIPTS}/frozen_a.py"]
    frozen_repo.save_list(doc)
    assert any("frozen_a.py" in p and "not in" in p for p in frozen_repo.tree_problems())


def test_stale_entry_fails(frozen_repo):
    doc = frozen_repo.list_doc()
    doc["entries"].append(
        {"kind": "pysym", "path": f"{SCRIPTS}/common.py", "name": "unrelated_helper", "sha256": "x"}
    )
    frozen_repo.save_list(doc)
    assert any("stale entry" in p for p in frozen_repo.tree_problems())


def test_new_file_under_datagen_src_fails(frozen_repo):
    frozen_repo.write("datagen_rs/src/c.rs", "fn c() {}\n")
    problems = frozen_repo.tree_problems()
    assert any("datagen_rs/src/c.rs" in p for p in problems), problems
    assert any("closed-glob" in p for p in problems), problems


def test_comment_or_gold_ddl_edit_needs_nothing(frozen_repo):
    frozen_repo.edit(DDL, "(b INT)", "(b INT, c INT)")
    frozen_repo.edit(DDL, "def render_ddl", "# a comment\ndef render_ddl")
    assert frozen_repo.tree_problems() == []


def test_silver_ddl_edit_needs_trailer(frozen_repo):
    frozen_repo.edit(DDL, "(a INT)", "(a BIGINT)")
    assert any("SILVER_X_DDL" in p for p in frozen_repo.tree_problems())
    frozen_repo.regen()
    frozen_repo.commit("silver ddl")
    assert any("Freeze-cost" in p for p in frozen_repo.range_problems())


def test_render_ddl_edit_needs_trailer(frozen_repo):
    frozen_repo.edit(
        DDL, "t.format(catalog=catalog, table=table)", "t.format(table=table, catalog=catalog)"
    )
    frozen_repo.regen()
    frozen_repo.commit("render")
    assert any("Freeze-cost" in p for p in frozen_repo.range_problems())


def test_image_input_edit_needs_trailer(frozen_repo):
    frozen_repo.write("datagen_rs/entrypoint.py", "print('datagen 2')\n")
    frozen_repo.regen()
    frozen_repo.commit("entrypoint")
    assert any("Freeze-cost" in p for p in frozen_repo.range_problems())


def test_dynamic_import_in_frozen_file_fails_and_dynamic_ok_passes(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/frozen_a.py",
        "def main():\n",
        "def main():\n    import importlib\n    importlib.import_module('x')\n",
    )
    problems = frozen_repo.tree_problems()
    assert any("dynamic import import_module in main" in p for p in problems), problems
    doc = frozen_repo.list_doc()
    doc["dynamic_ok"].append(
        {
            "path": f"{SCRIPTS}/frozen_a.py",
            "what": "main:import_module",
            "occurrence": 1,
            "reason": "test",
        }
    )
    frozen_repo.save_list(doc)
    frozen_repo.regen()
    assert not any("dynamic import" in p for p in frozen_repo.tree_problems())
    # A second identical call is not covered by the first's entry.
    frozen_repo.edit(
        f"{SCRIPTS}/frozen_a.py",
        "    importlib.import_module('x')\n",
        "    importlib.import_module('x')\n    importlib.import_module('x')\n",
    )
    assert any("dynamic import" in p for p in frozen_repo.tree_problems())


def test_function_mutating_pinned_name_refused_and_dynamic_ok_passes(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/detection_rules.py",
        "def reader():",
        'def tune():\n    _STRUCTURING_THRESHOLDS.update({"EUR": 1})\n\n\ndef reader():',
    )
    assert _mutation_fixture_problems(frozen_repo)
    doc = frozen_repo.list_doc()
    doc["dynamic_ok"].append(
        {
            "path": f"{SCRIPTS}/detection_rules.py",
            "what": "tune:_STRUCTURING_THRESHOLDS",
            "occurrence": 1,
            "reason": "test",
        }
    )
    frozen_repo.save_list(doc)
    assert not _mutation_fixture_problems(frozen_repo)


def test_wrong_python_refuses(frozen_repo, guard, monkeypatch):
    monkeypatch.setattr(guard, "AST_PYTHON", "3.12")
    with pytest.raises(guard.GuardError, match="defined on Python 3.12"):
        guard.require_python()
    assert guard.main(["--root", str(frozen_repo.root), "check-tree"]) == 1


def test_rebinding_a_pinned_name_fails(frozen_repo):
    frozen_repo.write(
        f"{SCRIPTS}/common.py",
        (frozen_repo.root / f"{SCRIPTS}/common.py").read_text()
        + "\n\ndef aml_opening_balance(x):\n    return 0\n",
    )
    assert any("bound 2 times" in p for p in frozen_repo.tree_problems())


def test_manifest_is_recomputed_from_the_tree(frozen_repo, guard):
    sha = guard.manifest_sha256(guard.WorkTree(frozen_repo.root))
    assert len(sha) == 64
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    with pytest.raises(guard.GuardError, match="does not match the list"):
        guard.manifest_sha256(guard.WorkTree(frozen_repo.root))


# --- check-range -------------------------------------------------------------


def test_frozen_edit_without_trailer_fails(frozen_repo):
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    frozen_repo.regen()
    frozen_repo.commit("edit")
    assert any("carries 0" in p for p in frozen_repo.range_problems())


def test_frozen_edit_with_parity_trailer_and_green_parity_passes(frozen_repo):
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    frozen_repo.regen()
    frozen_repo.commit("edit", "Parity")
    assert frozen_repo.range_problems() == []


def test_parity_claim_with_red_parity_fails(frozen_repo):
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    frozen_repo.regen()
    frozen_repo.commit("edit", "Parity")
    assert any("parity result is 'failure'" in p for p in frozen_repo.range_problems("failure"))
    frozen_repo.commit("later, not frozen")
    red = frozen_repo.range_problems(evidence=lambda c: (False, "AML parity (pyspark 4.1.1)"))
    assert any("without green AML parity" in p for p in red), red


def test_no_token_skips_and_require_all_fails(frozen_repo, guard):
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    frozen_repo.regen()
    frozen_repo.commit("edit", "Parity")
    frozen_repo.commit("later")
    problems, notes = guard.range_problems(frozen_repo.root, "HEAD", "success", None, None)
    assert problems == [] and any("SKIP" in n for n in notes)
    problems, _ = guard.range_problems(
        frozen_repo.root, "HEAD", "success", None, None, require_all=True
    )
    assert any("SKIP" in p for p in problems)


def test_void_and_two_trailers_fail(frozen_repo):
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    frozen_repo.regen()
    frozen_repo.commit("edit", "Void")
    assert any("Void needs an owner decision" in p for p in frozen_repo.range_problems())
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh2"', '"lh3"')
    frozen_repo.regen()
    _git(frozen_repo.root, "add", "-A")
    _git(frozen_repo.root, "commit", "-q", "-m", "x\n\nFreeze-cost: Parity\nFreeze-cost: None")
    assert any("carries 2" in p for p in frozen_repo.range_problems())


def test_common_helper_edit_needs_trailer_docstring_edit_does_not(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/common.py", '"""Opening balance."""', '"""Opening balance (v2)."""'
    )
    assert frozen_repo.tree_problems() == []
    frozen_repo.commit("docstring")
    assert frozen_repo.range_problems() == []
    frozen_repo.edit(f"{SCRIPTS}/common.py", "% 200", "% 300")
    assert any("aml_opening_balance" in p for p in frozen_repo.tree_problems())
    frozen_repo.regen()
    frozen_repo.commit("body")
    assert any("carries 0" in p for p in frozen_repo.range_problems())


def test_new_unimported_helper_needs_nothing(frozen_repo, guard):
    frozen_repo.edit(
        f"{SCRIPTS}/common.py",
        "def unrelated_helper",
        "def new_helper():\n    return 2\n\n\ndef unrelated_helper",
    )
    assert frozen_repo.tree_problems() == []
    frozen_repo.commit("helper")
    assert frozen_repo.range_problems() == []
    assert frozen_repo.history_problems() == []


def test_reader_function_edit_needs_nothing(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/detection_rules.py",
        'return _STRUCTURING_THRESHOLDS["USD"]',
        'return _STRUCTURING_THRESHOLDS["USD"] * 2',
    )
    assert frozen_repo.tree_problems() == []


def test_module_level_mutation_of_pinned_dict_needs_trailer(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/detection_rules.py",
        '_STRUCTURING_THRESHOLDS = {"USD": 10000}',
        '_STRUCTURING_THRESHOLDS = {"USD": 10000}\n_STRUCTURING_THRESHOLDS["EUR"] = 1',
    )
    assert any("_STRUCTURING_THRESHOLDS" in p for p in frozen_repo.tree_problems())


def test_module_level_read_of_pinned_name_needs_trailer(frozen_repo):
    frozen_repo.edit(f"{SCRIPTS}/common.py", "_OFFSET = 10", "_OFFSET = 10\nX = _OFFSET + 1")
    assert any("_OFFSET" in p for p in frozen_repo.tree_problems())


def test_import_time_side_effect_without_the_pinned_name_needs_trailer(frozen_repo):
    """A new function that mutates pinned state through an alias, called at
    module level: it names no pinned symbol, and the prelude pin catches it."""
    frozen_repo.edit(
        f"{SCRIPTS}/common.py",
        "def unrelated_helper",
        "def _poke():\n    r = _RUNS\n    r.add('x')\n\n\n_poke()\n\n\ndef unrelated_helper",
    )
    problems = frozen_repo.tree_problems()
    assert any("prelude" in p for p in problems), problems


def test_evil_merge_editing_a_pin_is_walked(frozen_repo):
    base = _git(frozen_repo.root, "rev-parse", "HEAD").strip()
    _git(frozen_repo.root, "checkout", "-q", "-b", "side")
    frozen_repo.write("side.txt", "x")
    frozen_repo.commit("side")
    _git(frozen_repo.root, "checkout", "-q", "-")
    frozen_repo.write("main.txt", "y")
    frozen_repo.commit("main")
    _git(frozen_repo.root, "merge", "-q", "--no-ff", "--no-commit", "side")
    frozen_repo.edit(f"{SCRIPTS}/common.py", "% 200", "% 300")
    frozen_repo.regen()
    _git(frozen_repo.root, "add", "-A")
    _git(frozen_repo.root, "commit", "-q", "-m", "merge side")
    assert base
    assert any("carries 0" in p for p in frozen_repo.range_problems())


# --- check-history -----------------------------------------------------------


def _lock(repo, guard):
    doc = repo.list_doc()
    preds = (repo.root / guard.PREDICTIONS_PATH).read_bytes()
    manifest = guard.manifest_sha256(guard.WorkTree(repo.root))
    pdoc = json.loads(preds)
    pdoc["frozen_manifest_sha256"] = manifest
    (repo.root / guard.PREDICTIONS_PATH).write_text(json.dumps(pdoc))
    import hashlib

    doc["state"] = "locked"
    doc["locked"] = {
        "predictions_sha256": hashlib.sha256(
            (repo.root / guard.PREDICTIONS_PATH).read_bytes()
        ).hexdigest(),
        "manifest_sha256": manifest,
        "date": "2026-10-15",
    }
    repo.save_list(doc)
    repo.commit("lock", "None")


def test_lock_checks_and_edit_after_locked_fails(frozen_repo, guard):
    _lock(frozen_repo, guard)
    assert frozen_repo.tree_problems() == []
    assert frozen_repo.history_problems() == []
    # An edit plus a list update after the lock.
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    doc = frozen_repo.list_doc()
    doc["state"] = "open"
    frozen_repo.save_list(doc)
    frozen_repo.regen()
    doc = frozen_repo.list_doc()
    doc["state"] = "locked"
    frozen_repo.save_list(doc)
    frozen_repo.commit("edit after lock", "Parity")
    problems = frozen_repo.history_problems()
    assert any(
        "while it is locked" in p or "differs from the locked list" in p for p in problems
    ), problems


def test_frozen_edit_without_list_change_after_lock_is_seen(frozen_repo, guard):
    _lock(frozen_repo, guard)
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    frozen_repo.commit("sneaky")
    assert any("differs from the locked list" in p for p in frozen_repo.history_problems())


def test_locked_to_spent_only_changes_spent_seeds(frozen_repo, guard):
    _lock(frozen_repo, guard)
    looks = {
        "looks": [
            {"role": r, "seed": i, "state": "complete", "report_sha256": "ab" * 32}
            for i, r in enumerate(guard.PROTECTED_ROLES)
        ]
    }
    (frozen_repo.root / guard.LOOKS_PATH).write_text(json.dumps(looks))
    prereg = {"corpora": {"spent_seeds": [1, 2], "registered_looks_open": False}}
    (frozen_repo.root / guard.PREREG_PATH).write_text(json.dumps(prereg))
    doc = frozen_repo.list_doc()
    tree_entries = guard.compute(guard.WorkTree(frozen_repo.root), doc).entries
    doc["entries"] = sorted(tree_entries.values(), key=lambda e: json.dumps(guard.entry_key(e)))
    doc["state"] = "spent"
    frozen_repo.save_list(doc)
    frozen_repo.commit("spent", "None")
    problems = frozen_repo.history_problems()
    assert any("beyond corpora.spent_seeds" in p for p in problems), problems


def test_deleting_the_list_fails(frozen_repo):
    (frozen_repo.root / frozen_repo.g.LIST_PATH).unlink()
    frozen_repo.commit("drop list")
    assert any("deletes" in p for p in frozen_repo.history_problems())


def test_append_only_change_fails_closed_without_history_function(frozen_repo, guard):
    path = guard.HELDOUT_PATH
    frozen_repo.write(path, json.dumps({"roles": {"evaluation": ["h1"]}, "spent": []}))
    frozen_repo.regen()
    frozen_repo.commit("heldout created", "None")
    frozen_repo.write(path, json.dumps({"roles": {"evaluation": ["h1", "h2"]}, "spent": []}))
    frozen_repo.regen()
    frozen_repo.commit("heldout append", "None (append)")
    problems = frozen_repo.history_problems()
    assert any("no datagen_seed.heldout_history_problems" in p for p in problems), problems
    assert frozen_repo.range_problems() == []  # the append trailer itself is accepted


def test_append_trailer_refused_on_non_append_change(frozen_repo):
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    frozen_repo.regen()
    frozen_repo.commit("edit", "None (append)")
    assert any("beyond an append-only entry" in p for p in frozen_repo.range_problems())


# --- the real repository -----------------------------------------------------


def test_real_tree_matches_its_list():
    g = _load_guard("frozen_guard_real")
    tree = g.WorkTree(REPO)
    problems, state = g.tree_problems(tree, g.load_list(tree))
    assert problems == [], problems
    assert state is not None
    counts: dict[str, int] = {}
    for path, _ in state.closure.symbols:
        counts[path] = counts.get(path, 0) + 1
    # The closure the d3 recheck counted (plus materialised_source, which the
    # frozen silver stream imports since V16-2).
    assert counts[f"{SCRIPTS}/common.py"] >= 51
    assert counts["src/lakebench/config/datagen_seed.py"] >= 17
    assert counts[f"{SCRIPTS}/tm_operations.py"] >= 5
    assert counts[f"{SCRIPTS}/detection_rules.py"] >= 1
    assert counts["src/lakebench/aml/d8_shards.py"] == 4


# --- pre-build and Full review cases ----------------------------------------


@pytest.mark.parametrize(
    "line",
    [
        '_ = globals()["_RUNS"].add("x")',
        'import sys\n_ = setattr(sys.modules[__name__], "aml_opening_balance", lambda x: 0)',
        "_ = list(map(unrelated_helper, [0]))",
        "from datetime import datetime as os",
        "exec('_RUNS.add(1)')",
    ],
    ids=["globals", "setattr", "map", "external-rebind", "exec"],
)
def test_import_time_code_trips_the_prelude(frozen_repo, line):
    frozen_repo.write(
        f"{SCRIPTS}/common.py",
        (frozen_repo.root / f"{SCRIPTS}/common.py").read_text() + "\n" + line + "\n",
    )
    problems = frozen_repo.tree_problems()
    assert any("prelude" in p or "bound 2 times" in p for p in problems), problems


def test_bare_name_decorator_trips_the_prelude(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/common.py",
        "def unrelated_helper",
        "def _p(f):\n    _RUNS.add(1)\n    return f\n\n\n@_p\ndef unrelated_helper",
    )
    assert any("prelude" in p for p in frozen_repo.tree_problems())


def test_pure_constant_and_table_do_not_trip_the_prelude(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/common.py",
        "_OFFSET = 10",
        "_OFFSET = 10\nNEW_TABLE = {'a': unrelated_helper, 'b': (1, 2)}\nNEW_SET = frozenset({'x'})",
    )
    assert frozen_repo.tree_problems() == []


def test_star_import_in_a_closure_module_refused(frozen_repo):
    frozen_repo.edit(f"{SCRIPTS}/common.py", "import os", "import os\nfrom os.path import *")
    assert any("import *" in p for p in frozen_repo.tree_problems())


def test_package_init_on_the_import_path_is_pinned(frozen_repo):
    frozen_repo.write("src/lakebench/aml/__init__.py", "import os\nos.environ['X'] = '1'\n")
    problems = frozen_repo.tree_problems()
    assert any("src/lakebench/aml/__init__.py" in p for p in problems), problems


def test_floor_symbol_is_expanded(frozen_repo):
    """SCRIPT_MAPS is pinned with what it calls: repointing a shipped file
    through the helper changes a pin."""
    frozen_repo.edit(
        MAPS,
        'return tuple(_src(f"spark/scripts/{n}") for n in names)',
        'return tuple(_src(f"spark/scripts2/{n}") for n in names)',
    )
    problems = frozen_repo.tree_problems()
    assert any("_scripts" in p for p in problems), problems


def test_workflow_level_settings_are_part_of_the_pinned_job(frozen_repo):
    ci = frozen_repo.root / ".github/workflows/ci.yml"
    ci.write_text("defaults:\n  run:\n    shell: 'true {0}'\n" + ci.read_text())
    assert any("ci-job" in p for p in frozen_repo.tree_problems())


def test_function_global_mutation_refused(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/common.py",
        "def unrelated_helper():\n    return 1",
        "def unrelated_helper():\n    global _OFFSET\n    _OFFSET = 11\n    return 1",
    )
    assert _mutation_fixture_problems(frozen_repo)


def test_pre_guard_lane_merged_later_is_judged_at_the_merge(frozen_repo):
    base = _git(frozen_repo.root, "rev-list", "--max-parents=0", "HEAD").strip()
    _git(frozen_repo.root, "checkout", "-q", "-b", "old-lane", base)
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh9"')
    frozen_repo.commit("pre-guard frozen edit, no trailer")
    _git(frozen_repo.root, "checkout", "-q", "-")
    _git(frozen_repo.root, "merge", "-q", "--no-ff", "--no-commit", "old-lane")
    frozen_repo.regen()
    _git(frozen_repo.root, "add", "-A")
    _git(frozen_repo.root, "commit", "-q", "-m", "merge old lane")
    assert any("carries 0" in p for p in frozen_repo.range_problems())
    _git(frozen_repo.root, "commit", "-q", "--amend", "-m", "merge old lane\n\nFreeze-cost: Parity")
    assert frozen_repo.range_problems() == []


def test_clean_merge_of_two_frozen_lanes_needs_no_merge_trailer(frozen_repo):
    _git(frozen_repo.root, "checkout", "-q", "-b", "l1")
    frozen_repo.edit(f"{SCRIPTS}/common.py", "% 200", "% 300")
    frozen_repo.regen()
    frozen_repo.commit("l1", "Parity")
    _git(frozen_repo.root, "checkout", "-q", "-")
    _git(frozen_repo.root, "checkout", "-q", "-b", "l2")
    frozen_repo.write("datagen_rs/src/a.rs", "fn a2() {}\n")
    frozen_repo.regen()
    frozen_repo.commit("l2", "Rebuild")
    _git(frozen_repo.root, "checkout", "-q", "l1")
    _git(frozen_repo.root, "merge", "-q", "--no-ff", "-m", "merge l2", "l2")
    assert frozen_repo.tree_problems() == []
    problems = frozen_repo.range_problems(evidence=lambda c: (True, ""))
    assert problems == [], problems


def test_same_frozen_content_as_head_is_covered_by_this_run(frozen_repo):
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    frozen_repo.regen()
    edit = frozen_repo.commit("edit", "Parity")
    frozen_repo.write("README.md", "x")
    frozen_repo.commit("docs")
    # The edit commit has no runs of its own, but head has its frozen content.
    no_runs = lambda c: (c != edit, "no runs")  # noqa: E731
    assert frozen_repo.range_problems(evidence=no_runs) == []
    # A later frozen edit makes the old commit need its own runs again.
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh2"', '"lh3"')
    frozen_repo.regen()
    frozen_repo.commit("edit 2", "Parity")
    assert any(edit[:12] in p for p in frozen_repo.range_problems(evidence=no_runs))


def test_rebuild_also_needs_parity(frozen_repo):
    frozen_repo.write("datagen_rs/src/a.rs", "fn a2() {}\n")
    frozen_repo.regen()
    frozen_repo.commit("datagen", "Rebuild")
    assert any("needs green AML parity" in p for p in frozen_repo.range_problems("failure"))


def test_append_trailer_refused_on_a_dynamic_ok_change(frozen_repo):
    doc = frozen_repo.list_doc()
    doc["dynamic_ok"].append({"path": "x.py", "what": "f:g", "occurrence": 1, "reason": "r"})
    frozen_repo.save_list(doc)
    frozen_repo.commit("allow", "None (append)")
    assert any("beyond an append-only entry" in p for p in frozen_repo.range_problems())


def test_predictions_rewritten_under_lock_fail(frozen_repo, guard):
    _lock(frozen_repo, guard)
    import hashlib

    preds = frozen_repo.root / guard.PREDICTIONS_PATH
    pdoc = json.loads(preds.read_text())
    pdoc["recall_min"] = 0.1
    preds.write_text(json.dumps(pdoc))
    doc = frozen_repo.list_doc()
    doc["locked"]["predictions_sha256"] = hashlib.sha256(preds.read_bytes()).hexdigest()
    frozen_repo.save_list(doc)
    frozen_repo.commit("rewrite predictions", "None")
    assert any("while it is locked" in p for p in frozen_repo.history_problems())


def test_spent_needs_both_recorded_looks(frozen_repo, guard):
    _lock(frozen_repo, guard)
    doc = frozen_repo.list_doc()
    doc["state"] = "spent"
    frozen_repo.save_list(doc)
    frozen_repo.commit("spent too early", "None")
    problems = frozen_repo.history_problems()
    assert any("has no recorded report sha256" in p for p in problems), problems


def test_deleting_the_heldout_file_fails(frozen_repo, guard):
    path = guard.HELDOUT_PATH
    frozen_repo.write(path, json.dumps({"roles": {}, "spent": []}))
    frozen_repo.regen()
    frozen_repo.commit("heldout", "None")
    (frozen_repo.root / path).unlink()
    frozen_repo.regen()
    frozen_repo.commit("drop heldout", "Parity")
    assert any(f"deletes {path}" in p for p in frozen_repo.history_problems())


# --- fix-pass cases ------------------------------------------------------------


def test_change_committed_apart_from_its_regen_is_walked(frozen_repo):
    """A builtin shadowed in a closure module, committed without regen, then
    the regen alone: the new pin shows at the first commit."""
    frozen_repo.edit(
        f"{SCRIPTS}/common.py",
        "_OFFSET = 10",
        "_OFFSET = 10\n\n\ndef _lb_abs(x):\n    return 0\n\n\nabs = _lb_abs",
    )
    first = frozen_repo.commit("shadow abs")
    frozen_repo.regen()
    frozen_repo.commit("regen only")
    problems = frozen_repo.range_problems()
    assert any(first[:12] in p and "carries 0" in p for p in problems), problems


def test_other_module_mutating_a_pinned_name_at_import_refused(frozen_repo):
    frozen_repo.write(
        "src/lakebench/aml/tamper.py",
        "from detection_rules import _STRUCTURING_THRESHOLDS\n_STRUCTURING_THRESHOLDS.clear()\n",
    )
    problems = frozen_repo.tree_problems()
    assert any("<module> mutates pinned" in p for p in problems), problems


def test_callee_of_import_time_code_is_pinned(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/common.py",
        "_OFFSET = 10",
        "_OFFSET = 10\n\n\ndef _side():\n    return 1\n\n\n_SIDE = _side()",
    )
    frozen_repo.regen()
    frozen_repo.commit("side", "Parity")
    assert frozen_repo.range_problems() == []
    frozen_repo.edit(
        f"{SCRIPTS}/common.py", "def _side():\n    return 1", "def _side():\n    return 2"
    )
    assert any("_side" in p for p in frozen_repo.tree_problems())


def test_package_shadowing_a_module_is_seen(frozen_repo):
    frozen_repo.write("src/lakebench/aml/shards/__init__.py", "def check_plan(x):\n    return 0\n")
    problems = frozen_repo.tree_problems()
    assert any("shards/__init__.py" in p for p in problems), problems


def test_extension_module_shadowing_refused(frozen_repo):
    (frozen_repo.root / "src/lakebench/aml/shards.cpython-311-x86_64-linux-gnu.so").write_bytes(
        b"x"
    )
    assert any("extension module shadows" in p for p in frozen_repo.tree_problems())


def test_symlinked_frozen_file_refused(frozen_repo, guard):
    target = frozen_repo.root / "datagen_rs/entrypoint.py"
    real = frozen_repo.root / "datagen_rs/real_entrypoint.py"
    real.write_text(target.read_text())
    target.unlink()
    target.symlink_to("real_entrypoint.py")
    with pytest.raises(guard.GuardError, match="symlink"):
        frozen_repo.tree_problems()


def test_history_after_reopen_is_not_rejudged(frozen_repo, guard, monkeypatch):
    _lock(frozen_repo, guard)
    looks = {
        "looks": [
            {"role": r, "seed": i, "state": "complete", "report_sha256": "ab" * 32}
            for i, r in enumerate(guard.PROTECTED_ROLES)
        ]
    }
    (frozen_repo.root / guard.LOOKS_PATH).write_text(json.dumps(looks))
    frozen_repo.commit("looks", "None")
    doc = frozen_repo.list_doc()
    doc["state"] = "spent"
    frozen_repo.save_list(doc)
    frozen_repo.commit("spent", "None")
    doc = frozen_repo.list_doc()
    doc["state"] = "open"
    frozen_repo.save_list(doc)
    frozen_repo.commit("reopen", "None")
    # A later guard pins one more file: the old lock is not re-judged.
    monkeypatch.setattr(guard, "REQUIRED_FILES", (*guard.REQUIRED_FILES, "README.md"))
    frozen_repo.write("README.md", "x")
    frozen_repo.regen()
    frozen_repo.commit("v1.8 pin", "None")
    problems = frozen_repo.history_problems()
    assert not any("differs from the locked list" in p for p in problems), problems


def test_state_cache_gives_the_same_answer(frozen_repo, guard, tmp_path):
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    frozen_repo.regen()
    frozen_repo.commit("edit")
    cache = tmp_path / "states.json"
    first = guard.range_problems(
        frozen_repo.root,
        "HEAD",
        "success",
        None,
        None,
        evidence=lambda c: (True, ""),
        state_cache=cache,
    )[0]
    assert cache.is_file() and first
    second = guard.range_problems(
        frozen_repo.root,
        "HEAD",
        "success",
        None,
        None,
        evidence=lambda c: (True, ""),
        state_cache=cache,
    )[0]
    assert second == first


# --- final-pass cases ----------------------------------------------------------


def _spend(repo, guard):
    looks = {
        "looks": [
            {"role": r, "seed": i, "state": "complete", "report_sha256": "ab" * 32}
            for i, r in enumerate(guard.PROTECTED_ROLES)
        ]
    }
    (repo.root / guard.LOOKS_PATH).write_text(json.dumps(looks))
    repo.commit("looks", "None")
    doc = repo.list_doc()
    doc["state"] = "spent"
    repo.save_list(doc)
    repo.commit("spent", "None")


def test_edit_under_lock_stays_visible_after_spent(frozen_repo, guard):
    _lock(frozen_repo, guard)
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh"', '"lh2"')
    frozen_repo.commit("edit under lock", "Parity")
    frozen_repo.edit(f"{SCRIPTS}/frozen_a.py", '"lh2"', '"lh"')
    frozen_repo.commit("revert", "Parity")
    _spend(frozen_repo, guard)
    assert any("differs from the locked list" in p for p in frozen_repo.history_problems())


def test_second_lock_after_a_new_pin_passes(frozen_repo, guard, monkeypatch):
    _lock(frozen_repo, guard)
    _spend(frozen_repo, guard)
    doc = frozen_repo.list_doc()
    doc["state"] = "open"
    doc["locked"] = None
    frozen_repo.save_list(doc)
    frozen_repo.commit("reopen", "None")
    monkeypatch.setattr(guard, "REQUIRED_FILES", (*guard.REQUIRED_FILES, "README.md"))
    frozen_repo.write("README.md", "x")
    frozen_repo.regen()
    frozen_repo.commit("v1.8 pin", "None")
    _lock(frozen_repo, guard)
    problems = frozen_repo.history_problems()
    assert not any("differs from the locked list" in p for p in problems), problems


@pytest.mark.parametrize(
    "text",
    [
        "from detection_rules import _STRUCTURING_THRESHOLDS as T\nT |= {'EUR': 1}\n",
        "import detection_rules as dr\ndr._STRUCTURING_THRESHOLDS['EUR'] = 1\n",
        "import detection_rules as dr\ndr._STRUCTURING_THRESHOLDS = {}\n",
        "import operator\nfrom detection_rules import _STRUCTURING_THRESHOLDS as T\n"
        "operator.setitem(T, 'EUR', 1)\n",
        "from detection_rules import _STRUCTURING_THRESHOLDS as T\ndict.update(T, {'EUR': 1})\n",
        "import detection_rules as dr\nsetattr(dr, '_STRUCTURING_THRESHOLDS', {})\n",
        "from .shards import check_plan\ncheck_plan.cache = {}\n",
    ],
    ids=[
        "augassign",
        "alias-subscript",
        "alias-rebind",
        "operator-setitem",
        "dict-update",
        "setattr",
        "relative",
    ],
)
def test_mutations_of_pinned_names_from_other_modules_refused(frozen_repo, text):
    frozen_repo.write("src/lakebench/aml/tamper.py", text)
    problems = frozen_repo.tree_problems()
    assert any("mutates pinned" in p for p in problems), problems


def test_external_import_used_by_a_pinned_statement_is_hashed(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/detection_rules.py",
        '_STRUCTURING_THRESHOLDS = {"USD": 10000}',
        'from math import floor as _f\n\n_STRUCTURING_THRESHOLDS = {"USD": 10000}\n'
        '_STRUCTURING_THRESHOLDS["EUR"] = _f(9000.7)',
    )
    frozen_repo.regen()
    frozen_repo.commit("eur", "Parity")
    frozen_repo.edit(
        f"{SCRIPTS}/detection_rules.py",
        "from math import floor as _f",
        "from math import ceil as _f",
    )
    assert any("prelude" in p for p in frozen_repo.tree_problems())


def test_optional_import_and_main_guard_idioms_do_not_fail(frozen_repo):
    frozen_repo.edit(
        f"{SCRIPTS}/common.py",
        "_OFFSET = 10",
        "_OFFSET = 10\ntry:\n    import yaml\nexcept ImportError:\n    yaml = None\n"
        "for _k in (1, 2):\n    pass\nfor _k in (3,):\n    pass\n",
    )
    frozen_repo.write(
        f"{SCRIPTS}/common.py",
        (frozen_repo.root / f"{SCRIPTS}/common.py").read_text()
        + '\n\nif __name__ == "__main__":\n    unrelated_helper()\n',
    )
    frozen_repo.regen()
    problems = [p for p in frozen_repo.tree_problems() if "bound" in p]
    assert problems == [], problems
