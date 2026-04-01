---
name: execute
description: "Execute a release spec. Use after the user brings a spec back from Claude Desktop."
disable-model-invocation: true
argument-hint: "[version, e.g. 1.4]"
---

# Execute Release Spec

The user has brought back a release spec from Claude Desktop. Time to execute.

## Pre-flight

1. Read `dev-artifacts/SCOPE-v$ARGUMENTS.md` -- this is the spec. If it doesn't
   exist, ask the user to save it there first.
2. Read `dev-artifacts/SNAPSHOT-v$ARGUMENTS.md` -- this is the codebase state
   the spec was designed against. Verify the snapshot is still current (no
   major changes since it was generated).
3. Read `dev-artifacts/BUGS.md` -- check which open bugs the spec addresses.
4. Read the Development Checklists in `CLAUDE.md`.

## Execution

1. Break the spec into discrete work items. Use TodoWrite to track them.
2. For each work item:
   - Implement the change
   - Run `ruff check` + `ruff format`
   - Run `/usr/local/bin/pytest tests/ -x -v` to verify no regressions
   - Update `dev-artifacts/ENGINEERING_HISTORY.md` with what was done and why
3. After code changes: `pip install -e .` then re-run tests.
4. Follow all "During Module Extraction or Major Refactors" checklist items
   if doing structural changes.
5. Follow all "Before Every CLI Flag or Command Addition" checklist items
   if adding CLI features.

## Completion

When all work items are done, tell the user. Next step is `/uat`.
