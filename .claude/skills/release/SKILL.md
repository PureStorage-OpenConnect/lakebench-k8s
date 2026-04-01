---
name: release
description: "Run pre-release checklist gates and prepare the release. Use after /review."
disable-model-invocation: true
argument-hint: "[version, e.g. 1.4]"
---

# Release Checklist

Final gates before tagging v$ARGUMENTS. This is a checklist, not automation --
each item must pass before proceeding.

## Pre-Release Gates (from Development Checklists)

Run through every item. Stop on first failure.

- [ ] All UAT scenarios pass (or failures documented as known/upstream).
- [ ] `dev-artifacts/BUGS.md` is updated: new bugs filed, fixed bugs closed.
- [ ] `dev-artifacts/ENGINEERING_HISTORY.md` has entries for all non-trivial changes.
- [ ] `docs/` user-facing docs reflect any new commands, flags, or config changes.
- [ ] Example configs in `examples/` are valid against the current schema.
      Validate: `for f in examples/*.yaml; do lakebench config validate "$f"; done`
- [ ] `ruff check` + `ruff format` pass with zero issues.
- [ ] `/usr/local/bin/pytest tests/ -x -v` -- all tests pass.
- [ ] Lessons learned review exists at `dev-artifacts/v$ARGUMENTS-lessons-learned.md`.
- [ ] Development Checklists in CLAUDE.md updated with new countermeasures.

## Version Bump

1. Update version in `pyproject.toml`.
2. Update version references in CLAUDE.md if needed.
3. Update test count in CLAUDE.md if changed.

## Tag

Ask the user for confirmation before tagging. Then:

```bash
git add -A
git commit -m "release: v$ARGUMENTS"
git tag v$ARGUMENTS
```

Do NOT push unless the user explicitly asks.

## Post-Release (from Development Checklists)

- [ ] File any deferred bugs in `BUGS.md` with version and severity.
- [ ] Archive UAT configs and runner scripts.
- [ ] Update CLAUDE.md version number and test count.
