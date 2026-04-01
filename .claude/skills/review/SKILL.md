---
name: review
description: "Run lessons learned review for a release. Use after UAT passes."
disable-model-invocation: true
argument-hint: "[version, e.g. 1.4]"
---

# Lessons Learned Review

Post-release review for v$ARGUMENTS. Generates the review document and
updates process checklists.

## Data Gathering

1. Read `dev-artifacts/ENGINEERING_HISTORY.md` -- all entries for this release.
2. Read `dev-artifacts/BUGS.md` -- bugs found and fixed in this version.
3. Run `git log --oneline release/v$ARGUMENTS..HEAD` (or appropriate range)
   to see the full commit history.
4. Read `dev-artifacts/SCOPE-v$ARGUMENTS.md` -- what was planned vs. what
   was actually delivered.

## Review Document

Write `dev-artifacts/v$ARGUMENTS-lessons-learned.md` covering:

1. **Release summary** -- scope, commit count, test count, bug count.
2. **What went well** -- things that worked, validated approaches.
3. **What bit us** -- bugs, surprises, process failures. For each:
   - What happened
   - Root cause
   - Countermeasure (specific, actionable)
4. **Bug pattern analysis** -- cluster bugs by root cause category.
   Identify the dominant clusters.
5. **Open items carried forward** -- bugs and work deferred to next release.
6. **Process recommendations** -- specific changes to make next cycle better.

## Update Process

1. Add any new countermeasures to the Development Checklists in `CLAUDE.md`.
   This is how lessons become enforced process.
2. Update `dev-artifacts/ENGINEERING_HISTORY.md` with the review entry.
3. Update `dev-artifacts/BUGS.md` -- close fixed bugs, file deferred items.

## Completion

Tell the user the review is done and what checklist items were added.
Next step is `/release`.
