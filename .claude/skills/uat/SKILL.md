---
name: uat
description: "Run UAT checklist and generate test configs for live cluster testing."
disable-model-invocation: true
argument-hint: "[version, e.g. 1.4] [--scale N]"
---

# UAT Preparation and Execution

Prepare and run User Acceptance Testing for the release.

## Pre-flight (from Development Checklists)

1. Run `pip install -e .` to ensure latest code is installed.
2. Run `/usr/local/bin/pytest tests/ -x -v` -- all tests must pass.
3. Run `ruff check` + `ruff format` -- zero issues.

## Test Matrix

1. Read `dev-artifacts/SCOPE-v$ARGUMENTS.md` for the UAT test matrix.
   If no matrix is defined, generate one from `_SUPPORTED_COMBINATIONS`:
   every recipe x {batch, sustained} x supported Spark versions.
2. Every new CLI flag and command added in this release must have a
   UAT scenario.

## Config Generation

1. Generate YAML configs for every test in the matrix. Save to `/tmp/`.
   Each test gets:
   - Dedicated namespace: `vNM-tNN` (e.g., `v14-t01`)
   - Dedicated S3 buckets: `vNM-tNN-{bronze,silver,gold}`
   - `create_storage_class: false` (pre-create SC before parallel tests)
2. Generate the runner script at `/tmp/vNM-run-test.sh`.
3. Default scale is 1 unless `--scale N` is specified.

## Execution Plan

1. Pre-create the shared StorageClass (avoid create race -- v1.2 lesson).
2. Run 4 tests concurrently from round 1 (not sequentially -- v1.3 lesson).
3. Per-job timeout: 1200s minimum.
4. Logs at `/tmp/vNM-vNM-tNN.log`.
5. Check results: `grep "RESULT:" /tmp/vNM-*.log`

## Post-UAT

1. Report pass/fail per test.
2. File any new bugs in `dev-artifacts/BUGS.md`.
3. Update `dev-artifacts/ENGINEERING_HISTORY.md` with UAT results.
4. If fixes are needed, apply them and re-run affected tests.

When UAT passes, tell the user. Next step is `/review`.
