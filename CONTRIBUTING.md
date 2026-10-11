# Contributing to Lakebench

From a fresh clone to a pull request: setup, the tests a change needs,
review and commits. The code map, test tooling, CI and how to add a recipe
or a workload are in [the development guide](docs/development.md).

## Quick path

From a fresh clone to a green local run:

<!-- contributor-path:begin -->
```bash
git clone https://github.com/PureStorage-OpenConnect/lakebench-k8s.git
cd lakebench-k8s
# python3.11 here; python3.10, python3.12 or python3.13 work too
python3.11 -m venv .venv && . .venv/bin/activate
make dev
make check-fast
pip install "pyspark==4.0.1"
make test-spark
```
<!-- contributor-path:end -->

- `make dev` installs the package in editable mode with the `dev` extra
  and the pre-commit hooks.
- `make check-fast` runs ruff, the format check, mypy and the unit tests in
  parallel.
- `make test-spark` runs the PySpark tests on a local Spark. It downloads the
  Iceberg and Delta jars they need and checks them by sha256. It needs
  Java 17 and takes about 70 minutes (measured on a CI runner).
- Then branch, push to your fork and open a pull request against `main`
  ([Pull requests](#pull-requests)).

What Lakebench deploys and measures: [README.md](README.md),
[the recipes](docs/recipes.md) and
[the configuration reference](docs/configuration.md).

## Setup

- **Python 3.10 to 3.13.** CI tests 3.10 and 3.13; the `[aml]` extra's
  numeric pins have no wheels for 3.14. Write code that runs on 3.10.
- **Java 17** on `PATH`, for the Spark tier only.
- **Rust 1.98.1**, the version CI pins, only if you change the data
  generator in `datagen_rs/` (`rustup toolchain install 1.98.1`, then
  `cargo build --release` there).

- No Kubernetes cluster is needed for any test below the live tier.

The pre-push hook refuses a push that reaches a commit from before the
2026-09-30 history rewrite. It scans what you push with gitleaks, using the
scanner config from `origin/main`. So keep `origin` pointing at this
repository (the clone above does), and push to your fork as a second remote.
Install the hook by copying it into your clone:

```bash
cp scripts/hooks/pre-push "$(git rev-parse --git-common-dir)/hooks/pre-push"
chmod 0755 "$(git rev-parse --git-common-dir)/hooks/pre-push"
```

It needs `gitleaks` on `PATH`. Every worktree of a clone shares it. If it
refuses a push, fix the cause; do not push with `--no-verify`. What it
checks: [Hooks](docs/development.md#hooks-and-the-prose-guard).

## Test tiers

Run the cheapest tier that can see the failure your change could cause, and
every tier below it.

| Tier | What it checks | Command |
|---|---|---|
| Static | lint, formatting, types | `make lint`, `ruff format --check src/ tests/ scripts/`, `make typecheck` |
| Unit | everything that runs without a cluster or a Spark JVM | `make check-fast` (static plus unit, in parallel) |
| Spark | the PySpark job scripts on a local Spark, with the Iceberg and Delta jars | `make test-spark` |
| Live, scale 1 | deploy, generate, run and destroy on a real cluster | the commands under [Live-cluster changes](#live-cluster-changes) |
| Larger | sizing, timing and data-dependent results at scale 10 and up | the same commands with a larger `scale` |

- Every change runs the static and unit tiers.
- A change to a script under `src/lakebench/spark/scripts/` also runs the
  Spark tier, on `pyspark==4.0.1` and `pyspark==4.1.1`. CI does not run it.
- A change to datagen output, pipeline logic or resource sizing also proves
  itself live at scale 1.
- Only what unit tests cannot see needs a cluster: backend behaviour,
  concurrency between deployments, sizing, timing and results that depend on
  the data.
- Without a cluster you can load, say so in the pull request. A maintainer
  runs the live tier before it merges.
- A change under `datagen_rs/` runs `cargo fmt --check`,
  `cargo clippy --all-targets --locked -- -D warnings` and
  `cargo test --release --locked` there.

A test passes when the output is right, not when the command exits 0: a
run that wrote no rows, ran no rules or skipped a stage is a failure. A fix
comes with a test that fails without the fix; check it by reverting the fix
once.

- Unit tests live in `tests/test_<module>.py`. They mock Kubernetes, S3 and
  Spark with the fixtures in `tests/conftest.py`, and S3 with
  [moto](https://github.com/getmoto/moto).
- A test that imports a package the `dev` extra does not install skips with
  `pytest.importorskip`.
- Tests that import pyspark go under `tests/spark/`.

## Pull requests

1. Fork the repository on GitHub, add the fork as a remote of your clone
   (`git remote add fork <your fork's URL>`) and create a branch from
   `main` (`git switch -c my-change origin/main`).
2. Keep one concern per pull request. A bug fix does not carry a drive-by
   refactor.
3. Add tests: new code comes with unit tests, and a fix with a test that
   fails without it.
4. Run the tiers your change needs (above), then `git push fork my-change`
   and open the pull request against `main`. To run every check a release
   runs, `python scripts/release_gate.py` lists what failed.
5. In the description, lead with what the change does and why, then how
   you tested it beyond the unit tier. Say whether docs changed.
6. Update `docs/`, `README.md` and `CHANGELOG.md` in the same pull request
   when the change alters behaviour, a CLI flag, a config key, a default or
   a number the docs quote.
7. A change that can break a 1.6 config, command line or script also gets
   an entry in `docs/upgrading/breaking-1.7.yaml`. It also gets its heading
   in `UPGRADING-1.7.md` and its one-line bullet under "Breaking changes".
   `python3.11 scripts/upgrading.py missing` prints a skeleton for each
   one the code shows.

CI runs on every push and pull request. Jobs and time limits:
[What CI runs](docs/development.md#what-ci-runs).

A maintainer reviews every pull request before it merges.

## Review

A review tries to break the change rather than approve it.

- Findings are ordered by blast radius, silent data corruption first.
- Each has a concrete failure scenario and the file and line it happens at.
- "Looks fine" is not a review.
- Reviews challenge hand-waving: a step that says "handle" or "validate"
  without saying how, or a claim of "works" with no command or run behind it.
- Reviews challenge assumptions nobody has checked, and whether the design is
  the right one at all.

A large change gets one review per dimension it can break. A fix made for a
finding gets its own review.

## Performance claims

Performance claims must link to a specific run's `metrics.json`.

## Commits and style

Commit messages are in the present tense: a one-line summary, then a
paragraph on why if it is not obvious. Name the test that proves a fix.
Reference commits by hash rather than pull request number, so the message
survives a repository move. Commit under your own name and email.

Do not add an AI attribution trailer or any other AI credit line to a
commit or a pull request. Do not skip hooks (`--no-verify`) or bypass
signing; if a hook fails, fix what it found.

Prose, in code, comments, docs and commit messages:

- No em dash character (U+2014). Use `--` or restructure the sentence.
- No emoji.
- No AI attribution (above).
- No filler: cut stock phrases that announce a point instead of making it,
  and say the thing.
- Full sentences. Bullets are for genuine lists and tables for genuine
  comparisons.
- User-facing docs open with the answer or the recommendation.

`scripts/prose_guard.py` (the release gate's `prose` check) checks the em
dash, emoji and AI-attribution rules on every tracked file. It names the
file, the line and the fix. The rest is for review.

Code:

- Lines up to 100 characters; ruff's configuration is in `pyproject.toml`.
- `from __future__ import annotations` in new modules, and `X | Y` unions.
- Pydantic v2 for configuration, dataclasses for internal structures.
- Type annotations and docstrings on public functions and classes; no
  `type: ignore` without a comment giving the reason.
- No comment that says what the code does. Write one when the reason is
  not obvious: a hidden constraint, an invariant, a workaround for a
  specific upstream bug. A comment does not name the task or ticket that
  added the code; that belongs in the commit message.
- No helper for a one-off operation, no feature flag or compatibility shim
  when a straight change works, and no half-finished implementation. Error
  handling goes where the boundary needs it: user input and external
  systems.

## Live-cluster changes

A change that touches datagen output, pipeline logic or resource profiles
proves itself on a real cluster before it merges:

```bash
lakebench deploy -y my-config.yaml
lakebench generate -y --timeout 1200 my-config.yaml
lakebench run --timeout 1800 my-config.yaml
lakebench destroy --force my-config.yaml
```

- This creates namespaces and buckets and runs Spark jobs sized in the tens
  of cores. Use a cluster you are allowed to load, and always finish with
  `destroy`.
- Clean up only with `lakebench destroy`, never by deleting the namespace:
  destroy also releases the shared state a deployment holds.
- Do not create the namespace yourself before `deploy`.
- Configs, examples and fixtures carry placeholders such as
  `${LAKEBENCH_S3_ACCESS_KEY}`, never a real key or endpoint.

## What not to do

- Do not push to `main`; open a pull request from a branch.
- Do not force-push a branch other people are reviewing.
- Do not put real customer data anywhere. Every synthetic transaction,
  entity and account name is fabricated. Public sanctions lists may be
  cited; real customer identifiers never.

## Help, security and conduct

Unsure about an approach? Open an issue before you invest the time. Say what
you are trying to do, what you tried, what happened (paste the error), and
give your `lakebench version` output and cluster details.

Security problems go through private reporting instead; see
[SECURITY.md](SECURITY.md). Everyone taking part follows the
[code of conduct](CODE_OF_CONDUCT.md).

## Licence

Lakebench is licensed under the [Apache License 2.0](LICENSE). By
submitting a contribution you agree that it is licensed under the same
terms. You keep the copyright in your contribution.
