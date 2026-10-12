# Contributing to Lakebench

How to set up a clone, which tests a change needs, and how to open a pull
request. The code map, test tooling and CI are in
[the development guide](docs/development.md).

## Setup

```bash
git clone https://github.com/PureStorage-OpenConnect/lakebench-k8s.git
cd lakebench-k8s
python3.11 -m venv .venv && . .venv/bin/activate
make dev
make check-fast
```

- Python 3.10 to 3.13. Write code that runs on 3.10.
- Java 17 on `PATH`, only for the Spark tests (`pip install "pyspark==4.0.1"`,
  then `make test-spark`).
- Rust 1.98.1, only if you change the data generator in `datagen_rs/`.
- No Kubernetes cluster is needed for anything below the live tier.

`make dev` installs the package in editable mode with the `dev` extra and
the pre-commit hooks. The pre-push hook is installed separately and needs
`gitleaks` on `PATH`:

```bash
cp scripts/hooks/pre-push "$(git rev-parse --git-common-dir)/hooks/pre-push"
chmod 0755 "$(git rev-parse --git-common-dir)/hooks/pre-push"
```

It scans what you push for secrets using the scanner config from
`origin/main`, so keep `origin` pointing at this repository and push to your
fork as a second remote.

## Tests

Run the cheapest tier that can see the failure your change could cause, and
every tier below it.

| Tier | What it checks | Command |
|---|---|---|
| Unit | lint, formatting, types and everything that runs without a cluster or a Spark JVM | `make check-fast` |
| Spark | the PySpark job scripts on a local Spark | `make test-spark` |
| Live | deploy, generate, run and destroy on a real cluster | [Live-cluster changes](#live-cluster-changes) |

- Every change runs `make check-fast`.
- A change to a script under `src/lakebench/spark/scripts/` also runs
  `make test-spark` on `pyspark==4.0.1` and `pyspark==4.1.1`. CI does not
  run this tier, so say in the pull request whether you ran it.
- A change under `datagen_rs/` runs `cargo fmt --check`,
  `cargo clippy --all-targets --locked -- -D warnings` and
  `cargo test --release --locked` there.
- A change to datagen output, pipeline logic or resource sizing also proves
  itself on a cluster at scale 1. Without a cluster, say so in the pull
  request and a maintainer runs it.

A test passes when the output is right, not when the command exits 0: a
run that wrote no rows or skipped a stage is a failure. A bug fix comes with
a test that fails without it.

Unit tests live in `tests/test_<module>.py` and mock Kubernetes, S3 and
Spark with the fixtures in `tests/conftest.py`. Tests that import pyspark go
under `tests/spark/`.

## Pull requests

1. Fork the repository, add the fork as a remote and branch from `main`.
2. Keep one concern per pull request.
3. Add tests: new code comes with unit tests, a fix with a test that fails
   without it.
4. Run the tiers your change needs, push to your fork and open the pull
   request against `main`.
5. In the description, say what the change does and why, and how you tested
   it beyond the unit tier. A performance claim links to a specific run's
   `metrics.json`.
6. Update `docs/`, `README.md` and `CHANGELOG.md` in the same pull request
   when the change alters behaviour, a CLI flag, a config key, a default or
   a number the docs quote. A change that moves a measured result gets a
   Breaking entry in `CHANGELOG.md` that says what moved and by how much.

CI runs on every push and pull request. A maintainer reviews every pull
request before it merges.

## Style

- Commit messages are in the present tense: a one-line summary, then a
  paragraph on why if it is not obvious. Name the test that proves a fix.
  Reference commits by hash, not pull request number.
- No em dash character, no emoji and no AI attribution anywhere in the
  repository or in commit messages. `python scripts/prose_guard.py` checks
  tracked files.
- Do not skip hooks (`--no-verify`); if a hook fails, fix what it found.
- Lines up to 100 characters; ruff's configuration is in `pyproject.toml`.
- `from __future__ import annotations` in new modules, and `X | Y` unions.
- Pydantic v2 for configuration, dataclasses for internal structures.
- Type annotations and docstrings on public functions and classes; no
  `type: ignore` without a comment giving the reason.
- Comments say why, not what. They do not name the task that added the
  code.
- No feature flag or compatibility shim when a straight change works.

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
  entity and account name is fabricated.

## Help, security and conduct

Unsure about an approach? Open an issue before you invest the time, with
what you tried, what happened and your `lakebench version` output.

Report security problems through the repository's private vulnerability
reporting on GitHub, not in a public issue. Everyone taking part follows the
[code of conduct](CODE_OF_CONDUCT.md).

## Licence

Lakebench is licensed under the [Apache License 2.0](LICENSE). By
submitting a contribution you agree that it is licensed under the same
terms. You keep the copyright in your contribution.
