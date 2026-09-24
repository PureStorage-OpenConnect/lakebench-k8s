# Contributing to lakebench

Thanks for looking at the code. This document tells you what "ready to
review" means here, so we spend less time cycling on style and more
time on the substance of the change.

## Rules that catch real defects

These come from bugs we shipped and then fixed retroactively. They
are not opinions.

### Adversarial subagent review before "done"

Every change beyond a one-line typo goes past at least one adversarial
review before it is called complete. Not "please review this" -- the
prompt asks the reviewer to attack the change, find silent-corruption
defects ranked by blast radius, and give a concrete failure scenario
for each finding. Cheerleading reviews miss the class of bugs that
matters.

Every change touching >200 LOC, multiple modules, or the profile /
compat / recipe / DDL / typology tables gets parallel adversarial
subagents by dimension (correctness, race/concurrency, config/schema,
resource sizing, destroy/cleanup ordering, test coverage). Findings
are consolidated with severity ordering, silent-corruption first, and
recorded in the PR description alongside the change.

If a fix is applied for a finding, the fix gets its own adversarial
pass. Fixes have the same blind-spot problem as the original code.

### Distribution checks don't prove semantics

For synthetic-data changes, distribution bands are necessary but not
sufficient. A change must additionally:

- Run a reference detector or scoring function against the output
- Include a leakage check (planted signal recovered by the intended
  rule, absent otherwise)

Three consecutive review cycles of "distribution looks fine" shipped
five P1 defects that only surfaced when a real detection query hit
the data. The rule is: if it changes what the data looks like, prove
the detector still finds what it should find.

### Metrics as the change gate

Any performance claim in a PR description, doc, or blog post links
to a `metrics.json` from a specific run tagged with commit SHA,
cluster spec, and scale factor. Not "we saw 4.34 GB/s"; instead
"commit abc123, 8 pods x 8 CPU, scale 0.5, snappy,
`lakebench-output/runs/run-2026-09-20-c360-scale-0.5/metrics.json`,
4.34 GB/s aggregate."

If you cannot produce the metrics.json, the claim does not go in.

### Change-scope discipline

- Bug fixes do not include drive-by refactors.
- One-shot operations do not need helpers.
- No feature flags or backward-compatibility shims when a straight
  code change works.
- Three similar lines is better than a premature abstraction.
- No half-finished implementations.
- Add error handling only where the boundary demands it (user input,
  external APIs). Trust internal callers.

### Comments

Default is no comments. Add one when the *why* is non-obvious: a
hidden constraint, a subtle invariant, a workaround for a specific
bug, behavior that would surprise a reader. If removing the comment
would not confuse a future reader, do not write it.

Do not explain *what* the code does -- well-named identifiers already
do that. Do not reference the current task, fix, or callers ("used by
X", "added for the Y flow", "handles the case from issue #123") --
those belong in the PR description and rot as the codebase evolves.

## Setup

You need Python 3.10 or newer (CI tests 3.10 to 3.13) and, for the data
generator, Rust 1.98.1 (the version CI pins; `rustup toolchain install
1.98.1`).

```bash
git clone https://github.com/PureStorage-OpenConnect/lakebench-k8s.git
cd lakebench-k8s

# Python side, in a virtual environment
python3.11 -m venv .venv    # any Python 3.10 or newer
. .venv/bin/activate
pip install -e ".[dev]"
pre-commit install          # optional: ruff, gitleaks and cargo fmt on commit

# Rust datagen side
cd datagen_rs && cargo build --release && cd ..
```

## Testing

### Unit tests (mandatory)

```bash
pytest tests/ -x
ruff check src tests scripts
ruff format --check src tests scripts
mypy src/lakebench/

# Rust
cd datagen_rs
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --release -- --test-threads=1
cd ..
```

All must pass before any commit that touches the affected code path.
`--test-threads=1` on Rust because a few tests manipulate env vars in
ways that race under parallel execution.

The PySpark tests under `tests/spark/` skip unless PySpark is installed,
and need a Java 17 runtime on `PATH` when it is. To run them:

```bash
pip install "pyspark==4.0.1" pyarrow
pytest tests/spark -q
```

`python scripts/release_gate.py` runs every check above plus the example
validation, version, changelog and secret-scan checks, and lists what
failed. CI runs the same checks on every push.

### Live-cluster tests (for non-trivial changes)

Changes that touch datagen output, pipeline logic, or resource
profiles need to prove themselves on a real cluster before merging.
The pattern:

```bash
lakebench deploy -y my-config.yaml
lakebench generate -y --wait --timeout 1200 my-config.yaml
lakebench run --timeout 1800 my-config.yaml
lakebench destroy --force my-config.yaml
```

This creates namespaces and buckets and runs Spark jobs sized in the
tens of cores, so use a cluster you are allowed to load, and always
finish with `destroy`.

## PR expectations

A PR is ready when:

- [ ] Tests pass locally, both suites
- [ ] `ruff check`, `ruff format` and `mypy` clean
- [ ] Adversarial review findings recorded in the PR description for
      non-trivial changes
- [ ] Performance claims cite a specific metrics.json run
- [ ] Docs and `CHANGELOG.md` updated where the change is user-visible
- [ ] Linked issue, if the change fixes or opens a bug
- [ ] Description leads with the answer, then the reasoning. Not a
      wall of bullets.

## Commit messages

Present tense, one-line summary, then a paragraph of rationale if
needed. Reference commits (`abc123`) rather than PR numbers so the
message survives repository migrations.

Never include Co-Authored-By: Claude or similar AI attribution.
Commits are authored under a real name (`AndrewSillifant`, not
`root`).

Never skip hooks (`--no-verify`) or bypass signing unless
explicitly asked. If a hook fails, fix the underlying issue.

## Style

- Never use em dashes (U+2014). Use `--` or restructure the sentence.
  Applies to code, comments, docs, commit messages, and PR bodies.
- No emoji in code, commits, or documentation.
- Avoid LLM filler: "comprehensive", "leverage", "Note that", "This
  ensures", "It is worth noting", "delve", and similar. Cut the
  phrase or say the thing directly.
- Prose in full sentences. Bullets are for genuine lists; tables are
  for genuine comparisons.
- User-facing docs open with the answer or recommendation. Do not
  bury it.

## Repository areas

- `src/lakebench/` -- Python CLI, deploy, benchmark, metrics
- `datagen_rs/` -- Rust datagen (financial + customer360 schemas)
- `docs/` -- user-facing documentation
- `tests/` -- Python unit tests (`tests/spark/` runs PySpark locally)
- `datagen_rs/tests/` -- Rust regression tests
- `examples/` -- hardened example configs per recipe
- `scripts/` -- release gate, version and coverage checks

If you are new here, start with `README.md`, `docs/recipes.md` and
`docs/configuration.md`.

## What NOT to do

- Do not commit unless the change was explicitly requested.
- Do not push to `main`. Open a PR from a feature branch.
- Do not force-push a branch other people are reviewing.
- Do not include real customer data anywhere. Every synthetic
  transaction, entity, and account name is fabricated. OFAC SDN is
  public and citing real entities in synthetic transactions is fine.
  Real customer identifiers are never OK.
- Do not add Prometheus scraping, Grafana panels, or observability
  wiring for short-lived batch jobs whose metrics are terminal, not
  longitudinal. One structured JSON line at completion is enough for
  the scorecard; polling adds noise without value.

## Getting help

Open a discussion issue with:

- What you are trying to do
- What you tried
- What happened (paste the error, don't paraphrase)
- Your `lakebench version` output and cluster details

Security problems go through private reporting instead; see
`SECURITY.md`. Everyone taking part is expected to follow
`CODE_OF_CONDUCT.md`.
