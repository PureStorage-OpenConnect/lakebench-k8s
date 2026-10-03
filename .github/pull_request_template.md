## What and why

<!-- Lead with what the change does and why. A reviewer who reads only this
paragraph should know whether they want it. -->

## Evidence

<!-- Tests added or run, with their result lines. Any performance or
detection-quality claim cites the run: commit, cluster, scale, and the path
to its metrics.json. -->

## Checklist

- [ ] `make check-fast` passes, plus `make test-spark` for Spark script changes
- [ ] `ruff check src tests scripts` and `ruff format --check src tests scripts` are clean
- [ ] `mypy src/lakebench/` is clean
- [ ] If `datagen_rs/` changed: `cargo fmt --check`, `cargo clippy --all-targets --locked -- -D warnings`, `cargo test --release --locked`
- [ ] New behaviour has a test
- [ ] User-visible changes are in `docs/` and `CHANGELOG.md`
- [ ] No credentials, no em dashes, no AI attribution in commits
