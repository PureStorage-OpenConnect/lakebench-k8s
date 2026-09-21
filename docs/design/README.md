# Lakebench design docs

This directory holds the design rationale for architectural choices in lakebench. Reference material for maintainers and external contributors, curated from work-in-progress notes that live locally in the maintainer's `dev-artifacts/` directory.

Each design doc follows one shape:

- **Problem** -- what breaks without this design.
- **The invariant / contract** -- what must hold, stated as a testable proposition where possible.
- **Non-goals** -- what is explicitly out of scope.
- **Verification** -- unit-level proxies + live UAT.

Current design docs:

- [namespace-isolation.md](namespace-isolation.md) -- shared-cluster ownership taxonomy, the four resource categories, and the invariant "destroying deployment A does not affect deployment B running in parallel." Shipped in v1.5.0.

## Not yet in this directory

Working specs that are still evolving live under `docs/` as `lakebench.next-spec*.md` files rather than under `docs/design/`. When a spec's decisions have shipped and stabilised, they get curated into this directory as a design doc; the working spec then becomes historical.

Currently under active spec work:

- `docs/lakebench.next-spec.md` -- FAML (financial crime / AML) workload domain, plus Spark 4.2 upgrade path.
- `docs/lakebench.next-spec-datagen-v2-plan.md`, `-execution.md`, `-memo.md` -- Rust datagen v2 (shipped as of the [Unreleased] CHANGELOG entry).
- `docs/lakebench.next-spec-eng-2c3-memo.md`, `-addendum.md` -- engineering addenda to the FAML spec.

Once FAML v1 ships, the FAML design will be curated here.

## Reading order for new contributors

If you are new to lakebench and evaluating whether to contribute:

1. `docs/architecture.md` for the module map.
2. `docs/getting-started.md` to run a first pipeline.
3. `docs/design/namespace-isolation.md` (this directory) for the ownership rules any deploy/destroy change must respect.
4. `dev-artifacts/uat-scenarios/` in the maintainer's tree (not in the public repo by design) shows the six live parallel-safety tests that gate each release.
