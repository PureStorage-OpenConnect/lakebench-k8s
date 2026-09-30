# Lakebench design docs

This directory holds the design rationale for architectural choices in lakebench. Reference material for maintainers and external contributors, curated from the maintainers' working notes once a design has shipped.

Each design doc follows one shape:

- **Problem** -- what breaks without this design.
- **The invariant / contract** -- what must hold, stated as a testable proposition where possible.
- **Non-goals** -- what is explicitly out of scope.
- **Verification** -- unit-level proxies + live UAT.

Current design docs:

- [namespace-isolation.md](namespace-isolation.md) -- shared-cluster ownership taxonomy, the four resource categories, and the invariant "destroying deployment A does not affect deployment B running in parallel." Shipped in v1.5.0.

## Maintainer material

`docs/internal/` holds tracked maintainer material that is not user
documentation and is excluded from the source distribution: the design
contradictions register referenced by [DESIGN.md](../DESIGN.md) and the
registered AML evaluation protocol. The original AML and Rust datagen v2
working specs have shipped and were removed; git history keeps them.

## Reading order for new contributors

If you are new to lakebench and evaluating whether to contribute:

1. `docs/architecture.md` for the module map.
2. `docs/getting-started.md` to run a first pipeline.
3. `docs/design/namespace-isolation.md` (this directory) for the ownership rules any deploy/destroy change must respect.
4. The six live parallel-safety scenarios (S-P1 to S-P6, listed in [namespace-isolation.md](namespace-isolation.md#verification)) gate each release; the maintainers run them against a real cluster, so their scripts are not in this repository.
