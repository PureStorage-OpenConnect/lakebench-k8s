# Releasing lakebench

A release is a `v*` tag on a commit that is on `main`. Pushing the tag runs
`.github/workflows/release.yml`, which does, in order:

1. the full CI workflow (`ci.yml`) on the tagged commit;
2. `verify-tag`: the tested commit (`$GITHUB_SHA`) is reachable from
   `origin/main`, and the tag is exactly `v` plus the normalised package
   version, which must be a final release: no dev or pre-release suffix
   (`scripts/check_version.py`);
3. `gate`: the release-only checks of `scripts/release_gate.py` with
   `--require-all` (examples, version, changelog, prose, package guard, UAT results,
   and the release evidence checks below);
4. the wheel and sdist, and the PyInstaller binaries for linux-amd64,
   macos-amd64 and macos-arm64, each smoke-tested;
5. the GitHub Release with the binaries and their `SHA256SUMS`, which
   `install.sh` checks each download against;
6. the PyPI upload, only after the GitHub Release exists, so a version on
   PyPI always has its binaries.

Steps 5 and 6 run only in `PureStorage-OpenConnect/lakebench-k8s`. On a
fork they are skipped and `release-dry-run` downloads the same artifacts,
lists them, writes the same `SHA256SUMS` and runs `install.sh` against
them instead. A tag on a fork
therefore runs the build and artifact steps without publishing; the
GitHub Release and PyPI upload actions themselves run only on a real tag. `release.yml` has no
`workflow_dispatch` trigger, because a manual run in the upstream
repository would reach the PyPI upload.

## What the workflow cannot enforce

A tag runs the `release.yml` of the commit it points at. A tag pushed on an
older commit, whose `release.yml` predates these gates, publishes with none
of them. That is how 1.5.0 reached PyPI from an unmerged commit. Only
repository settings close this, and they must be in place before the gates
above mean anything:

| Setting | Where | Value |
|---|---|---|
| Required reviewer on the `pypi` environment | Settings, Environments, `pypi` | at least one maintainer; also restrict deployment to tags matching `v*` |
| Tag ruleset for `v*` | Settings, Rules, Rulesets, new tag ruleset | restrict creation, update and deletion to maintainers; block force pushes |
| Branch protection on `main` | Settings, Rules, Rulesets or Branches | require a pull request and the CI status checks; block force pushes |
| PyPI trusted publisher | pypi.org, project lakebench-k8s, Publishing | repository PureStorage-OpenConnect/lakebench-k8s, workflow `release.yml`, environment `pypi` |

The trusted publisher is bound to the workflow file name, so `release.yml`
must not be renamed without updating it on PyPI.

## Before tagging

Run the whole gate locally; it must pass with nothing skipped:

```bash
python scripts/release_gate.py --tag v<version> --require-all
```

It needs `cargo` (Rust 1.98.1) and `gitleaks` on `PATH`, or `GITLEAKS`
pointing at the binary, and a full clone: the `gitleaks-history` check scans
every commit reachable from `HEAD`, and every commit and tag message, beyond
the `.gitleaksignore` baseline (`scripts/gitleaks_history.py`, the scanner CI
runs), and fails on a shallow clone, on git older than 2.36 (no
`--remerge-diff`) and when gitleaks scanned no commit. The `pre-push-hook` check needs the hook from
`scripts/hooks/pre-push` installed in the clone (see `docs/development.md`);
without it the check is skipped, which `--require-all` fails. It checks the
directory git runs hooks from (`core.hooksPath` when set).

The history scan reads `.gitleaks.toml` and `.gitleaksignore` from a
trusted ref, the first of two that has the file. For a push to `main`, a
pull request to `main` and a `v*` tag that is `origin/main`, then
`origin/integrate/v1.5.0`; for every other branch it is
`origin/integrate/v1.5.0` (the ref the pre-push hook reads), then
`origin/main`. So an allowlist or baseline change takes effect for lane and
train branches when it merges to integrate (the train review and the list
pinned in `tests/test_gitleaks_baseline.py` are the control; integrate has
no branch protection, so that rests on only the main lane merging there),
and for `main` and releases only once it is on `main`. The pull request
that merges integrate to `main` is scanned with `main`'s config, so a
`.gitleaks.toml` allowlist that integrate gained since the last merge must
reach `main` first in its own pull request, branched from `main` and
carrying only that change; otherwise the history scan fails on the
integrate pull request for the commits the allowlist covers. The second
pass uses the branch's own config, so a new rule applies at once. Main's required checks should include "Secret scan (history)" and
"AML statistics (slow)".

The version lives only in `src/lakebench/__init__.py`; `pyproject.toml`
reads it through hatch. Set it to the release version (no `.dev` suffix),
add a `## [<version>]` section to `CHANGELOG.md`, and commit both through a
pull request to `main`.

### UAT results

The gate requires `uat/results-<version>.md`, for example
`uat/results-1.6.0.md`, with a heading line that is exactly
`# UAT results <version>` and a markdown table with at least one data row.
It is the record of the live-cluster runs behind the release: one row per
recipe, workload and mode tested, with its result and the run id
(`YYYYMMDD-HHMMSS-xxxxxx`). The gate checks the heading, that a row exists,
that the table cites at least one run id, that no id in the table is
malformed, and that every cited run id resolves to a `metrics.json` whose
`run_id` matches: `lakebench-output/runs/run-<id>/`, `uat/runs/run-<id>/`,
`uat/perf/run-<id>/`, `$LAKEBENCH_PERF_RUNS_DIR/run-<id>/`, or a
`.../metrics.json` path inside the repository named in the table. Ids in
prose outside the table are not checked. `lakebench-output/` is not
committed, so for the check to pass in the release workflow the cited runs'
`metrics.json` must be checked in under `uat/runs/` (or `uat/perf/`). The
maintainer who tags is still responsible for the content.

### Release evidence

The support record (`src/lakebench/config/validated_combinations.yaml`) is
generated from the release-matrix run records once they are checked in
under `uat/runs/`, and never edited by hand:

```bash
PYTHONPATH=$PWD/src python -m lakebench.config.support . --from-records uat/runs \
    --tree "$(cat uat/freeze-<version>)" \
    --expected uat/expected-results-<version>.json --write
PYTHONPATH=$PWD/src python -m lakebench.config.support .   # regenerate the README and docs tables
```

`.` is the repository root: the record is written to its
`src/lakebench/config/validated_combinations.yaml`, and the release
datagen image's lineage evidence is read from it. Both commands refuse
(exit 2) unless the lakebench they import is that root's `src`, because the
tables, the record rules and the release image come from the imported
code; hence `PYTHONPATH`. `--expected` is the expected-results file the
`expected-results` check reads.

It keeps each record that is release evidence (the `records` check's
rules, below) on a release-matrix row run at that row's Spark minor and
table format version (SPEC section 11, `RELEASE_MATRIX_VERSIONS` in
`src/lakebench/metrics/release_record.py`), groups them by workload,
recipe, mode, Spark minor and table format version, and writes one entry
per group with the freeze commit as its `tree`. A record whose directory is
not `run-<its run_id>` is refused, and so is every copy of a run id found
with two different records; byte-equal copies count once. Every refused
record is listed on stderr with its reasons, and so is every matrix row,
scale included, that no kept run covers (the `support-record` check
refuses the tag until each is covered). Without `--write` it prints the
record instead. It exits 0 when it built at least one entry, 1 when no
record is release evidence (nothing is written) and 2 on a usage error, and
touches no other file. Commit the record and the regenerated tables
together; both are on the post-freeze allowlist.

Four checks hold the release to its evidence; `records` and
`support-record` read the cited run records themselves
(`src/lakebench/metrics/release_record.py`). `records`, `freeze` and
`expected-results` are skipped until the freeze is declared in
`uat/freeze-<version>` (one line, the 40-hex sha of the freeze commit), and
`support-record` until a `--tag` is given; `--require-all`, which the
release workflow passes, fails a skipped check. The release workflow runs
all four with the repository's full history.

- `records`: every run cited in the UAT results table is release evidence.
  A record is refused when it has no experiment block; did not pass; did not
  measure rows in every layer (the verdict's `layer_rows` gate, with no
  layer passed on bytes alone); missed an expected stage (bronze, silver, gold
  and the benchmark, unless the recipe has no query engine; AML batch also
  scoring; C360 continuous also the result check) or skipped one; for AML,
  ran a rule set other than the expected one or errored a rule; returned
  results other than `uat/expected-results-<version>.json`'s (batch: each
  query's result fingerprint and the alert set; continuous: the set of query
  sets its rounds ran); was not run by the freeze commit from a clean tree
  whose code did not change during the run; read a held-out corpus; is not
  exp2 from the release datagen image (the digest `ImagesConfig.datagen`
  pins, with that image's lineage entry); or was bound by an evaluation
  sizing profile or any Lakebench limit in `limits.bound_kinds` the row does
  not allow (none is allowed today). A continuous record must also say which
  query set each round executed (a round with no QpH, every query failed,
  is left out, and a record in which no round measured a QpH is refused);
  a C360 continuous record must also have a result check in which no query
  failed and that matches the expected file's fingerprints, which its continuous entry must list; a corpus with
  recorded problems (such as datagen pods on different images) is refused.
- `support-record` (with `--tag`): `validated_combinations.yaml` lists every
  release-matrix row at the row's Spark minor and table format version
  (`RELEASE_MATRIX_VERSIONS`), lists nothing outside the matrix, and was
  validated on the freeze tree; every run it lists is in `uat/runs/`, is
  release evidence and is the workload, recipe, mode, Spark minor and table
  format version of its entry; and every matrix row, scale included, has
  such a run.
- `freeze`: the freeze commit is an ancestor of `HEAD`, the tree is clean,
  and every change after it is in `uat/`, `validated_combinations.yaml`,
  `benchmarks/perf/baselines.yaml` or `docs/benchmarks/examples/`, the
  `CHANGELOG.md` release heading, the `__version__` line of
  `src/lakebench/__init__.py` (the release bump), or a generated block of
  `README.md` or a top-level `docs/*.md` file that equals what its generator
  writes.
- `expected-results`: `uat/expected-results-<version>.json` exists and its
  last commit comes before the freeze commit; a fingerprint set newer than
  the freeze is refused.

The perf gate refuses the same bound runs: a run an evaluation profile or a
Lakebench limit bound is never recorded as a baseline or compared with one.

### Package guard

The `package-guard` check builds the wheel and sdist (`python -m build
--no-isolation`: the sdist, then the wheel from it) and runs
`scripts/package_guard.py` over them and over the script ConfigMaps
rendered from the wheel: no `docs/internal/` or other maintainer-only
member, no access key, private key or gitleaks finding, and, once the
held-out hash file (`heldout_hashes.json` beside the pre-registration)
exists, no integer that hashes to a held-out seed. A binary or link member
fails too, since nothing could check its content: exclude it from the
sdist. While the hash file's `absence_check` is `report`, a held-out hit,
or a hash file that does not load yet, prints as `PENDING-OA5` and the
check is SKIP, so the release gate (`--require-all`) stays red until the
maintainers' commit removes the plaintext and sets it to `enforce`; once
it says `enforce`, both are a FAIL. Without the file the held-out part is
SKIP too. CI's package build runs the same guard on every push, without
`--require-all`, and the release's `build-dist` job runs it with
`--require-all` on the very files it uploads.

### AML silver parity

`scripts/release/silver_parity.py` checks that a batch and a drained
continuous AML deployment of the same corpus (seed 43, the same scale)
build the same silver tables:

```bash
python3.11 scripts/release/silver_parity.py BATCH_CFG CONTINUOUS_CFG \
    --batch-record <batch run dir> --continuous-record <continuous run dir>
```

It refuses (exit 2) unless each record belongs to its config and the two
deployments differ, both are successful AML runs of one corpus (seed,
scale, generator image, time range, dirty-data ratio, role and
perturbation), the batch record is a batch run, and the continuous record
shows a drained corpus (`pipeline_benchmark.corpus_drained`,
`continuous.drain.state` `drained`, no `continuous.gate_problems`) from a
run with one datagen pod (`workload.datagen.parallelism: 1`): continuous
mode numbers statement entries and running balances in arrival order,
which equals the batch order only when bronze arrives in order.

For each silver table it compares, through Trino, the row count and an
order-insensitive `checksum` of one `xxhash64` per row over the row's
business columns joined as text, so a value moved from one row to another
is caught. The business columns are read from the silver DDL in
`src/lakebench/deploy/financial_ddl.py`. Left out: the batch-version
sentinels (`_batch_id`, `_stream_id`, `ingest_ts`) and
`entity_profiles.profile_updated_ts` (batch stamps the data-clock date,
continuous the latest merged transaction time). `counterparty_edges` is
compared as one row per (source, target) with first and last times and
summed amounts and counts, because continuous mode appends an edge row per
micro-batch. The entity-profile accumulators continuous mode merges
incrementally (`passthrough_ratio`, `avg_gap_days`, `stddev_amount_usd`,
`avg_amount_usd`, `_m2`) are compared per entity within 1e-9, as the
Spark-tier parity test does. Every table must hold rows on both sides; the
batch-versions counts are reported, not compared. A difference names the
differing columns. Exit 1 on any difference, 4 when a query fails.

### Performance baselines

The `perf-baselines` check fails when a required pinned perf config
(`benchmarks/perf/`) has no accepted baseline, has no run, or its run
regressed or was refused. It is not in the release workflow's `--only` list
(`release.yml` runs examples, version, changelog, prose, package-guard, uat-results,
records, support-record, freeze and expected-results), so it does not block a tag by itself: run it locally as part
of the whole gate above before tagging. Check in the `metrics.json` of each
required perf run as `uat/perf/run-<id>/metrics.json` so the result can be
reproduced from the repository. See
[perf-regression-gate.md](perf-regression-gate.md).

## After tagging

```bash
git tag -a v<version> -m "lakebench <version>"
git push origin v<version>
```

Approve the `pypi` environment deployment once the GitHub Release is up.
If the PyPI job fails, re-run that job; do not re-tag.
