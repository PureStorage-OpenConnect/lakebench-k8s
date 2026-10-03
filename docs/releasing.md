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

### Release harness

The release matrix runs through `scripts/release/harness.py` from a
worktree detached at the freeze commit (with `scripts/release/ledger.py`,
the row log and the deployments-ledger edits, and
`scripts/release/cluster.py`, the read-only cluster queries and the
admission decision). It is not part of the wheel. It
runs lakebench only as `env PYTHONPATH=<worktree>/src python3.11 -m
lakebench`, so the editable install never shadows the release tree.

```bash
git worktree add --detach /home/repos/lb-release-<version> <freeze sha>
cd /home/repos/lb-release-<version>
python3.11 scripts/release/harness.py plan --matrix scripts/release/matrix-1.7.yaml --freeze <sha>
export LB_S3_ENDPOINT=... LAKEBENCH_S3_ACCESS_KEY=... LAKEBENCH_S3_SECRET_KEY=...
python3.11 scripts/release/harness.py run --matrix scripts/release/matrix-1.7.yaml \
    --freeze <sha> --context <kube context> --out /root/lakebench-release/<version> \
    --deployments-ledger <deployments ledger file> --ledger-lock <its writers' lock file> \
    [--rows M01,M02] [--slots 3] [--rehearsal]
python3.11 scripts/release/harness.py resume --out /root/lakebench-release/<version> \
    --context <kube context> --deployments-ledger <deployments ledger file> \
    --ledger-lock <its writers' lock file>
```

`plan` needs no cluster: it writes each row's config with `lakebench
init`, checks that it resolves to the row's Spark minor and table format
version (`RELEASE_MATRIX_VERSIONS`), and prints the row's peak from the
same sizing code as `lakebench plan`. The matrix file must list only
release-matrix rows (`RELEASE_MATRIX`); Customer 360 rows use seed 42 and
AML rows the pre-registered calibration seed (43); a protected seed is
refused without being printed.

`run` refuses to start (exit 2) when HEAD is on a branch, `--freeze` is
not a commit or HEAD is not that commit (`--rehearsal` waives only the
second), the tree has a tracked change or an untracked or ignored file
under `src/` or `scripts/`, lakebench is imported from outside the tree,
`--context` is not in the kubeconfig, the ledger file or a credential
variable is missing, or `--out` is inside the worktree or under `/tmp`.
`--freeze` is resolved to its full sha before anything is recorded. Row
configs, their `.lakebench/` state, logs and `rows.jsonl` (one line per row
transition) live under `--out`, one directory per row, never in the
worktree.

For each row it waits for admission and writes the row into the
deployments ledger table under the same lock as the admission decision,
deploys with `--require-new`, reads the deployment's incarnation
(`<namespace uid>#<nonce>`) from the config's state file, runs `run
--generate --yes` with the default per-job timeout, writes the report,
scrubs the record into `<out>/uat/runs/` and destroys with `destroy --yes
--expect-incarnation <uid>#<nonce>`. A row passes only when its scrubbed
record has no `release_record.record_problems` finding; the exit code
alone never passes a row. `<out>/results.md` is the UAT results table: a
row that did not pass cites no run id in the table (the release gate reads
every id there), and its runs are listed below it. The "group check"
column reads "not checked": cross-row fingerprint equality is a separate
check. A rehearsal writes `results-rehearsal.md` with its own heading,
judges records against HEAD, and is never evidence. Copy `results.md` and
the scrubbed records into `uat/` in the post-freeze data commit.

A matrix row may list `extra_steps`, run on the row's own deployment after
its run passes and before its destroy. `continuous-after-batch` (on M01, a
Customer 360 batch row only) runs `run --continuous --yes` on the same
deployment: it must reset the batch's state, which it does only after
proving it owns the namespace and buckets, and its record must pass on its
verdict and rows per layer. A row with that step is admitted at the larger
of its batch and continuous peaks. Extra-step records go to
`<out>/extra/runs/` and their results to `results-extra.md`; they do not
change the row's verdict, but a failed, skipped or missing step makes the
harness exit non-zero.

Safety rules. Destroy is never passed `--force` and never re-invoked. Exit
6 is followed by read-only polls for up to 20 minutes. Any other non-zero
exit, or "Destroy NOT completed", marks the row `failed` or
`destroy-refused` and stops admitting rows; `lease.held` is not retried,
because destroy already waits for the lease. A ledger row is closed only
when the namespace and the row's buckets are gone; otherwise the row is
`left` for a person and admission stops. A deploy that fails is never
retried: its namespace is destroyed by incarnation only when it carries the
row's own confirmed nonce. A failed row stops new admissions, but rows
already running finish and destroy. Each lakebench step has a time limit
(deploy 2 h, run 12 h, destroy 2 h). A child past its limit gets SIGINT,
then three SIGTERMs over 25 minutes, and SIGKILL only after that, because
a command holding the cluster lease finishes its shared change and
releases the lease before it stops.

Admission counts lakebench namespaces, ledger rows and the harness's own
rows against the four-deployment limit. It counts every ledger deployment
at its plan peak, and one whose config cannot be read at the largest
default peak for its scale. It keeps load within 80% of schedulable
allocatable, runs at most two AML continuous rows at once, and runs an
`alone` row with nothing else, including another harness's `alone` row in
the ledger. Unreadable nodes admit nothing.

The ledger table is edited under `--ledger-lock`, with a backup under
`<out>/ledger-backups/`, only when the file did not change while it was
read, and the edit is read back. The lock excludes only writers that take
it: while the harness runs, everyone who edits the ledger (the main lane
included) must edit it under the same lock file, or a hand admission can
race the harness past the four-deployment limit. A row is marked
`destroyed` only after its ledger row is closed. The first Ctrl-C stops
admission and sends one SIGINT to running `lakebench run` children; deploys,
destroys and scenario scripts finish, and rows stop before their next step.
A second Ctrl-C sends one SIGINT to every child running at that moment.
`resume` continues every unfinished row and never re-deploys, re-runs or
re-destroys; it refuses while a row's child process, a scenario script, or
any process naming a row's config is still running.

### Parallel-safety scenarios

The six S-P scenarios live in `scripts/release/scenarios/` and run one at a
time through the harness, from the same detached worktree:

```bash
python3.11 scripts/release/harness.py scenario S-P1 --freeze <sha> --context <kube context> \
    --out /root/lakebench-release/<version> --deployments-ledger <deployments ledger file> \
    --ledger-lock <its writers' lock file>
```

| Scenario | Script | What it proves |
|---|---|---|
| S-P1 | `s-p1-destroy-running.sh` | Destroying A while B's pipeline runs leaves B's record passing (success, scale ratio 0.95 to 1.10) and B's generated objects in place |
| S-P2 | `s-p2-concurrent-deploy.sh` | Two deploys two seconds apart both finish, with distinct identities, nonces and SecretClasses |
| S-P3 | `s-p3-destroy-during-deploy.sh` | Destroying B while A generates leaves A's datagen Job, buckets and generated objects alone |
| S-P4 | `s-p4-double-destroy.sh` | Two destroys of A a second apart: exactly one deletes, the other converges with a named outcome |
| S-P5 | `s-p5-legacy-bucket-destroy.sh` | A pre-existing bucket with no owner is refused at deploy and at destroy and left as it was |
| S-P6 | `s-p6-same-bucket-two-configs.sh` | A second deployment naming A's bucket is refused at deploy and the bucket stays A's |

The harness writes the scenario's configs (Customer 360 batch, scale 1,
hive-iceberg-spark-trino, explicit bucket names) and their ledger rows,
admits all of the scenario's deployments together, and runs the script with
`LB_CONFIG_A`, `LB_CONFIG_B`, `LB_UAT_LOG_DIR`, `LB_KUBE_CONTEXT` and
`LB_EXIT_<NAME>` (each exit code of the release tree) exported, and a
`lakebench` shim for the release tree first on `PATH`. Refusals and
failures are checked by exit code and by the paths `LB_EXIT_PATH_FILE`
names; three success-path lines are still matched as text ("Namespace X
deleted", "deletion started by another run", "concurrent destroy").
`kubectl` and `helm` run only with the pinned context. For S-P5 the harness
creates the bucket, named outside A's name prefix, with one object, and
deletes it afterwards only if it is unchanged; a bucket it did not create
is never touched. For S-P6, B names A's bronze bucket. Bucket owners are
read from the bucket tag, or on a backend without tagging (FlashBlade) from
the bucket's `.lakebench/owner.json` marker. A script that exits early
stops its background jobs (three SIGTERMs over 25 minutes before SIGKILL),
and the harness waits for the script's whole process group before it
cleans up.

After the script, whatever its exit code, the harness checks that each
deployment the scenario keeps is present with its incarnation, destroys
every leftover namespace with `--expect-incarnation` (B before A), allowing
only the refusal the scenario expects (a bucket with no owner, or another
deployment's, refused and left), and closes a ledger row only when the
namespace and the buckets that deployment owns are gone. A scenario passes
when the script exits 0 with its `PASS:` line and every harness check
holds. Scenario results go to `<out>/results-extra.md`, never to
`results.md`, so the release gate's records check does not read them.
`resume` cleans up a scenario the harness stopped in, polls a destroy that
was running, and marks the scenario failed; it never re-runs the script.

### Upgrade from 1.6

`harness.py upgrade` checks that a deployment made by Lakebench 1.6 can be
run and destroyed by the release:

```bash
python3.11 scripts/release/harness.py upgrade --freeze <sha> --context <kube context> \
    --out /root/lakebench-release/<version>-upgrade --deployments-ledger <ledger file> \
    --ledger-lock <its writers' lock file> [--v16-venv <venv with lakebench 1.6>] \
    [--bystander-config <config of a deployment running meanwhile>]
```

Without `--v16-venv` it creates `<out>/v16-venv` and installs
`lakebench-k8s==1.6.0` there (`--v16-spec` changes it); either way it
refuses an interpreter whose lakebench is not 1.6 or is imported from
outside its own environment. It uses one Customer 360 batch scale 1
deployment on hive-iceberg-spark-trino:

1. 1.6 `init` (credentials as `${VAR}` references, the context pinned) and
   at once this tree's `init --from OLD -o NEW`, which must keep the name
   and the `<name>-bronze/-silver/-gold` buckets. The namespace must not
   exist yet. The ledger row names NEW and is admitted at the largest
   default peak for scale 1.
2. 1.6 `deploy` and `run --generate`. The namespace's `<uid>#<nonce>` is
   read right after the deploy. The 1.6 run must be `PASSED` by its own
   stored verdict with output rows in bronze verify, silver build and gold
   finalize (a 1.6 record cannot pass this release's record checks).
   Baseline: the bronze datagen objects (key, size, ETag) and the row
   counts of the silver and gold tables from 1.6 `query`, which must be
   above zero.
3. This tree's `deploy NEW --yes`, which adopts the 1.6 namespace and
   records a nonce (`run` refuses a namespace with no dependency server
   until it is deployed by this tree), only while the namespace is still
   the incarnation 1.6 deployed. The silver and gold row counts must be
   unchanged through this deploy.
4. `run NEW --yes` without `--generate`, over the 1.6 bronze, which must be
   unchanged afterwards. Its record is scrubbed into `<out>/extra/runs/`,
   never `uat/runs/`, and passes on its verdict, rows per layer, stages and
   the freeze commit; the release-image and corpus-lineage checks do not
   apply to it, because 1.6 generated the corpus and writes no corpus
   markers.
5. `destroy NEW --yes --expect-incarnation <uid>#<nonce>`. A bystander's
   namespace incarnation and buckets are read before the 1.7 deploy and its
   generated objects just before the destroy, and all are checked after the
   destroy (its silver and gold change while it runs; its own harness judges
   its record). The bystander must outlive the upgrade.

A failed step is never retried. The deployment is destroyed by an
incarnation this row made: this tree's confirmed nonce, else the nonce 1.6
stamped, or a nonce of this tree's deploy that did not finish on the
namespace 1.6 created. Otherwise it is left for a person. Ctrl-C stops the
routine before its next deploy or run. The result goes to
`results-extra.md`. `resume` cleans up a stopped upgrade with this tree's
CLI only and never re-runs it. Its normal path does not destroy a 1.6
deployment through a nameless 1.6 directory with no recorded nonce; that
path has its own tests. The 1.6 run's record is copied to
`<out>/extra/v16-runs/` before the 1.6 count queries append their metrics
to it.

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
