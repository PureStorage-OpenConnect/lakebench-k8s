# Releasing lakebench

How a lakebench release is cut, for maintainers. Every step that a command
can do is a `make` target; `make release-check VERSION=X.Y.Z` runs them in
order and stops at the first failure. The steps that need a person (merging
to `main`, tagging, approving the PyPI upload, credential state) are the
owner checklist in section 5.

## 1. Roles

| Step | Who | How |
|---|---|---|
| Open a cycle, set the version, keep the CHANGELOG | maintainer | pull requests to the integration branch |
| Freeze: the release worktree at the freeze commit | maintainer | `git worktree add --detach` (section 3) |
| Scripted release steps | maintainer | `make release-check VERSION=X.Y.Z` (section 4) |
| Merge the release to `main`, tag, approve PyPI, credential state | owner | the checklist in section 5 |
| Repository settings that bind the tag workflow | owner | section 6 |

## 2. Start of a cycle

- The version lives only in `src/lakebench/__init__.py` (`__version__`);
  `pyproject.toml` reads it through hatch.
- Open a `## [Unreleased]` section at the top of `CHANGELOG.md`. Changes are
  recorded there as they merge, one line per change; breaking changes go
  under `### Breaking changes`.

## 3. Freeze and the release commit

Run the steps in this order. Each command runs on the commit named.

1. **Before the freeze**, on the commit to be frozen: commit
   `uat/expected-results-<version>.json` (the `expected-results` check
   refuses one whose last commit comes after the freeze), then run
   `make release-check VERSION=X.Y.Z DRY=1`; it must print no `pending:`
   line.
   The Makefile is not on the post-freeze allowlist, so a step still
   pending after the freeze can only be fixed by freezing again, which
   voids every matrix record.
2. **The freeze.** Create the release worktree, detached at the freeze
   commit; the release harness refuses a branch checkout, a dirty tree or
   another commit:

   ```bash
   git worktree add --detach ../lakebench-release-X.Y.Z <freeze sha>
   ```

   The release-matrix runs execute from this worktree, so their records
   name the freeze commit.
3. **The release commit.** In that worktree, once the runs are recorded,
   create a branch from the freeze commit (`git switch -c release/X.Y.Z`)
   and commit only what the post-freeze allowlist permits (the `freeze`
   check in section 4): `uat/freeze-<version>` (one line, the 40-hex sha
   of the freeze commit), `uat/results-<version>.md` and the scrubbed
   records under `uat/runs/`, the regenerated support record and its
   tables, the `__version__` bump to the release version and the
   CHANGELOG's `## [<version>] - <date>` heading. The `records`, `freeze`
   and `expected-results` checks of the release gate are skipped until
   `uat/freeze-<version>` exists, and `--require-all` fails a skipped
   check.
4. **The full check**, on the release commit:
   `make release-check VERSION=X.Y.Z`. It must pass with every step.
5. Push the branch and open the release pull request to `main`; the owner
   takes it from there (section 5).

## 4. Scripted steps

```bash
make release-check VERSION=X.Y.Z          # every step, in order (the release commit)
make release-check VERSION=X.Y.Z DRY=1    # the dry run: wiring only, no cluster
make rc-<id> VERSION=X.Y.Z                # one step alone
```

The targets need GNU make. `release-check` refuses a dirty tree and the
`-i` and `-k` flags, then runs the steps below in order, one
`make rc-<id>` each, and stops at the first that fails. `DRY=1` (any
non-empty value) runs each step's dry variant, runs every step even after one fails, and lists the
failures at the end. The build goes to a fresh temporary directory, removed
afterwards, unless `DIST=<empty dir>` is given. A step whose command does not exist yet prints
`pending:` and fails, so the dry run stays red until it is built; the table
marks those steps. Every Python step runs with `PYTHONPATH=src`, so it
tests the worktree's own code, not an installed copy; `PYTHON=python3.11`
picks the interpreter (default `python3`).

<!-- release-steps:begin -->
| Step | Command | `DRY=1` variant |
|---|---|---|
| `version` | `scripts/check_version.py --tag vX.Y.Z`: the tag is `v` plus the package version, a final release | the version source and its PEP 440 form only |
| `local-refs` | `tests/test_releasing_doc.py::test_no_dev_artifacts_reference`: no tracked file points at the maintainer workspace | same |
| `generated-docs` | `scripts/gen_docs.py --check`: support tables, configuration and CLI references, sizing tables, exit codes, prerequisites | same |
| `doc-readers` | `scripts/check_doc_readers.py`: every tracked doc is linked or read | same |
| `breaking-changes` | `tests/test_breaking_changes.py` and the CHANGELOG format test (pending) | same |
| `filler-words` | `git grep` over tracked files for the filler phrases and AI-voice phrases in the Makefile's `FILLER_WORDS` and `AI_VOICE` | same |
| `matrix` | the release harness's check that the matrix records of section 3 are complete (pending) | the harness's two-row dry run (pending) |
| `uat-results` | the release harness's check of `uat/results-<version>.md` and the scrubbed `uat/runs/` (pending) | its dry run (pending) |
| `support-record` | the support record regenerated from the matrix records, then `git diff --exit-code`: the committed record is the one the records give | a check mode (pending) |
| `build` | `python -m build` into an empty `DIST` directory | same |
| `package-guard` | `scripts/package_guard.py --dist <dir> --require-all` | same |
| `gate` | `scripts/release_gate.py --tag vX.Y.Z --require-all`, with `PERF_RUNS="NAME=RUN ..."` for the perf configs | `--list` |
<!-- release-steps:end -->

The table lists the step ids in the order `RELEASE_STEPS` in the `Makefile`
runs them; `tests/test_releasing_doc.py` fails when the two differ, when a
script a step names is missing, and, from the run-up to a freeze on (an
`uat/expected-results-<version>.json` for a version with no CHANGELOG
release section yet, or a final `__version__` with its
`uat/freeze-<version>`), when any step is still pending.

### The release gate

`scripts/release_gate.py --tag v<version> --require-all` must pass with
nothing skipped. It needs `cargo` (Rust 1.98.1) and `gitleaks` on `PATH`,
or `GITLEAKS` pointing at the binary, and a full clone: the
`gitleaks-history` check scans every commit reachable from `HEAD`, and
every commit and tag message, beyond the `.gitleaksignore` baseline
(`scripts/gitleaks_history.py`, the scanner CI runs), and fails on a
shallow clone, on git older than 2.36 (no `--remerge-diff`) and when
gitleaks scanned no commit. The `pre-push-hook` check needs the hook from
`scripts/hooks/pre-push` installed in the clone (see
`docs/development.md`); without it the check is skipped, which
`--require-all` fails. It checks the directory git runs hooks from
(`core.hooksPath` when set).

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
pass uses the branch's own config, so a new rule applies at once. Main's
required checks should include "Secret scan (history)" and "AML statistics
(slow)".

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
its run passes and before its destroy, and only while the deployment is
still the row's incarnation. `continuous-after-batch` (on M01, a Customer
360 batch row only) runs `run --continuous --force-reset --skip-deploy
--yes` on the same deployment: the reset of the batch's tables still needs
the ownership proof, and the reset job's own lines (read with `lakebench
logs ... bronze-verify`, the job's last attempt) must show at least one
table dropped with PURGE, no table kept as foreign, and every deleted
location inside the row's own buckets, and the objects the run itself
cleared (checkpoints, raw data) must be in the row's buckets. The continuous record must pass on its verdict, rows per layer and
commit. A row with that step is admitted at the larger of its batch and
continuous peaks. Extra-step records go to `<out>/extra/runs/` and their
results to `results-extra.md`, which also lists a step that did not finish
as MISSING; copy both to `uat/extra/` (never `uat/runs/`, which the support
record reads) in the post-freeze data commit. They do not change the row's
verdict, but a failed, skipped or missing step makes the harness exit
non-zero.

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
table format version (`RELEASE_MATRIX_VERSIONS` in
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
`uat/freeze-<version>`, and `support-record` until a `--tag` is given;
`--require-all`, which the release workflow passes, fails a skipped check.
The release workflow runs all four with the repository's full history.

- `records`: every run cited in the UAT results table is release evidence.
  A record is refused when it has no experiment block; did not pass; did not
  measure rows in every layer (the verdict's `layer_rows` gate, with no
  layer passed on bytes alone); missed an expected stage (bronze, silver, gold
  and the benchmark, unless the recipe has no query engine; AML batch also
  scoring; C360 continuous also the result check) or skipped one; for AML,
  ran a rule set other than the expected one or errored a rule; returned
  results other than `uat/expected-results-<version>.json`'s (batch: each
  query's result fingerprint and the alert set; continuous: the set of query
  sets its rounds ran, where a round whose only failed queries are Q9, which
  the verdict tolerates, counts for the set it listed when its executed
  queries are the listed ones less Q9 and both recorded set ids match those
  names); was not run by the
  freeze commit from a clean tree whose code did not change during the run;
  read a held-out corpus; is not
  exp2 from the release datagen image (the digest `ImagesConfig.datagen`
  pins, with that image's lineage entry); or was bound by an evaluation
  sizing profile or any Lakebench limit in `limits.bound_kinds` the row does
  not allow (none is allowed today). A continuous record must also say which
  query set each round executed (a round with no QpH, every query failed,
  is left out, and a record in which no round measured a QpH is refused);
  a C360 continuous record must also have a result check in which no query
  failed and that matches the expected file's fingerprints, which its
  continuous entry must list; a corpus with recorded problems (such as
  datagen pods on different images) is refused.
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
`continuous.drain.state` `drained`) from a run with one datagen pod
(`workload.datagen.parallelism: 1`), and both configs query through Trino.
Continuous mode numbers statement entries and running balances in arrival
order, which equals the batch order only when bronze arrives in order; one
pod still writes files concurrently, so a difference confined to those
columns is labelled as possibly an ordering artefact, which a rerun with
the silver stream's strict-parity switch settles.

For each silver table it compares, through Trino, the row count and an
order-insensitive `checksum` of one `xxhash64` per row over the row's
business columns joined as text, so a value moved from one row to another
is caught. The business columns are read from the silver DDL in
`src/lakebench/deploy/financial_ddl.py`. Left out: the batch-version
sentinels (`_batch_id`, `_stream_id`, `ingest_ts`, `committed_at`) and
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
regressed or was refused. It is in the release workflow's `--only` list
(`release.yml` runs examples, version, changelog, prose, package-guard, uat-results,
records, support-record, freeze, expected-results and perf-baselines), so a
tag with a required config that fails is refused. Check in the
`metrics.json` of each required perf run as `uat/perf/run-<id>/metrics.json`,
where the check finds it.

The v1.7 re-baseline pins three configs: `aml-batch-s10` (AML batch scale
10, hive-iceberg-spark-trino on Spark 4.1) and `c360-batch-s10` with its
Polaris twin `c360-batch-s10-polaris` (Customer 360 batch scale 10; the
two differ only in the catalog, both on Spark 4.0). Each runs once on the
freeze tree as `lakebench run --generate --repeat 3`; repetition 1 is
recorded as its baseline (n=1) through `scripts/perf_gate.py`, all three
records go to `uat/perf/`, and the post-freeze data commit marks exactly
these three `required: true`; until then none is required, and once the
CHANGELOG dates 1.7.0 a test requires exactly these three. The release
check then compares repetition 3 with repetition 1 of one series, which
shows repeatability rather than the absence of a regression. See
[perf-regression-gate.md](docs/perf-regression-gate.md).

## 5. Owner checklist

Only the owner does these. Nothing in `make release-check` merges, tags,
pushes or publishes.

1. Merge the release pull request (section 3, step 5) to `main`. Its
   release commit has set `__version__` to the release version (no `.dev`
   suffix) and the CHANGELOG's `## [<version>] - <date>` heading, and
   `make release-check` passed on it.
2. Tag the merged commit and push the tag:

   ```bash
   git tag -a v<version> -m "lakebench <version>"
   git push origin v<version>
   ```

3. Approve the `pypi` environment deployment once the GitHub Release is up.
   If the PyPI job fails, re-run that job; do not re-tag.
4. Confirm the credential state: the `gitleaks-history` gate check
   reported nothing beyond the `.gitleaksignore` baseline, and every
   credential that was ever exposed in the published history has been
   rotated.
5. When the release holds registered AML looks, announce their window
   before they run (`docs/internal/aml-protocol.md`).

## 6. Repository settings

A tag runs the `release.yml` of the commit it points at. A tag pushed on an
older commit, whose `release.yml` predates these gates, publishes with none
of them. That is how 1.5.0 reached PyPI from an unmerged commit. Only
repository settings close this, and they must be in place before the gates
mean anything:

| Setting | Where | Value |
|---|---|---|
| Required reviewer on the `pypi` environment | Settings, Environments, `pypi` | at least one maintainer; also restrict deployment to tags matching `v*` |
| Tag ruleset for `v*` | Settings, Rules, Rulesets, new tag ruleset | restrict creation, update and deletion to maintainers; block force pushes |
| Branch protection on `main` | Settings, Rules, Rulesets or Branches | require a pull request and the CI status checks; block force pushes |
| PyPI trusted publisher | pypi.org, project lakebench-k8s, Publishing | repository PureStorage-OpenConnect/lakebench-k8s, workflow `release.yml`, environment `pypi` |

The trusted publisher is bound to the workflow file name, so `release.yml`
must not be renamed without updating it on PyPI.

## 7. What the tag workflow does

A release is a `v*` tag on a commit that is on `main`. Pushing the tag runs
`.github/workflows/release.yml`, which does, in order:

1. the full CI workflow (`ci.yml`) on the tagged commit;
2. `verify-tag`: the tested commit (`$GITHUB_SHA`) is reachable from
   `origin/main`, and the tag is exactly `v` plus the normalised package
   version, which must be a final release: no dev or pre-release suffix
   (`scripts/check_version.py`);
3. `gate`: the release-only checks of `scripts/release_gate.py` with
   `--require-all` (examples, version, changelog, prose, package guard, UAT
   results, and the release evidence checks of section 4);
4. the wheel and sdist, and the PyInstaller binaries for linux-amd64,
   macos-amd64 and macos-arm64, each smoke-tested;
5. the GitHub Release with the binaries and their `SHA256SUMS`, which
   `install.sh` checks each download against;
6. the PyPI upload, only after the GitHub Release exists, so a version on
   PyPI always has its binaries.

Steps 5 and 6 run only in `PureStorage-OpenConnect/lakebench-k8s`. On a
fork they are skipped and `release-dry-run` downloads the same artifacts,
lists them, writes the same `SHA256SUMS` and runs `install.sh` against
them instead. A tag on a fork therefore runs the build and artifact steps
without publishing; the GitHub Release and PyPI upload actions themselves
run only on a real tag. `release.yml` has no `workflow_dispatch` trigger,
because a manual run in the upstream repository would reach the PyPI
upload.

## 8. After the release

- Set `__version__` to the next `X.Y.0.dev0` and open a new
  `## [Unreleased]` section in `CHANGELOG.md`.
- Remove the release worktree (`git worktree remove`).
