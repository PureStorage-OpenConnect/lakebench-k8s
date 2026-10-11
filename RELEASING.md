# Releasing lakebench

How a lakebench release is cut, for maintainers. Only the owner merges to
`main`, tags and approves the PyPI upload.

## 1. The release commit

- The version lives only in `src/lakebench/__init__.py` (`__version__`);
  `pyproject.toml` reads it through hatch. During a cycle it is
  `X.Y.0.dev0`.
- `CHANGELOG.md` keeps a `## [Unreleased]` section; changes are recorded as
  they merge, one line each, breaking changes under `### Breaking changes`.
- The release commit sets `__version__` to `X.Y.Z` and renames the
  changelog section to `## [X.Y.Z] - <date>`. It reaches `main` through a
  pull request with CI green.

## 2. Local checks before the release pull request

```bash
make release-check VERSION=X.Y.Z          # every step, in order
make release-check VERSION=X.Y.Z DRY=1    # wiring only
make rc-<step> VERSION=X.Y.Z              # one step alone
```

`release-check` needs GNU make and a clean tree, refuses `-i` and `-k`, and
stops at the first failing step. Python steps run with `PYTHONPATH=src`;
`PYTHON=python3.11` picks the interpreter.

| Step | Command |
|---|---|
| `version` | `scripts/check_version.py --tag vX.Y.Z`: the tag is `v` plus the package version, a final release |
| `filler-words` | `git grep` for the Makefile's `FILLER_WORDS` and `AI_VOICE` phrases |
| `build` | `python -m build` into an empty `DIST` directory (a temporary one by default) |
| `package-guard` | `scripts/package_guard.py --dist <dir> --require-all` |
| `gate` | `scripts/release_gate.py --tag vX.Y.Z --require-all` |

`scripts/release_gate.py` with no `--only` runs every check:

- ruff, mypy and the unit tests;
- cargo fmt, clippy and test;
- gitleaks over the tree and over the full history beyond
  `.gitleaksignore` (`scripts/gitleaks_history.py`);
- the package guard and the installed pre-push hook;
- examples, version, changelog and prose.

It needs `cargo` and `gitleaks` on `PATH` and a full
clone. The Spark tier and the `slow` AML tests are not in CI; run them
locally before the release pull request.

## 3. Owner checklist

1. Merge the release pull request to `main`.
2. Tag the merged commit and push the tag:

   ```bash
   git tag -a vX.Y.Z -m "lakebench X.Y.Z"
   git push origin vX.Y.Z
   ```

3. Approve the `pypi` environment deployment once the GitHub Release is up.
   If the PyPI job fails, re-run that job; do not re-tag.
4. Confirm every credential ever exposed in published history is rotated.

## 4. What the tag workflow does

`.github/workflows/release.yml`, on a `v*` tag:

1. `ci`: the full CI workflow on the tagged commit.
2. `verify-tag`: the tested commit is on `origin/main` and the tag is `v`
   plus the package version (`scripts/check_version.py`).
3. `gate`: `scripts/release_gate.py --require-all --only
   examples,version,changelog,prose,package-guard`.
4. The wheel and sdist (package guard with `--require-all` on the files it
   uploads), and PyInstaller binaries for linux-amd64 (built on Rocky Linux
   8, glibc 2.28), macos-amd64 and macos-arm64, each smoke-tested.
5. The GitHub Release with the binaries and `SHA256SUMS`, which `install.sh`
   checks each download against.
6. The PyPI upload, after the GitHub Release, so a version on PyPI always
   has its binaries.

Steps 5 and 6 run only in `PureStorage-OpenConnect/lakebench-k8s`. There is
no `workflow_dispatch` trigger: a manual run upstream would reach PyPI.

## 5. Repository settings

A tag runs the `release.yml` of the commit it points at, so a tag on an
older commit runs that commit's gates. That is how 1.5.0 reached PyPI from
an unmerged commit. These settings close it:

| Setting | Where | Value |
|---|---|---|
| Required reviewer on the `pypi` environment | Settings, Environments, `pypi` | at least one maintainer; deployment restricted to `v*` tags |
| Tag ruleset for `v*` | Settings, Rules, Rulesets | creation, update and deletion restricted to maintainers; no force pushes |
| Branch protection on `main` | Settings, Rules | pull request and CI checks required; no force pushes |
| PyPI trusted publisher | pypi.org, lakebench-k8s, Publishing | this repository, workflow `release.yml`, environment `pypi` |

The trusted publisher is bound to the workflow file name; do not rename
`release.yml` without updating PyPI.

## 6. After the release

Set `__version__` to the next `X.Y.0.dev0` and open a new `## [Unreleased]`
section in `CHANGELOG.md`.
