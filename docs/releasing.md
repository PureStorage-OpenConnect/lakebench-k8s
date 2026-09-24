# Releasing lakebench

A release is a `v*` tag on a commit that is on `main`. Pushing the tag runs
`.github/workflows/release.yml`, which does, in order:

1. the full CI workflow (`ci.yml`) on the tagged commit;
2. `verify-tag`: the tested commit (`$GITHUB_SHA`) is reachable from
   `origin/main`, and the tag is exactly `v` plus the normalised package
   version, which must be a final release: no dev or pre-release suffix
   (`scripts/check_version.py`);
3. `gate`: the release-only checks of `scripts/release_gate.py` with
   `--require-all` (examples, version, changelog, em dashes, UAT results);
4. the wheel and sdist, and the PyInstaller binaries for linux-amd64,
   macos-amd64 and macos-arm64, each smoke-tested;
5. the GitHub Release with the binaries;
6. the PyPI upload, only after the GitHub Release exists, so a version on
   PyPI always has its binaries.

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
pointing at the binary.

The version lives only in `src/lakebench/__init__.py`; `pyproject.toml`
reads it through hatch. Set it to the release version (no `.dev` suffix),
add a `## [<version>]` section to `CHANGELOG.md`, and commit both through a
pull request to `main`.

### UAT results

The gate requires `uat/results-<version>.md`, for example
`uat/results-1.6.0.md`, with a heading line that is exactly
`# UAT results <version>` and a markdown table with at least one data row.
It is the record of the live-cluster runs behind the release: one row per
recipe, workload and mode tested, with its result and the run id that
resolves to `lakebench-output/runs/run-<id>/metrics.json`. The gate checks
the heading and that a row exists; the maintainer who tags is responsible
for the content.

## After tagging

```bash
git tag -a v<version> -m "lakebench <version>"
git push origin v<version>
```

Approve the `pypi` environment deployment once the GitHub Release is up.
If the PyPI job fails, re-run that job; do not re-tag.
