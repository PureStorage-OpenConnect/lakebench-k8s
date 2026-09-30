# Security policy

## Reporting a vulnerability

Report security problems privately through GitHub's private vulnerability
reporting: open the repository's **Security** tab and choose **Report a
vulnerability**
(<https://github.com/PureStorage-OpenConnect/lakebench-k8s/security/advisories/new>).
Do not open a public issue, pull request or discussion for a suspected
vulnerability.

Include what you can of the following:

- the lakebench version (`lakebench version`) and how it was installed
  (PyPI, binary, source);
- the affected command or component, and the configuration involved with any
  credentials removed;
- steps to reproduce, and what an attacker gains.

You should get an acknowledgement within five working days. We will agree a
disclosure date with you once the problem is understood, and credit you in
the advisory unless you ask us not to.

## Supported versions

Security fixes go into the latest released minor version. Older versions are
not patched; upgrade to the latest release.

## Scope

lakebench deploys workloads into a Kubernetes cluster and reads and writes
S3 buckets with credentials you supply. Reports we treat as vulnerabilities
include:

- credentials written somewhere they should not be: logs, `metrics.json`,
  journal files, ConfigMaps, or anything committed to this repository;
- a `destroy` or `clean` that deletes resources belonging to another
  deployment or another user;
- manifests that grant more cluster privilege than the documented
  requirements (for example the `anyuid` SCC on OpenShift).

Weaknesses in the components lakebench deploys (Spark, Trino, Hive, Polaris,
PostgreSQL) belong with those projects, unless lakebench configures them in
an unsafe way.

## Secrets in this repository

CI runs [gitleaks](https://github.com/gitleaks/gitleaks) on every push with
the rules in `.gitleaks.toml`, and the same scan is available as a pre-commit
hook (`.pre-commit-config.yaml`). If you find a credential in the tree or in
history, report it through the private channel above.
