"""Where a registered (held-out) AML corpus's seed lives on the cluster.

Owner decision 10-03: a registered corpus's seed reaches the cluster only
through a Kubernetes Secret in the deployment's namespace. The datagen Job
and the reference scorer read it as ``LB_DATAGEN_SEED`` from a secretKeyRef,
so it is in no Job argument, pod spec or SparkApplication spec, and
``scripts/aml_gate.py`` reads it from an owner-only file. Development seeds
(43 and every other seed that is not held out) are passed as before.

The Secret is named after the seed's ``seed_ref`` (a salted hash), so a name
is never reused for another seed: a kubelet that still caches an earlier,
immutable Secret can never hand its value to a new pod. This module is pure;
``deploy/datagen.py`` writes and deletes the Secret.
"""

from __future__ import annotations

from typing import Any

from lakebench.config.datagen_seed import PROTECTED_ROLES, config_seed, heldout_role, seed_ref

#: Name prefix; the full name adds the first 16 hex characters of seed_ref.
SEED_SECRET_PREFIX = "lakebench-datagen-seed-"
SEED_SECRET_KEY = "seed"
#: The component label every seed Secret carries (destroy deletes by it).
SEED_SECRET_COMPONENT = "datagen-seed"
#: Annotation naming the Secret's seed by its full seed_ref.
SEED_REF_ANNOTATION = "lakebench.io/seed-ref"
#: The environment variable the datagen entrypoint, the generator and the
#: reference scorer read the seed from.
SEED_ENV = "LB_DATAGEN_SEED"


def uses_seed_secret(cfg: Any) -> bool:
    """Whether this config's corpus seed goes to the cluster through a seed
    Secret: a financial config that declares the evaluation or robustness
    role, or whose seed hashes to a held-out seed. A hash record that cannot
    be read answers True (fail closed). Every other config, seed 43
    included, passes its seed in plaintext as before."""
    workload = cfg.architecture.workload
    if workload.schema_type.value != "financial":
        return False
    dg = workload.datagen
    if getattr(dg, "corpus_role", None) in PROTECTED_ROLES:
        return True
    seed = getattr(dg, "seed", None)
    if seed is None:
        return False
    try:
        return heldout_role(int(seed)) is not None
    except Exception:  # noqa: BLE001 -- unreadable record: fail closed
        return True


def config_seed_ref(cfg: Any) -> str:
    """The salted hash naming this config's financial corpus seed."""
    return seed_ref("financial", config_seed(cfg))


def seed_secret_name(cfg: Any) -> str:
    """The seed Secret's name for this config's seed."""
    return SEED_SECRET_PREFIX + config_seed_ref(cfg)[:16]


def seed_secret_selector(deployment: str) -> str:
    """Label selector for every seed Secret of a deployment."""
    return (
        f"app.kubernetes.io/component={SEED_SECRET_COMPONENT},"
        f"app.kubernetes.io/instance={deployment}"
    )


def seed_secret_env(cfg: Any) -> dict:
    """The container env entry that reads the seed from the config's Secret."""
    return {
        "name": SEED_ENV,
        "valueFrom": {
            "secretKeyRef": {
                "name": seed_secret_name(cfg),
                "key": SEED_SECRET_KEY,
                "optional": False,
            }
        },
    }
