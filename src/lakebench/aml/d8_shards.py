"""D8 shard plan helpers (AML-GOALS 5a D8, pre-registration scale_invariance.shard_rule).

The scale-10 corpus is split into customer shards that each look like a
scale-2 corpus. A planted typology instance must never span shards: its crew
and counterparties would be scored in one shard while their shared signal sits
in another, so the shard AP would not be the AP of an s2-like corpus. The
instances therefore define a graph over entity keys (every party of an
instance is joined to every other), and each connected component goes to one
shard whole.

Standard library only: aml_features imports it on the Spark driver.
"""

from __future__ import annotations

from collections.abc import Hashable, Iterable, Mapping


def components(instances: Iterable[Iterable[Hashable]]) -> dict:
    """{key: representative} for every key in any instance, where the
    representative is the smallest key of the key's connected component (two
    instances sharing any key are one component). Union-find with path
    halving; deterministic for a given input set."""
    parent: dict = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for inst in instances:
        keys = [k for k in inst if k is not None]
        if not keys:
            continue
        root = find(keys[0])
        for k in keys[1:]:
            other = find(k)
            if other == root:
                continue
            # The smaller key stays the root, so the root is the component's
            # smallest key.
            if other < root:
                parent[root] = other
                root = other
            else:
                parent[other] = root
    return {k: find(k) for k in list(parent)}


def component_sizes(reps: Mapping) -> dict:
    """{representative: number of keys} for a components() result."""
    out: dict = {}
    for r in reps.values():
        out[r] = out.get(r, 0) + 1
    return out


def spanning_instances(instances: Mapping, shard_of: Mapping) -> list:
    """Instance ids whose parties land in more than one shard. ``instances``
    maps an instance id to its party keys, ``shard_of`` maps a key to its
    shard; keys without a shard (non-customers, which are never scored) are
    ignored. Any result is a leak: the plan must be refused."""
    bad = []
    for iid, keys in instances.items():
        shards = {shard_of[k] for k in keys if k in shard_of}
        if len(shards) > 1:
            bad.append(iid)
    return sorted(bad, key=str)


def check_plan(instances: Mapping, shard_of: Mapping) -> None:
    """Raise ValueError when any instance spans shards (fail closed)."""
    bad = spanning_instances(instances, shard_of)
    if bad:
        raise ValueError(
            f"D8 shard plan leaks: {len(bad)} planted instance(s) span shards "
            f"(first: {bad[0]}); refusing to score"
        )
