"""Read-only cluster queries and the admission decision for the release harness.

Everything here reads; nothing creates, patches or deletes. The harness
builds one ``CoreV1Api`` pinned to ``--context`` and passes it in.

Admission (``admit``) is a pure function of what was read, so it is unit
tested without a cluster:

* **Count.** Lakebench deployments are the union of namespaces labelled
  ``app.kubernetes.io/managed-by=lakebench``, live rows of the markdown
  deployments ledger (a row admitted by the main lane before its namespace
  exists) and this harness's own active rows. The union must be below
  ``max_deployments`` (4, CLAUDE.md section 8) before a row is admitted.
* **Load.** Every namespace's pod requests (non-terminal pods), except that
  a ledger or own namespace counts at least its plan peak: the harness's own
  rows by their computed peak, other ledger rows by the peak of their config
  when it loads, and otherwise by ``fallback_peak`` (the largest peak of the
  rows this harness plans, or of any default sizing cell at the row's
  scale), because a deployment between jobs requests almost nothing. The
  candidate's own namespaces are counted once, by its peak. Load plus the row's peak must stay within ``fraction`` of
  the schedulable nodes' allocatable cores and memory.
* **Fail closed.** Unreadable nodes or pods, or no schedulable node, admit
  nothing.
* **Shape.** At most ``slots`` own rows at once; at most
  ``max_aml_continuous`` own AML continuous rows; an ``alone`` row needs no
  other lakebench deployment and blocks every other admission while active,
  including one marked ``alone`` in the ledger by another harness process.

Scratch capacity is not part of this decision: node allocatable does not
show it, and each ``run``'s own capacity preflight checks scratch.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

MANAGED_BY_SELECTOR = "app.kubernetes.io/managed-by=lakebench"
GIB = 1024**3


@dataclass(frozen=True)
class Peak:
    cores: float
    gib: float

    def __add__(self, other: Peak) -> Peak:
        return Peak(self.cores + other.cores, self.gib + other.gib)

    def max(self, other: Peak) -> Peak:
        return Peak(max(self.cores, other.cores), max(self.gib, other.gib))


ZERO = Peak(0.0, 0.0)


@dataclass(frozen=True)
class Snapshot:
    """Schedulable allocatable and per-namespace requests at one moment."""

    allocatable: Peak
    requests: Mapping[str, Peak]


@dataclass(frozen=True)
class Unknown:
    reason: str


@dataclass(frozen=True)
class ActiveRow:
    namespace: str
    peak: Peak
    alone: bool = False
    aml_continuous: bool = False


@dataclass(frozen=True)
class Candidate:
    """What asks for admission: one row, or a scenario's ``size``
    deployments at once (``namespaces``, ``peak`` their sum)."""

    row: str
    namespace: str
    peak: Peak
    alone: bool = False
    aml_continuous: bool = False
    size: int = 1
    namespaces: tuple[str, ...] = ()


@dataclass
class Decision:
    admit: bool
    reasons: list[str] = field(default_factory=list)
    blocking: list[str] = field(default_factory=list)


def admit(
    cand: Candidate,
    snapshot: Snapshot | Unknown,
    *,
    managed: Iterable[str],
    ledger_live: Iterable[str],
    own_active: Iterable[ActiveRow],
    ledger_peaks: Mapping[str, Peak | None],
    fallback_peak: Peak,
    ledger_alone: Iterable[str] = (),
    slots: int = 3,
    max_deployments: int = 4,
    fraction: float = 0.8,
    max_aml_continuous: int = 2,
) -> Decision:
    """Whether *cand* may be ledgered and deployed now."""
    own = list(own_active)
    own_ns = {a.namespace for a in own}
    managed_set = set(managed)
    ledger_set = set(ledger_live)
    reasons: list[str] = []
    blocking: list[str] = []

    if len(own) >= slots:
        reasons.append(f"{len(own)} of {slots} harness slots in use")
    mine = {cand.namespace, *cand.namespaces}
    if any(a.alone for a in own):
        reasons.append("an alone row is running")
    foreign_alone = sorted(set(ledger_alone) - own_ns - mine)
    if foreign_alone:
        reasons.append("an alone deployment of another harness is in the ledger")
        blocking.extend(foreign_alone)
    if cand.aml_continuous and sum(a.aml_continuous for a in own) >= max_aml_continuous:
        reasons.append(f"{max_aml_continuous} AML continuous rows already running")
    deployments = (managed_set | ledger_set | own_ns) - mine
    if cand.alone and deployments:
        reasons.append("alone row: other lakebench deployments exist")
        blocking.extend(sorted(deployments - own_ns))
    if len(deployments) + cand.size > max_deployments:
        reasons.append(
            f"{len(deployments)} lakebench deployments plus {cand.size} more would pass "
            f"the limit of {max_deployments}"
        )
        blocking.extend(sorted(deployments - own_ns))

    if isinstance(snapshot, Unknown):
        reasons.append(f"cluster capacity unreadable: {snapshot.reason}")
        return Decision(False, reasons, sorted(set(blocking)))

    load = ZERO
    own_peaks = {a.namespace: a.peak for a in own}
    for ns in (set(snapshot.requests) | own_ns | ledger_set) - mine:
        req = snapshot.requests.get(ns, ZERO)
        if ns in own_peaks:
            load = load + req.max(own_peaks[ns])
        elif ns in ledger_set:
            peak = ledger_peaks.get(ns) or fallback_peak
            load = load + req.max(peak)
        else:
            load = load + req
    limit = Peak(snapshot.allocatable.cores * fraction, snapshot.allocatable.gib * fraction)
    need = load + cand.peak
    if need.cores > limit.cores or need.gib > limit.gib:
        reasons.append(
            f"load {load.cores:.0f} cores / {load.gib:.0f} GiB plus the row's "
            f"{cand.peak.cores:.0f} / {cand.peak.gib:.0f} exceeds {fraction:.0%} of "
            f"allocatable ({limit.cores:.0f} cores / {limit.gib:.0f} GiB)"
        )
        heavy = sorted(
            ((ns, r) for ns, r in snapshot.requests.items() if ns not in own_ns),
            key=lambda kv: -kv[1].cores,
        )[:5]
        blocking.extend(ns for ns, _ in heavy)
    return Decision(not reasons, reasons, sorted(set(blocking)))


class ClusterReader:
    """Read-only queries over one pinned ``CoreV1Api``."""

    def __init__(self, core_v1: Any) -> None:
        self.core_v1 = core_v1

    def namespace_identity(self, namespace: str) -> Any:
        """The release tree's ``NamespaceIdentity`` or None when absent;
        raises on any other API error."""
        from lakebench.config.deploy_state import read_namespace_identity

        return read_namespace_identity(self.core_v1, namespace)

    def namespace_exists(self, namespace: str) -> bool:
        return self.namespace_identity(namespace) is not None

    def managed_namespaces(self) -> set[str]:
        items = self.core_v1.list_namespace(
            label_selector=MANAGED_BY_SELECTOR, _request_timeout=(10, 60)
        ).items
        return {ns.metadata.name for ns in items}

    def snapshot(self) -> Snapshot | Unknown:
        """Schedulable allocatable and per-namespace requests; Unknown on any
        failure (fail closed)."""
        from lakebench.k8s.client import _schedulable
        from lakebench.quantity import QuantityError, parse, pod_request

        try:
            nodes = self.core_v1.list_node(_request_timeout=(10, 60)).items
        except Exception as e:  # noqa: BLE001 -- any failure means unknown
            return Unknown(f"listing nodes failed ({type(e).__name__})")
        try:
            pods = self.core_v1.list_pod_for_all_namespaces(
                field_selector="status.phase!=Succeeded,status.phase!=Failed",
                _request_timeout=(10, 120),
            ).items
        except Exception as e:  # noqa: BLE001
            return Unknown(f"listing pods failed ({type(e).__name__})")
        cores = gib = 0.0
        schedulable = 0
        try:
            for node in nodes:
                if not _schedulable(node):
                    continue
                alloc = node.status.allocatable or {}
                cores += float(parse(str(alloc.get("cpu", 0))))
                gib += float(parse(str(alloc.get("memory", 0)))) / GIB
                schedulable += 1
            requests: dict[str, Peak] = {}
            for pod in pods:
                cpu, mem = pod_request(pod)
                ns = pod.metadata.namespace
                requests[ns] = requests.get(ns, ZERO) + Peak(cpu, mem / GIB)
        except (QuantityError, AttributeError, TypeError, ValueError) as e:
            return Unknown(f"unreadable node or pod resources ({e})")
        if not schedulable:
            return Unknown("no schedulable node")
        return Snapshot(Peak(cores, gib), requests)
