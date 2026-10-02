"""Recording fake of the cluster: the oracle for SAF-4 and DEP-3 (G-3, SD-9).

What it replaces, for the duration of one test:

- every ``kubernetes.client.*Api`` class (``CoreV1Api``, ``AppsV1Api``,
  ``CustomObjectsApi`` and the rest, including ones added later),
  ``kubernetes.stream.stream`` and the kubeconfig loaders;
- ``subprocess.run`` and ``subprocess.Popen`` (and so ``check_output``,
  ``call`` and the ``lakebench.k8s._pinned`` helpers): no process starts;
- ``boto3.client``.

What it does not see: HTTP clients aimed at in-cluster services (``httpx``
reads of Prometheus), raw sockets, anything a child process does, and calls a
test mocks out above these entry points (``patch.object(SparkOperatorManager,
"_run")``). A child ``lakebench`` process and any unrecognised command are
therefore violations, not silent passes. A test patch that replaces one of
the entry points again (``patch("subprocess.run")``, ``patch.object(_pinned,
"subprocess")``, ``monkeypatch.setattr``) is a violation too: ``mock.patch``
is caught when entered, a monkeypatch still in place at the assert. Consumers
prove their run reached the mutation under test with
:meth:`K8sRecorder.assert_recorded` (invariant 3: exit 0 is not a pass).

Every call is recorded as a :class:`Call` and classified:

- **read**: never a violation on its own.
- **local**: ``git``, ``podman``, ``docker`` and shell no-ops (``echo``, ``sleep``).
- **own**: a mutation in the config's namespace of a kind known to be
  namespaced, of the config's namespace object, of the config's two
  SecretClasses (``lakebench-s3-credentials-<ns>``,
  ``lakebench-s3-ca-cert-<ns>``, exact names), or of one of the config's
  buckets this deployment provably owns: its ``lakebench.deployment`` tag, a
  create in this test, or the namespace's created-buckets or
  adopted-empty-buckets annotation. Tagging an untagged config bucket (deploy
  adopting it) is own; deleting a bucket needs proof this deployment created
  it. A config bucket with no such proof is shared (``ownership.py`` refuses
  legacy buckets).
- **lease**: a mutation of the lease ConfigMap
  ``lakebench-system/lakebench-cluster-lock``, or the bootstrap create of the
  ``lakebench-system`` namespace that holds it.
- **shared**: every other mutation: any cluster-scoped kind not named above,
  any other namespace, every mutating ``helm`` verb (a chart can carry
  cluster-scoped objects whatever ``-n`` says), every mutating ``oc adm``
  verb, and anything whose target cannot be proven own (a kind the fixture
  does not know, a missing ``-n``, a custom kind with no namespaced CRD).

Violations, raised by :meth:`K8sRecorder.assert_clean` (and by the pytest
fixture at teardown):

- a shared mutation made while the calling thread does not hold the lease;
- a delete outside the config's namespace, held lease or not, unless the
  test names the target in ``allow_delete`` (``kubectl replace --force``,
  ``apply --prune`` and ``drain`` count as deletes);
- a write to a bucket the config does not own, or a delete of a config
  bucket this deployment did not create;
- taking over or deleting a live lease another process (or thread) holds,
  and any CLI mutation of the lease ConfigMap (``cluster_lock`` is API-only);
- a test patch over one of the recorder's entry points;
- a child ``lakebench`` process or an unrecognised external command;
- a cluster read the fake could not answer (script it with
  :meth:`K8sRecorder.on_command`), because real code that gets a made-up
  answer takes an error path and skips the mutations under test.

A mutation is recorded before it runs, so an attempt counts even when the
fake answers 404 or 409. ``lease_held`` is true for calls from the thread
that created (or took over, after expiry) the lease ConfigMap through the
Kubernetes API, until that lease is deleted.

The in-memory store lets real code run: namespaces, the lease ConfigMap,
Deployments and pods (the Spark Operator with its ``--namespaces=`` args),
CRDs with their scope, custom objects, helm releases with their values, and
buckets with objects and tags. Create answers 409 on an existing object and
404 on a missing namespace or CRD; replace and delete honour resourceVersion
and uid preconditions. ``helm get values``, ``list``, ``status``,
``upgrade --set`` (which also rewrites the operator's ``--namespaces=``),
``install`` and ``uninstall`` work on the release store; ``kubectl get -o
json`` and simple ``-o jsonpath=`` reads, and ``api-resources``, come from the
store. Other ``kubectl`` mutations answer success and do not change the store.
Namespace deletes are immediate (no Terminating phase). ``s3_tagging = False``
makes the bucket-tagging calls answer ``NotImplemented`` (FlashBlade), and
:meth:`K8sRecorder.fail_s3` and :meth:`K8sRecorder.fail` inject errors.

Usage::

    def test_destroy_only_touches_its_namespace(recording_k8s, cfg):
        recording_k8s.for_config(cfg)
        recording_k8s.add_namespace(cfg.get_namespace())
        recording_k8s.add_spark_operator(watched=[cfg.get_namespace()])
        ...  # run the code under test
        recording_k8s.assert_recorded(api="helm", verb="upgrade", lease_held=True)
        # teardown calls recording_k8s.assert_clean()

The fixture is registered for every test in ``tests/conftest.py``.
"""

from __future__ import annotations

import copy
import dataclasses
import fnmatch
import io
import itertools
import json
import os
import re
import shlex
import subprocess
import threading
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from lakebench.deploy.cluster_lock import LOCK_CONFIGMAP_NAME, LOCK_NAMESPACE

# ---------------------------------------------------------------------------
# Constants and kind tables
# ---------------------------------------------------------------------------

CLUSTER_TOOLS = frozenset({"kubectl", "oc", "helm"})
LOCAL_TOOLS = frozenset({"git", "podman", "docker", "echo", "printf", "true", "false", "sleep"})
# Prefixes that run another command; the fixture classifies what they run.
WRAPPERS = frozenset({"env", "timeout", "nice", "nohup", "stdbuf", "sudo", "xargs"})
SHELLS = frozenset({"sh", "bash", "dash", "zsh"})

READ = "read"
LOCAL = "local"
OWN = "own"
LEASE = "lease"
SHARED = "shared"

REASON_UNLEASED = "shared mutation without the cluster lease"
REASON_FOREIGN_DELETE = "delete outside the config's namespace"
REASON_FOREIGN_BUCKET = "write to a bucket the config does not own"
REASON_NOT_CREATED = "delete of a bucket this deployment did not create"
REASON_LEASE_TAKEN = "took over or deleted a live lease another process holds"
REASON_CHILD = "child lakebench process: its cluster calls are not recorded"
REASON_UNKNOWN_TOOL = "unrecognised external command: the fixture cannot see what it touches"
REASON_UNSCRIPTED = "cluster read the fake could not answer; script it with on_command()"
REASON_LEASE_CLI = "lease ConfigMap changed through the CLI, outside cluster_lock"
REASON_PATCHED_OVER = (
    "a test patch replaced a recorded entry point; calls through it went unrecorded"
)

_NOTE_REASONS = {
    "lease-taken": REASON_LEASE_TAKEN,
    "child": REASON_CHILD,
    "unknown-tool": REASON_UNKNOWN_TOOL,
    "unscripted": REASON_UNSCRIPTED,
    "not-created": REASON_NOT_CREATED,
    "lease-cli": REASON_LEASE_CLI,
    "patched-over": REASON_PATCHED_OVER,
}

TAG_DEPLOYMENT = "lakebench.deployment"
TAG_CREATED = "lakebench.created"


def own_secretclass_names(namespace: str) -> frozenset[str]:
    """The two cluster-scoped SecretClasses a deployment owns (exact names)."""
    return frozenset({f"lakebench-s3-credentials-{namespace}", f"lakebench-s3-ca-cert-{namespace}"})


def _words(text: str) -> frozenset[str]:
    return frozenset(text.split())


# kubectl spellings (singular, plural, short names) -> canonical plural.
_KIND_ALIASES = {
    "ns": "namespaces",
    "namespace": "namespaces",
    "project": "projects",
    "po": "pods",
    "pod": "pods",
    "svc": "services",
    "service": "services",
    "deploy": "deployments",
    "deployment": "deployments",
    "sts": "statefulsets",
    "statefulset": "statefulsets",
    "ds": "daemonsets",
    "daemonset": "daemonsets",
    "rs": "replicasets",
    "replicaset": "replicasets",
    "job": "jobs",
    "cj": "cronjobs",
    "cronjob": "cronjobs",
    "cm": "configmaps",
    "configmap": "configmaps",
    "secret": "secrets",
    "sa": "serviceaccounts",
    "serviceaccount": "serviceaccounts",
    "role": "roles",
    "rolebinding": "rolebindings",
    "clusterrole": "clusterroles",
    "clusterrolebinding": "clusterrolebindings",
    "pvc": "persistentvolumeclaims",
    "persistentvolumeclaim": "persistentvolumeclaims",
    "pv": "persistentvolumes",
    "persistentvolume": "persistentvolumes",
    "sc": "storageclasses",
    "storageclass": "storageclasses",
    "crd": "customresourcedefinitions",
    "crds": "customresourcedefinitions",
    "customresourcedefinition": "customresourcedefinitions",
    "scc": "securitycontextconstraints",
    "securitycontextconstraint": "securitycontextconstraints",
    "no": "nodes",
    "node": "nodes",
    "ep": "endpoints",
    "ing": "ingresses",
    "ingress": "ingresses",
    "netpol": "networkpolicies",
    "networkpolicy": "networkpolicies",
    "pdb": "poddisruptionbudgets",
    "poddisruptionbudget": "poddisruptionbudgets",
    "ev": "events",
    "event": "events",
    "lease": "leases",
    "limits": "limitranges",
    "limitrange": "limitranges",
    "quota": "resourcequotas",
    "resourcequota": "resourcequotas",
    "hpa": "horizontalpodautoscalers",
    "horizontalpodautoscaler": "horizontalpodautoscalers",
    "pc": "priorityclasses",
    "priorityclass": "priorityclasses",
    "route": "routes",
    "secretclass": "secretclasses",
    "sparkapp": "sparkapplications",
    "sparkapplication": "sparkapplications",
    "scheduledsparkapplication": "scheduledsparkapplications",
    "hivecluster": "hiveclusters",
    "trinocluster": "trinoclusters",
    "trinocatalog": "trinocatalogs",
    "podmonitor": "podmonitors",
    "servicemonitor": "servicemonitors",
    "prometheusrule": "prometheusrules",
    "mutatingwebhookconfiguration": "mutatingwebhookconfigurations",
    "validatingwebhookconfiguration": "validatingwebhookconfigurations",
    "apiservice": "apiservices",
    "csidriver": "csidrivers",
    "ingressclass": "ingressclasses",
    "runtimeclass": "runtimeclasses",
    "volumesnapshotclass": "volumesnapshotclasses",
    "controllerrevision": "controllerrevisions",
    "clusterversion": "clusterversions",
}

# Kinds the API server keeps at cluster scope: `-n` does not scope them.
CLUSTER_SCOPED_KINDS = _words(
    "namespaces projects nodes persistentvolumes storageclasses clusterroles "
    "clusterrolebindings customresourcedefinitions securitycontextconstraints "
    "priorityclasses mutatingwebhookconfigurations validatingwebhookconfigurations "
    "apiservices secretclasses csidrivers csinodes ingressclasses runtimeclasses "
    "volumesnapshotclasses clusterissuers clusterversions authenticationclasses "
    "listenerclasses"
)

# Kinds known to be namespaced. A mutation counts as own only for one of
# these (or the namespace object, an own SecretClass, or a custom kind whose
# seeded CRD says Namespaced): an unknown kind is shared, so a new
# cluster-scoped kind fails closed.
NAMESPACED_KINDS = _words(
    "all pods services deployments statefulsets daemonsets replicasets jobs "
    "cronjobs configmaps secrets serviceaccounts roles rolebindings "
    "persistentvolumeclaims endpoints ingresses networkpolicies "
    "poddisruptionbudgets events leases limitranges resourcequotas "
    "horizontalpodautoscalers routes sparkapplications scheduledsparkapplications "
    "hiveclusters trinoclusters trinocatalogs podmonitors servicemonitors "
    "prometheusrules controllerrevisions"
)

# API "create" calls that ask a question and change nothing.
_REVIEW_KINDS = _words(
    "selfsubjectaccessreviews subjectaccessreviews localsubjectaccessreviews "
    "selfsubjectrulesreviews selfsubjectreviews tokenreviews"
)

_API_METHOD_RE = re.compile(r"^(create|read|list|patch|replace|delete|get|connect|watch)_")
_API_SUBRESOURCES = (
    "_ephemeralcontainers",
    "_eviction",
    "_finalize",
    "_resize",
    "_status",
    "_scale",
    "_token",
    "_log",
)
_CONNECT_SUFFIXES = ("_exec", "_attach", "_portforward", "_proxy_with_path", "_proxy")
_META_KINDS = frozenset({"code", "api_versions", "api_resources", "api_group", "api"})


def _pluralize(word: str) -> str:
    if word.endswith(("ss", "x", "ch", "sh")):
        return word + "es"
    if word.endswith("s"):
        return word
    if word.endswith("y") and len(word) > 1 and word[-2] not in "aeiou":
        return word[:-1] + "ies"
    return word + "s"


def canonical_kind(kind: str) -> str:
    """Lower-case plural resource name with any API group dropped."""
    k = (kind or "").strip().lower()
    if not k:
        return ""
    k = k.split(".", 1)[0]
    if k in _KIND_ALIASES:
        return _KIND_ALIASES[k]
    if k in NAMESPACED_KINDS or k in CLUSTER_SCOPED_KINDS or k in _REVIEW_KINDS:
        return k
    return _pluralize(k)


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Call:
    """One recorded call.

    ``api`` is the Kubernetes API class name, the CLI tool (``kubectl``,
    ``helm``, ``oc``, ``git`` ...), ``s3`` or ``lakebench`` for a child
    process. ``kind`` is the canonical plural resource (``configmaps``,
    ``secretclasses``), ``releases`` for helm, ``buckets`` or ``objects`` for
    S3. ``scope`` is read, local, own, lease or shared (module docstring).
    ``lease_held`` is the calling thread's lease state before the call ran.
    ``note`` marks a call that is a violation for its own reason (a taken
    lease, an unscripted read, a child process).
    """

    api: str
    verb: str
    kind: str
    namespace: str | None
    name: str | None
    lease_held: bool
    argv: tuple[str, ...] | None = None
    mutating: bool = False
    deleting: bool = False
    scope: str = READ
    method: str = ""
    note: str = ""
    kwargs: dict[str, Any] = field(default_factory=dict, compare=False, hash=False)

    @property
    def target(self) -> str:
        """``kind/namespace/name`` for namespaced targets, else ``kind/name``."""
        parts = [self.kind or "?"]
        if self.namespace:
            parts.append(self.namespace)
        parts.append(self.name or "*")
        return "/".join(parts)

    def describe(self) -> str:
        how = " ".join(self.argv) if self.argv else f"{self.api}.{self.method or self.verb}"
        note = f", {self.note}" if self.note else ""
        return f"{self.verb} {self.target} [{self.scope}, lease_held={self.lease_held}{note}] via {how}"


@dataclass(frozen=True)
class Violation:
    call: Call
    reason: str

    def __str__(self) -> str:
        return f"{self.reason}: {self.call.describe()}"


@dataclass
class _Responder:
    prefix: tuple[str, ...]
    returncode: int = 0
    stdout: str = ""
    stderr: str = ""
    handler: Callable[[list[str], dict[str, Any]], Any] | None = None
    raises: BaseException | None = None


@dataclass
class _InjectedError:
    verb: str | None
    kind: str | None
    name: str | None
    namespace: str | None
    status: int
    times: int | None


@dataclass
class Release:
    """A helm release in the fake: what ``helm get values`` and ``list`` read."""

    name: str
    namespace: str
    chart: str
    version: str
    values: dict[str, Any] = field(default_factory=dict)
    revision: int = 1
    status: str = "deployed"


@dataclass
class _Lease:
    holder: str
    thread: int


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _api_exception(status: int, reason: str) -> Exception:
    from kubernetes.client.exceptions import ApiException

    return ApiException(status=status, reason=reason)


def _get(obj: Any, *path: str) -> Any:
    """Read a nested field from a model (snake_case) or a dict (either case)."""
    cur = obj
    for p in path:
        if cur is None:
            return None
        if isinstance(cur, dict):
            camel = re.sub(r"_([a-z])", lambda m: m.group(1).upper(), p)
            cur = cur.get(p, cur.get(camel))
        else:
            cur = getattr(cur, p, None)
    return cur


def _split_top(text: str, sep: str = ",") -> list[str]:
    """Split on ``sep`` outside parentheses and braces, honouring ``\\`` escapes."""
    out: list[str] = []
    cur = ""
    depth = 0
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "\\" and i + 1 < len(text):
            cur += text[i : i + 2]
            i += 2
            continue
        if ch in "({":
            depth += 1
        elif ch in ")}":
            depth -= 1
        if ch == sep and depth == 0:
            out.append(cur)
            cur = ""
        else:
            cur += ch
        i += 1
    out.append(cur)
    return out


def _labels_match(obj: Any, selector: str | None) -> bool:
    """Equality, inequality, existence and set-based (``in``/``notin``) terms."""
    if not selector:
        return True
    labels = _get(obj, "metadata", "labels") or {}
    for term in _split_top(selector):
        term = term.strip()
        if not term:
            continue
        m = re.fullmatch(r"(\S+)\s+(in|notin)\s+\((.*)\)", term)
        if m:
            key, op, vals = m.group(1), m.group(2), {v.strip() for v in m.group(3).split(",")}
            present = labels.get(key) in vals
            if (op == "in") != present:
                return False
            continue
        if "!=" in term:
            k, v = term.split("!=", 1)
            if labels.get(k.strip()) == v.strip():
                return False
        elif "=" in term:
            k, v = term.split("=", 1)
            if labels.get(k.strip().rstrip("=")) != v.strip().lstrip("="):
                return False
        elif term.startswith("!"):
            if term[1:] in labels:
                return False
        elif term not in labels:
            return False
    return True


def _serialize(obj: Any) -> Any:
    from kubernetes.client import ApiClient

    return ApiClient().sanitize_for_serialization(obj)


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in patch.items():
        if v is None:
            out.pop(k, None)
        elif isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _snake_to_model(kind_snake: str) -> str | None:
    import kubernetes.client as kc

    name = "V1" + "".join(p.capitalize() for p in kind_snake.split("_"))
    return name if hasattr(kc, name) else None


_PLURAL_TO_MODEL: dict[str, str] = {}


def _plural_to_model(plural: str) -> str | None:
    """``configmaps`` -> ``V1ConfigMap``, for the core kinds the fake knows."""
    if not _PLURAL_TO_MODEL:
        import kubernetes.client.models as km

        for name in dir(km):
            if not name.startswith("V1") or name.endswith("List") or name[2:3].islower():
                continue
            snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name[2:]).lower()
            p = canonical_kind(snake.replace("_", ""))
            if p in NAMESPACED_KINDS or p in CLUSTER_SCOPED_KINDS:
                _PLURAL_TO_MODEL.setdefault(p, name)
    return _PLURAL_TO_MODEL.get(plural)


def _deserialize(body: dict[str, Any], cls_name: str) -> Any:
    from kubernetes.client import ApiClient

    return ApiClient()._ApiClient__deserialize(copy.deepcopy(body), cls_name)  # type: ignore[attr-defined]


def _to_model(body: Any, cls_name: str | None) -> Any:
    """Turn a dict manifest into the client model the real API would return."""
    if not isinstance(body, dict) or not cls_name:
        return copy.deepcopy(body)
    try:
        return _deserialize(body, cls_name)
    except Exception:  # noqa: BLE001
        return copy.deepcopy(body)


def _set_meta(obj: Any, **values: Any) -> None:
    if isinstance(obj, dict):
        meta = obj.setdefault("metadata", {})
        camel = {
            "resource_version": "resourceVersion",
            "creation_timestamp": "creationTimestamp",
        }
        for k, v in values.items():
            meta[camel.get(k, k)] = v
        return
    from kubernetes.client.models import V1ObjectMeta

    if getattr(obj, "metadata", None) is None:
        obj.metadata = V1ObjectMeta()
    for k, v in values.items():
        setattr(obj.metadata, k, v)


def _parse_iso(s: str) -> float:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:  # noqa: BLE001
        return 0.0


def _jsonpath(doc: Any, expr: str) -> str:
    """The jsonpath subset Lakebench uses: ``{.a.b[0].c}`` and ``[*]``.

    Raises ValueError on anything else (``range``, filters, several groups),
    which the caller turns into an unscripted read.
    """
    m = re.fullmatch(r"\{([^{}]*)\}", expr.strip())
    if not m or any(t in m.group(1) for t in ("?(", "@", "range", "end")):
        raise ValueError(expr)
    path = m.group(1).strip()
    if path.startswith("."):
        path = path[1:]
    tokens = re.findall(r"[^.\[\]]+|\[\*\]|\[\d+\]", path)
    values: list[Any] = [doc]
    star = False
    for tok in tokens:
        nxt: list[Any] = []
        for v in values:
            if tok == "[*]":
                star = True
                if isinstance(v, list):
                    nxt.extend(v)
            elif tok.startswith("["):
                i = int(tok[1:-1])
                if isinstance(v, list) and i < len(v):
                    nxt.append(v[i])
            elif isinstance(v, dict) and tok in v:
                nxt.append(v[tok])
        values = nxt

    def fmt(v: Any) -> str:
        return json.dumps(v) if isinstance(v, (dict, list)) else str(v)

    if star:
        return " ".join(fmt(v) for v in values)
    return fmt(values[0]) if values else ""


def _parse_helm_value(raw: str) -> Any:
    raw = raw.strip()
    if raw.startswith("{") and raw.endswith("}"):
        return [_unescape(p).strip() for p in _split_top(raw[1:-1]) if p.strip()]
    return _unescape(raw)


def _unescape(s: str) -> str:
    return re.sub(r"\\(.)", r"\1", s)


def _apply_helm_sets(values: dict[str, Any], sets: list[str]) -> dict[str, Any]:
    out = copy.deepcopy(values)
    for s in sets:
        for assignment in _split_top(s):
            if "=" not in assignment:
                continue
            key, raw = assignment.split("=", 1)
            path = [_unescape(p) for p in re.split(r"(?<!\\)\.", key.strip())]
            cur = out
            for p in path[:-1]:
                if not isinstance(cur.get(p), dict):
                    cur[p] = {}
                cur = cur[p]
            cur[path[-1]] = _parse_helm_value(raw)
    return out


def _fake_load_kube_config(real: Callable[..., Any]) -> Callable[..., Any]:
    """A kubeconfig loader that sets a client configuration and nothing else.

    ``ClusterTarget.activate`` loads into a scratch configuration and refuses
    a load whose host differs from the context's cluster entry (the
    kubeconfig changed during the load), so a no-op stub reads as a rewrite.
    The library's own loader fills a given configuration, which keeps the
    host, CA and token what a real run would see; a credential plugin it
    starts goes through the patched ``subprocess`` and is recorded. With no
    configuration (the process default) nothing is loaded, as before.
    """

    def load(*args: Any, client_configuration: Any = None, **kwargs: Any) -> None:
        if client_configuration is None:
            return None
        kwargs.pop("persist_config", None)
        return real(
            *args, client_configuration=client_configuration, persist_config=False, **kwargs
        )

    return load


# ---------------------------------------------------------------------------
# kubectl / oc / helm argv parsing
# ---------------------------------------------------------------------------

_KUBE_VALUE_FLAGS = _words(
    "-n --namespace --context --kubeconfig --cluster --user --server -s --token "
    "--as --as-group --request-timeout -o --output -l --selector -f --filename "
    "-p --patch --type --timeout -c --container --for --field-selector --sort-by "
    "--since --tail -k --kustomize --replicas --template --from-literal "
    "--from-file --from-env-file --image --grace-period --cascade --field-manager "
    "-z --serviceaccount --rolebinding-name --resource --verb -v --v "
    "--api-group --dry-run-unused"
)

_HELM_VALUE_FLAGS = _words(
    "-n --namespace --kube-context --kubeconfig -f --values --set --set-string "
    "--set-file --set-json --set-literal --version --repo --timeout -o --output "
    "--revision --post-renderer --post-renderer-args --description --history-max "
    "--username --password --ca-file --cert-file --key-file --registry-config "
    "--repository-cache --repository-config --max --offset --filter -l --selector "
    "--kube-apiserver --kube-token --kube-as-user --kube-as-group --kube-ca-file "
    "--burst-limit --qps --cascade"
)

_KUBE_READ_VERBS = _words(
    "get describe logs log top explain version api-resources api-versions "
    "cluster-info wait whoami status diff events completion kustomize plugin "
    "options help port-forward proxy projects config"
)
# Verbs kubectl/oc knows that change something. Any other verb is unknown and
# fails closed (shared, and deleting if "delete" appears anywhere).
_KUBE_MUTATING_VERBS = _words(
    "apply create replace delete patch label annotate scale autoscale set edit "
    "exec attach cp debug drain cordon uncordon taint expose run rollout auth "
    "adm policy new-project new-app process certificate"
)

_HELM_DELETING_VERBS = _words("uninstall delete del un")
_HELM_READ_VERBS = _words(
    "get status list ls history hist show inspect template search version env "
    "lint verify repo dependency dep plugin pull fetch package completion "
    "registry help"
)
_SET_FLAGS = ("--set", "--set-string", "--set-literal")


@dataclass
class _Parsed:
    tool: str
    positionals: list[str]
    flags: dict[str, list[str]]
    bools: set[str]
    normalized: list[str]  # argv without context/kubeconfig flags

    def last(self, *names: str) -> str | None:
        """kubectl and helm keep the last value of a repeated flag (``-n`` and
        ``--namespace`` are one flag)."""
        order = [iv for n in names for iv in self.flags.get(f"@{n}", [])]
        return max(order)[1] if order else None

    def all(self, *names: str) -> list[str]:
        out: list[tuple[int, str]] = []
        for n in names:
            out.extend(self.flags.get(f"@{n}", []))
        return [v for _, v in sorted(out)]


_CONTEXT_FLAGS = ("--context", "--kube-context", "--kubeconfig")


def _parse_argv(argv: list[str], value_flags: frozenset[str]) -> _Parsed:
    tool = os.path.basename(argv[0]) if argv else ""
    positionals: list[str] = []
    flags: dict[str, Any] = {}
    bools: set[str] = set()
    normalized: list[str] = [tool]

    def put(k: str, v: str, i: int) -> None:
        flags.setdefault(k, []).append(v)
        flags.setdefault(f"@{k}", []).append((i, v))

    i = 1
    while i < len(argv):
        tok = argv[i]
        if tok == "--":
            normalized.extend(argv[i:])
            break
        if tok.startswith("-") and tok != "-":
            if "=" in tok and tok.startswith("--"):
                k, v = tok.split("=", 1)
                put(k, v, i)
                if k not in _CONTEXT_FLAGS:
                    normalized.append(tok)
                i += 1
                continue
            if tok in value_flags and i + 1 < len(argv):
                put(tok, argv[i + 1], i)
                if tok not in _CONTEXT_FLAGS:
                    normalized.extend([tok, argv[i + 1]])
                i += 2
                continue
            m = re.fullmatch(r"-([nfloc])(\S+)", tok)
            if m and f"-{m.group(1)}" in value_flags:
                put(f"-{m.group(1)}", m.group(2).lstrip("="), i)
                normalized.append(tok)
                i += 1
                continue
            bools.add(tok)
            normalized.append(tok)
            i += 1
            continue
        positionals.append(tok)
        normalized.append(tok)
        i += 1
    return _Parsed(tool, positionals, flags, bools, normalized)


def _dry_run(p: _Parsed) -> bool:
    """kubectl and helm treat ``none`` and ``false`` as a real run."""
    if "--dry-run" in p.bools:
        return True
    v = p.last("--dry-run")
    return v is not None and v.lower() in ("true", "client", "server")


@dataclass
class _Target:
    verb: str
    kind: str
    namespace: str | None
    name: str | None
    mutating: bool
    deleting: bool
    force_shared: bool = False
    note: str = ""


def _manifest_docs(p: _Parsed, kwargs: dict[str, Any]) -> list[dict[str, Any]] | None:
    """The objects every ``-f`` names, or None if any cannot be read."""
    sources = p.all("-f", "--filename")
    if not sources:
        return None
    out: list[dict[str, Any]] = []
    import yaml

    for src in sources:
        text: str | None = None
        if src == "-":
            raw = kwargs.get("input")
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", "replace")
            text = raw if isinstance(raw, str) else None
        elif os.path.isfile(src):
            try:
                with open(src, encoding="utf-8") as fh:
                    text = fh.read()
            except OSError:
                text = None
        if text is None:
            return None
        try:
            docs = [d for d in yaml.safe_load_all(text) if isinstance(d, dict)]
        except yaml.YAMLError:
            return None
        for d in docs:
            if d.get("kind") == "List" and isinstance(d.get("items"), list):
                out.extend(i for i in d["items"] if isinstance(i, dict))
            else:
                out.append(d)
    return out


def _kind_name_targets(rest: list[str]) -> list[tuple[str, str | None]]:
    """Every (kind, name) a kubectl resource argument list names.

    ``kind/name`` tokens each name one target. Otherwise the first token is a
    kind or a comma list of kinds and every later token that is not a
    ``key=value`` or ``key-`` label edit is a name.
    """
    rest = [r for r in rest if r]
    if not rest:
        return [("", None)]
    if any("/" in r for r in rest):
        out = []
        for r in rest:
            if "/" in r:
                kind, name = r.split("/", 1)
                out.append((canonical_kind(kind), name))
        return out
    kinds = [canonical_kind(k) for k in rest[0].split(",") if k]
    names = [r for r in rest[1:] if "=" not in r and not r.endswith("-")]
    if not names:
        return [(k, None) for k in kinds]
    return [(k, n) for k in kinds for n in names]


def _kube_targets(p: _Parsed, kwargs: dict[str, Any]) -> list[_Target]:
    pos = p.positionals
    if not pos:
        return [_Target("", "", None, None, mutating=False, deleting=False)]
    verb = pos[0]
    rest = pos[1:]
    ns_flag = p.last("-n", "--namespace")
    all_ns = "-A" in p.bools or "--all-namespaces" in p.bools
    dry = _dry_run(p)

    if verb not in _KUBE_READ_VERBS and verb not in _KUBE_MUTATING_VERBS:
        # An unknown verb, or a value flag the parser does not know shifting
        # the verb: fail closed.
        deleting = "delete" in pos
        return [_Target(verb, "", ns_flag, None, True, deleting, True)]

    if p.tool == "oc" and verb in ("adm", "policy"):
        words = rest[:2] if verb == "adm" else ["policy", *rest[:1]]
        sub = " ".join(words)
        mutating = not dry and rest[:1] != ["top"]
        deleting = mutating and any(w.startswith(("remove-", "prune")) for w in words)
        kind = "securitycontextconstraints" if "scc" in sub else "clusterpolicy"
        tail = rest[2:] if verb == "adm" else rest[1:]
        name = tail[0] if tail else None
        return [_Target(f"{verb} {sub}".strip(), kind, ns_flag, name, mutating, deleting, True)]
    if p.tool == "oc" and verb == "project":
        return [_Target(verb, "projects", None, rest[0] if rest else None, False, False)]
    if p.tool == "oc" and verb == "new-project":
        return [_Target(verb, "namespaces", None, rest[0] if rest else None, not dry, False)]

    sub = ""
    if verb in ("rollout", "auth", "set", "certificate", "config") and rest:
        sub, rest = rest[0], rest[1:]
    full_verb = f"{verb} {sub}".strip()

    if verb in _KUBE_READ_VERBS:
        mutating = False
    elif verb == "rollout":
        mutating = sub not in ("status", "history")
    elif verb == "auth":
        mutating = sub not in ("can-i", "whoami")
    elif verb == "apply" and rest[:1] == ["view-last-applied"]:
        mutating = False
    else:
        mutating = True
    if dry:
        mutating = False
    deleting = mutating and (
        verb in ("delete", "drain")
        or (verb == "replace" and "--force" in p.bools)
        or (verb == "apply" and "--prune" in p.bools)
    )

    def resolve_ns(kind: str, doc_ns: str | None = None) -> str | None:
        if kind in CLUSTER_SCOPED_KINDS:
            return None
        if all_ns:
            return "*"
        return ns_flag or doc_ns

    if verb in ("apply", "create", "replace", "delete") and p.all("-f", "--filename"):
        docs = _manifest_docs(p, kwargs)
        if not docs:
            return [_Target(full_verb, "", ns_flag, None, mutating, deleting, True)]
        out = []
        for d in docs:
            kind = canonical_kind(str(d.get("kind", "")))
            meta = d.get("metadata") or {}
            out.append(
                _Target(
                    full_verb,
                    kind,
                    resolve_ns(kind, meta.get("namespace")),
                    meta.get("name"),
                    mutating,
                    deleting,
                )
            )
        return out

    if verb == "create" and rest:
        kind = canonical_kind(rest[0])
        if kind == "secrets":
            name = rest[2] if len(rest) > 2 else None
        else:
            name = rest[1] if len(rest) > 1 else None
        return [_Target(full_verb, kind, resolve_ns(kind), name, mutating, deleting)]

    if verb in ("exec", "attach", "cp", "debug"):
        name = rest[0] if rest else None
        ns = ns_flag
        if verb == "cp":
            spec = next((r for r in rest if ":" in r), "")
            pod = spec.split(":", 1)[0]
            if "/" in pod:
                ns, pod = pod.split("/", 1)
            name = pod or None
        kind = "pods"
        if name and "/" in name:
            k, name = name.split("/", 1)
            kind = canonical_kind(k)
        return [_Target(full_verb, kind, ns, name, mutating, deleting)]

    return [
        _Target(full_verb, kind, resolve_ns(kind), name, mutating, deleting)
        for kind, name in _kind_name_targets(rest)
    ]


def _helm_target(p: _Parsed) -> _Target:
    pos = p.positionals
    verb = pos[0] if pos else ""
    # `helm get values <name>`: the release is the third word.
    name_at = 2 if verb == "get" else 1
    name = pos[name_at] if len(pos) > name_at else None
    ns = p.last("-n", "--namespace") or "default"
    # install, upgrade, uninstall, rollback, test and any unknown verb mutate.
    mutating = verb not in _HELM_READ_VERBS and not _dry_run(p)
    deleting = mutating and verb in _HELM_DELETING_VERBS
    return _Target(verb, "releases", ns, name, mutating, deleting, True)


def _unwrap(argv: list[str]) -> list[list[str]]:
    """The commands a wrapper (``env``, ``timeout``, ``sh -c``) really runs."""
    if not argv:
        return [argv]
    tool = os.path.basename(argv[0])
    if tool in SHELLS:
        if "-c" not in argv[1:]:
            return [argv]
        idx = argv.index("-c", 1)
        script = argv[idx + 1] if idx + 1 < len(argv) else ""
        if any(ch in script for ch in "$`(){}<>"):
            return [argv]  # substitutions, subshells, redirects: fail closed
        cmds: list[list[str]] = []
        for line in script.splitlines():
            lex = shlex.shlex(line, posix=True, punctuation_chars=";&|")
            lex.whitespace_split = True
            try:
                tokens = list(lex)
            except ValueError:
                return [argv]
            cur: list[str] = []
            for t in tokens:
                if t and set(t) <= set(";&|"):
                    if cur:
                        cmds.append(cur)
                    cur = []
                else:
                    cur.append(t)
            if cur:
                cmds.append(cur)
        out: list[list[str]] = []
        for c in cmds:
            out.extend(_unwrap(c))
        return out or [argv]
    if tool in WRAPPERS:
        rest = argv[1:]
        while rest and (rest[0].startswith("-") or (tool == "env" and "=" in rest[0])):
            flag = rest.pop(0)
            if tool == "timeout" and flag in ("-s", "-k", "--signal", "--kill-after") and rest:
                rest.pop(0)
            if tool == "env" and flag == "-u" and rest:
                rest.pop(0)
        if tool == "timeout" and rest:
            rest = rest[1:]  # the duration
        return _unwrap(rest) if rest else [argv]
    return [argv]


def _tool_name(argv: list[str]) -> str:
    base = os.path.basename(argv[0]) if argv else ""
    m = re.fullmatch(r"(kubectl|helm|oc)([-_.].*)?", base)
    return m.group(1) if m else base


def _is_child_lakebench(argv: list[str]) -> bool:
    base = os.path.basename(argv[0]) if argv else ""
    if base == "lakebench":
        return True
    if re.fullmatch(r"python[0-9.]*", base):
        return any(
            a == "-m" and b.startswith("lakebench") for a, b in zip(argv, argv[1:], strict=False)
        )
    return False


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakePopen:
    """Stand-in for ``subprocess.Popen``: never starts a process."""

    def __class_getitem__(cls, item: Any) -> type:
        return cls

    def __init__(self, recorder: K8sRecorder, args: Any, kwargs: dict[str, Any]) -> None:
        self._recorder = recorder
        self._first = len(recorder.calls)
        # A scripted TimeoutExpired means "did not finish in time": the
        # process starts, and communicate(timeout=...) raises it once.
        self._pending: BaseException | None = None
        try:
            result = recorder._run_command(args, kwargs, popen=True)
        except subprocess.TimeoutExpired as e:
            self._pending = e
            result = subprocess.CompletedProcess(args, -15, "", "")
        self._last = len(recorder.calls)
        self.signals: list[int] = []
        self.args = args
        self.pid = 0
        self.returncode: int | None = result.returncode
        text = bool(
            kwargs.get("text") or kwargs.get("universal_newlines") or kwargs.get("encoding")
        )
        out = result.stdout or ("" if text else b"")
        err = result.stderr or ("" if text else b"")
        self.stdout: Any = io.StringIO(out) if text else io.BytesIO(out)
        self.stderr: Any = io.StringIO(err) if text else io.BytesIO(err)
        self.stdin: Any = (
            (io.StringIO() if text else io.BytesIO())
            if kwargs.get("stdin") == subprocess.PIPE
            else None
        )
        self._out, self._err = out, err

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        return int(self.returncode or 0)

    def communicate(self, input: Any = None, timeout: float | None = None) -> tuple[Any, Any]:
        """Records the timeout on this process's calls; raises a scripted timeout once."""
        if timeout is not None:
            with self._recorder._lock:
                for i in range(self._first, self._last):
                    c = self._recorder.calls[i]
                    self._recorder.calls[i] = dataclasses.replace(
                        c, kwargs={**c.kwargs, "timeout": timeout}
                    )
        if self._pending is not None:
            pending, self._pending = self._pending, None
            raise pending
        return self._out, self._err

    def terminate(self) -> None:
        self.signals.append(15)

    def kill(self) -> None:
        self.signals.append(9)

    def send_signal(self, sig: int) -> None:
        self.signals.append(int(sig))

    def __enter__(self) -> _FakePopen:
        return self

    def __exit__(self, *exc: Any) -> None:
        pass


class _FakePaginator:
    def __init__(self, fn: Callable[..., dict[str, Any]]) -> None:
        self._fn = fn

    def paginate(self, **kwargs: Any) -> Iterator[dict[str, Any]]:
        kwargs.pop("PaginationConfig", None)
        yield self._fn(**kwargs)


class _FakeS3:
    """In-memory S3 client bound to the recorder's bucket store."""

    _MUTATING_PREFIXES = (
        "put_",
        "delete_",
        "create_",
        "abort_",
        "copy_",
        "upload_",
        "complete_",
        "restore_",
    )

    def __init__(self, recorder: K8sRecorder) -> None:
        self._r = recorder

    # -- plumbing ----------------------------------------------------------

    def _record(self, method: str, bucket: str | None, key: str | None = None) -> None:
        mutating = method.startswith(self._MUTATING_PREFIXES)
        deleting = mutating and method.startswith(("delete_", "abort_"))
        kind = "objects" if key is not None else "buckets"
        name = f"{bucket}/{key}" if key is not None else bucket
        self._r._record_s3(method, kind, name, bucket, mutating, deleting)
        for m, code, status, left in self._r._s3_errors:
            if m == method and (left[0] != 0):
                left[0] -= 1
                raise self._error(code, status, method)
        if method.endswith("bucket_tagging") and not self._r.s3_tagging:
            raise self._error("NotImplemented", 501, method)

    def _error(self, code: str, status: int, op: str) -> Exception:
        from botocore.exceptions import ClientError

        return ClientError(
            {
                "Error": {"Code": code, "Message": code},
                "ResponseMetadata": {"HTTPStatusCode": status},
            },
            op,
        )

    def _bucket(self, name: str, op: str) -> dict[str, bytes]:
        if name not in self._r.buckets_store:
            raise self._error("NoSuchBucket", 404, op)
        return self._r.buckets_store[name]

    # -- buckets -----------------------------------------------------------

    def list_buckets(self, **_kw: Any) -> dict[str, Any]:
        self._record("list_buckets", None)
        return {"Buckets": [{"Name": b} for b in sorted(self._r.buckets_store)]}

    def head_bucket(self, Bucket: str, **_kw: Any) -> dict[str, Any]:  # noqa: N803
        self._record("head_bucket", Bucket)
        if Bucket not in self._r.buckets_store:
            raise self._error("404", 404, "HeadBucket")
        return {}

    def create_bucket(self, Bucket: str, **_kw: Any) -> dict[str, Any]:  # noqa: N803
        self._record("create_bucket", Bucket)
        if Bucket in self._r.buckets_store:
            raise self._error("BucketAlreadyOwnedByYou", 409, "CreateBucket")
        self._r.buckets_store[Bucket] = {}
        self._r.created_buckets.add(Bucket)
        return {"Location": f"/{Bucket}"}

    def delete_bucket(self, Bucket: str, **_kw: Any) -> dict[str, Any]:  # noqa: N803
        self._record("delete_bucket", Bucket)
        objects = self._bucket(Bucket, "DeleteBucket")
        if objects or self._r.uploads_store.get(Bucket):
            raise self._error("BucketNotEmpty", 409, "DeleteBucket")
        del self._r.buckets_store[Bucket]
        self._r.tags_store.pop(Bucket, None)
        return {}

    def get_bucket_tagging(self, Bucket: str, **_kw: Any) -> dict[str, Any]:  # noqa: N803
        self._record("get_bucket_tagging", Bucket)
        self._bucket(Bucket, "GetBucketTagging")
        tags = self._r.tags_store.get(Bucket)
        if tags is None:
            raise self._error("NoSuchTagSet", 404, "GetBucketTagging")
        return {"TagSet": [{"Key": k, "Value": v} for k, v in tags.items()]}

    def put_bucket_tagging(self, Bucket: str, Tagging: dict[str, Any], **_kw: Any) -> dict:  # noqa: N803
        self._record("put_bucket_tagging", Bucket)
        self._bucket(Bucket, "PutBucketTagging")
        self._r.tags_store[Bucket] = {t["Key"]: t["Value"] for t in Tagging.get("TagSet", [])}
        return {}

    # -- objects -----------------------------------------------------------

    def list_objects_v2(self, Bucket: str, Prefix: str = "", **_kw: Any) -> dict[str, Any]:  # noqa: N803
        self._record("list_objects_v2", Bucket)
        objects = self._bucket(Bucket, "ListObjectsV2")
        contents = [
            {"Key": k, "Size": len(v)} for k, v in sorted(objects.items()) if k.startswith(Prefix)
        ]
        return {"Contents": contents, "KeyCount": len(contents), "IsTruncated": False}

    def list_multipart_uploads(self, Bucket: str, Prefix: str = "", **_kw: Any) -> dict:  # noqa: N803
        self._record("list_multipart_uploads", Bucket)
        self._bucket(Bucket, "ListMultipartUploads")
        ups = [
            {"Key": k, "UploadId": u}
            for (k, u) in sorted(self._r.uploads_store.get(Bucket, set()))
            if k.startswith(Prefix)
        ]
        return {"Uploads": ups, "IsTruncated": False}

    def get_paginator(self, operation: str) -> _FakePaginator:
        fn = getattr(type(self), operation, None)
        if fn is None:
            raise NotImplementedError(f"recording_k8s: no fake paginator for {operation}")
        return _FakePaginator(getattr(self, operation))

    def put_object(self, Bucket: str, Key: str, Body: Any = b"", **kw: Any) -> dict:  # noqa: N803
        self._record("put_object", Bucket, Key)
        objects = self._bucket(Bucket, "PutObject")
        if kw.get("IfNoneMatch") == "*" and Key in objects:
            raise self._error("PreconditionFailed", 412, "PutObject")
        data = Body.read() if hasattr(Body, "read") else Body
        objects[Key] = data.encode() if isinstance(data, str) else bytes(data or b"")
        return {"ETag": f'"{abs(hash(objects[Key]))}"'}

    def upload_file(self, Filename: str, Bucket: str, Key: str, **_kw: Any) -> None:  # noqa: N803
        self._record("upload_file", Bucket, Key)
        with open(Filename, "rb") as fh:
            self._bucket(Bucket, "PutObject")[Key] = fh.read()

    def upload_fileobj(self, Fileobj: Any, Bucket: str, Key: str, **_kw: Any) -> None:  # noqa: N803
        self._record("upload_fileobj", Bucket, Key)
        self._bucket(Bucket, "PutObject")[Key] = Fileobj.read()

    def get_object(self, Bucket: str, Key: str, **_kw: Any) -> dict[str, Any]:  # noqa: N803
        self._record("get_object", Bucket, Key)
        objects = self._bucket(Bucket, "GetObject")
        if Key not in objects:
            raise self._error("NoSuchKey", 404, "GetObject")
        return {"Body": io.BytesIO(objects[Key]), "ContentLength": len(objects[Key])}

    def head_object(self, Bucket: str, Key: str, **_kw: Any) -> dict[str, Any]:  # noqa: N803
        self._record("head_object", Bucket, Key)
        objects = self._bucket(Bucket, "HeadObject")
        if Key not in objects:
            raise self._error("404", 404, "HeadObject")
        return {"ContentLength": len(objects[Key])}

    def delete_object(self, Bucket: str, Key: str, **_kw: Any) -> dict[str, Any]:  # noqa: N803
        self._record("delete_object", Bucket, Key)
        self._bucket(Bucket, "DeleteObject").pop(Key, None)
        return {}

    def delete_objects(self, Bucket: str, Delete: dict[str, Any], **_kw: Any) -> dict:  # noqa: N803
        keys = [o["Key"] for o in Delete.get("Objects", [])]
        for k in keys or [None]:
            self._record("delete_objects", Bucket, k)
        objects = self._bucket(Bucket, "DeleteObjects")
        for k in keys:
            objects.pop(k, None)
        return {"Deleted": [{"Key": k} for k in keys], "Errors": []}

    def create_multipart_upload(self, Bucket: str, Key: str, **_kw: Any) -> dict:  # noqa: N803
        self._record("create_multipart_upload", Bucket, Key)
        self._bucket(Bucket, "CreateMultipartUpload")
        upload_id = f"upload-{next(self._r._counter)}"
        self._r.uploads_store.setdefault(Bucket, set()).add((Key, upload_id))
        return {"UploadId": upload_id}

    def abort_multipart_upload(self, Bucket: str, Key: str, UploadId: str, **_kw: Any) -> dict:  # noqa: N803
        self._record("abort_multipart_upload", Bucket, Key)
        ups = self._r.uploads_store.get(Bucket, set())
        if (Key, UploadId) not in ups:
            raise self._error("NoSuchUpload", 404, "AbortMultipartUpload")
        ups.discard((Key, UploadId))
        return {}

    def __getattr__(self, attr: str) -> Callable[..., Any]:
        if attr.startswith("_"):
            raise AttributeError(attr)

        def method(*a: Any, **kw: Any) -> dict[str, Any]:
            bucket = kw.get("Bucket", next((x for x in a if isinstance(x, str)), None))
            self._record(attr, bucket, kw.get("Key"))
            return {}

        return method


def _make_fake_api(recorder: K8sRecorder, api_name: str) -> type:
    class _FakeApi:
        def __init__(self, api_client: Any = None, *_a: Any, **_kw: Any) -> None:
            self.api_client = api_client

        def __getattr__(self, attr: str) -> Callable[..., Any]:
            if attr.startswith("_") or not _API_METHOD_RE.match(attr):
                raise AttributeError(f"{api_name} has no attribute {attr!r}")

            def method(*args: Any, **kwargs: Any) -> Any:
                return recorder._api_call(api_name, attr, args, kwargs)

            method.__name__ = attr
            return method

    _FakeApi.__name__ = api_name
    _FakeApi.__qualname__ = f"recording_k8s.{api_name}"
    return _FakeApi


# ---------------------------------------------------------------------------
# The recorder
# ---------------------------------------------------------------------------


class K8sRecorder:
    """Records and classifies every cluster and S3 call made while installed."""

    def __init__(
        self,
        namespace: str | None = None,
        *,
        buckets: Iterable[str] = (),
        allow_delete: Iterable[str] = (),
        deployment: str | None = None,
    ) -> None:
        self.namespace = namespace
        self.deployment = deployment
        self.buckets: set[str] = set(buckets)
        self.allow_delete: list[str] = list(allow_delete)
        self.calls: list[Call] = []
        # (kind, namespace or None, name) -> object
        self.store: dict[tuple[str, str | None, str], Any] = {}
        self.releases: dict[tuple[str, str], Release] = {}
        self.buckets_store: dict[str, dict[str, bytes]] = {}
        self.uploads_store: dict[str, set[tuple[str, str]]] = {}
        self.tags_store: dict[str, dict[str, str]] = {}
        self.created_buckets: set[str] = set()
        self.pod_logs: dict[tuple[str, str], str] = {}
        self.processes: list[Any] = []  # every fake Popen, with the signals it got
        self.exec_output = ""
        self.can_i = True
        self.s3_tagging = True  # False: the backend answers NotImplemented (FlashBlade)
        self.teardown_check = True
        self._s3_errors: list[tuple[str, str, int, list[int]]] = []
        self._responders: list[_Responder] = []
        self._errors: list[_InjectedError] = []
        self._lease: _Lease | None = None
        self._counter = itertools.count(1)
        self._lock = threading.RLock()
        self._installed = False
        self._no_calls_expected = False
        self._fakes: dict[str, Any] = {}
        self._op_pods: dict[str, bool] = {}

    # -- configuration -----------------------------------------------------

    def configure(
        self,
        *,
        namespace: str | None = None,
        buckets: Iterable[str] | None = None,
        allow_delete: Iterable[str] | None = None,
        deployment: str | None = None,
    ) -> K8sRecorder:
        if namespace is not None:
            self.namespace = namespace
        if buckets is not None:
            self.buckets = set(buckets)
        if allow_delete is not None:
            self.allow_delete = list(allow_delete)
        if deployment is not None:
            self.deployment = deployment
        return self

    def for_config(self, cfg: Any) -> K8sRecorder:
        """Scope to a ``LakebenchConfig``: namespace, deployment name, buckets."""
        self.namespace = cfg.get_namespace()
        self.deployment = cfg.name
        buckets = cfg.platform.storage.s3.buckets
        dumped = buckets.model_dump() if hasattr(buckets, "model_dump") else vars(buckets)
        self.buckets = {v for v in dumped.values() if isinstance(v, str) and v}
        return self

    @property
    def lease_held(self) -> bool:
        """Whether the calling thread holds the lease."""
        lease = self._lease
        return lease is not None and lease.thread == threading.get_ident()

    # -- seeding (never recorded) ------------------------------------------

    def _next_rv(self) -> str:
        return str(next(self._counter))

    def add(self, kind: str, obj: Any, namespace: str | None = None) -> Any:
        """Seed one object. ``kind`` is any kubectl spelling.

        A dict seed of a core kind is converted to its client model, as the
        real API returns; one that does not validate raises, so a seed can
        never silently differ from what Lakebench reads by attribute.
        """
        k = canonical_kind(kind)
        name = _get(obj, "metadata", "name")
        if not name:
            raise ValueError("seeded object needs metadata.name")
        cluster = k in CLUSTER_SCOPED_KINDS or self._crd_for(k) == "Cluster"
        ns = None if cluster else (namespace or _get(obj, "metadata", "namespace"))
        if not cluster and k not in NAMESPACED_KINDS and ns is None:
            raise ValueError(f"seed of {k}/{name}: a namespaced or unknown kind needs a namespace")
        model = _plural_to_model(k)
        if isinstance(obj, dict) and model:
            try:
                stored = _deserialize(obj, model)
            except Exception as e:  # noqa: BLE001
                raise ValueError(
                    f"seed of {k}/{name} does not validate as {model}: {e}; "
                    "seed a complete object (see add_crd)"
                ) from e
        else:
            stored = copy.deepcopy(obj)
        meta: dict[str, Any] = {"resource_version": self._next_rv()}
        if not _get(stored, "metadata", "uid"):
            meta["uid"] = f"uid-{k}-{name}-{self._next_rv()}"
        if ns:
            meta["namespace"] = ns
        _set_meta(stored, **meta)
        with self._lock:
            self.store[(k, ns, name)] = stored
        return stored

    def add_namespace(
        self,
        name: str,
        *,
        annotations: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
        phase: str = "Active",
    ) -> Any:
        from kubernetes.client.models import V1Namespace, V1NamespaceStatus, V1ObjectMeta

        ns = V1Namespace(
            metadata=V1ObjectMeta(
                name=name,
                annotations=dict(annotations or {}) or None,
                labels=dict(labels or {}) or None,
            ),
            status=V1NamespaceStatus(phase=phase),
        )
        return self.add("namespaces", ns)

    def add_crd(self, plural: str, group: str, kind: str, *, scope: str = "Namespaced") -> Any:
        """Seed a CRD that ``K8sClient`` and the platform probes can read."""
        from kubernetes.client.models import (
            V1CustomResourceDefinition,
            V1CustomResourceDefinitionNames,
            V1CustomResourceDefinitionSpec,
            V1CustomResourceDefinitionVersion,
            V1ObjectMeta,
        )

        if scope not in ("Namespaced", "Cluster"):
            raise ValueError(scope)
        crd = V1CustomResourceDefinition(
            metadata=V1ObjectMeta(name=f"{plural}.{group}"),
            spec=V1CustomResourceDefinitionSpec(
                group=group,
                names=V1CustomResourceDefinitionNames(kind=kind, plural=plural),
                scope=scope,
                versions=[V1CustomResourceDefinitionVersion(name="v1", served=True, storage=True)],
            ),
        )
        return self.add("customresourcedefinitions", crd)

    def _crd_for(self, plural: str) -> str | None:
        """The seeded scope of a custom kind, or None when no CRD is seeded."""
        for (k, _ns, _name), obj in self.store.items():
            if k == "customresourcedefinitions" and _get(obj, "spec", "names", "plural") == plural:
                return str(_get(obj, "spec", "scope"))
        return None

    def add_helm_release(
        self,
        name: str,
        namespace: str,
        *,
        chart: str | None = None,
        version: str = "1.0.0",
        values: dict[str, Any] | None = None,
    ) -> Release:
        rel = Release(name, namespace, chart or name, version, dict(values or {}))
        self.releases[(namespace, name)] = rel
        return rel

    def add_spark_operator(
        self,
        watched: Iterable[str] = ("default",),
        *,
        namespace: str = "spark-operator",
        version: str = "2.5.1",
        pods: bool = True,
    ) -> None:
        """Seed the operator: CRDs, release, both Deployments and their pods.

        Every container carries ``--namespaces=<watched>`` and the release's
        ``spark.jobNamespaces`` matches, which is what ``SparkOperatorManager``
        and destroy's pod check read. A ``helm upgrade --set
        spark.jobNamespaces=...`` rewrites both.
        """
        watched = list(watched)
        if ("namespaces", None, namespace) not in self.store:
            self.add_namespace(namespace)
        self.add_crd("sparkapplications", "sparkoperator.k8s.io", "SparkApplication")
        self.add_crd(
            "scheduledsparkapplications", "sparkoperator.k8s.io", "ScheduledSparkApplication"
        )
        self.add_helm_release(
            "spark-operator",
            namespace,
            chart="spark-operator",
            version=version,
            values={"spark": {"jobNamespaces": watched}},
        )
        self._op_pods[namespace] = pods
        self._sync_operator(namespace, watched)

    def _sync_operator(self, namespace: str, watched: list[str]) -> None:
        from kubernetes.client.models import (
            V1Container,
            V1Deployment,
            V1DeploymentSpec,
            V1DeploymentStatus,
            V1LabelSelector,
            V1ObjectMeta,
            V1Pod,
            V1PodSpec,
            V1PodStatus,
            V1PodTemplateSpec,
        )

        arg = "--namespaces=" + ",".join(watched)
        for component in ("controller", "webhook"):
            name = f"spark-operator-{component}"
            labels = {
                "app.kubernetes.io/name": "spark-operator",
                "app.kubernetes.io/instance": "spark-operator",
                "app.kubernetes.io/component": component,
            }
            container = V1Container(name=component, args=[component, "start", arg])
            self.store.pop(("deployments", namespace, name), None)
            self.add(
                "deployments",
                V1Deployment(
                    metadata=V1ObjectMeta(name=name, labels=labels),
                    spec=V1DeploymentSpec(
                        replicas=1,
                        selector=V1LabelSelector(match_labels=labels),
                        template=V1PodTemplateSpec(
                            metadata=V1ObjectMeta(labels=labels),
                            spec=V1PodSpec(containers=[container]),
                        ),
                    ),
                    status=V1DeploymentStatus(replicas=1, ready_replicas=1, available_replicas=1),
                ),
                namespace=namespace,
            )
            if self._op_pods.get(namespace, True):
                self.store.pop(("pods", namespace, f"{name}-0"), None)
                self.add(
                    "pods",
                    V1Pod(
                        metadata=V1ObjectMeta(name=f"{name}-0", labels=labels),
                        spec=V1PodSpec(containers=[copy.deepcopy(container)]),
                        status=V1PodStatus(phase="Running"),
                    ),
                    namespace=namespace,
                )

    def add_stackable(self) -> None:
        """Seed the Stackable CRDs Hive needs (SecretClass is cluster-scoped)."""
        self.add_crd("hiveclusters", "hive.stackable.tech", "HiveCluster")
        self.add_crd("secretclasses", "secrets.stackable.tech", "SecretClass", scope="Cluster")
        for op in ("commons-operator", "listener-operator", "secret-operator", "hive-operator"):
            self.add_helm_release(op, "stackable", chart=op, version="25.7.0")

    def add_openshift(self, version: str = "4.16.0") -> None:
        """Seed what Lakebench's OpenShift detection reads (CRDs, ClusterVersion)."""
        self.add_crd(
            "securitycontextconstraints",
            "security.openshift.io",
            "SecurityContextConstraints",
            scope="Cluster",
        )
        self.add_crd("routes", "route.openshift.io", "Route")
        self.add_crd("projects", "project.openshift.io", "Project", scope="Cluster")
        self.add_crd("clusterversions", "config.openshift.io", "ClusterVersion", scope="Cluster")
        self.add(
            "clusterversions",
            {"metadata": {"name": "version"}, "status": {"history": [{"version": version}]}},
        )

    def add_bucket(
        self,
        name: str,
        objects: dict[str, bytes] | Iterable[str] = (),
        *,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Seed a bucket. Tag it with ``lakebench.deployment`` to name its owner."""
        if isinstance(objects, dict):
            data = {k: bytes(v) for k, v in objects.items()}
        else:
            data = dict.fromkeys(objects, b"x")
        self.buckets_store[name] = data
        if tags is not None:
            self.tags_store[name] = dict(tags)

    def seed_lease(
        self,
        holder: str = "other-host@other-user@000000000000",
        *,
        acquired_at: str | None = None,
        ttl_seconds: int = 3600,
    ) -> None:
        """Seed a lease held by another process. It never counts as ours."""
        from kubernetes.client.models import V1ConfigMap, V1ObjectMeta

        if ("namespaces", None, LOCK_NAMESPACE) not in self.store:
            self.add_namespace(LOCK_NAMESPACE)
        at = acquired_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
        with self._lock:
            self.store.pop(("configmaps", LOCK_NAMESPACE, LOCK_CONFIGMAP_NAME), None)
            self.add(
                "configmaps",
                V1ConfigMap(
                    metadata=V1ObjectMeta(name=LOCK_CONFIGMAP_NAME),
                    data={"holder": holder, "acquired-at": at, "ttl-seconds": str(ttl_seconds)},
                ),
                namespace=LOCK_NAMESPACE,
            )
            self._lease = None

    # -- scripting ---------------------------------------------------------

    def on_command(
        self,
        *prefix: str,
        returncode: int = 0,
        stdout: str = "",
        stderr: str = "",
        handler: Callable[[list[str], dict[str, Any]], Any] | None = None,
        raises: BaseException | None = None,
    ) -> None:
        """Script a CLI answer for argv starting with ``prefix``.

        Matching ignores ``--context``/``--kube-context``/``--kubeconfig``.
        The newest matching responder wins over earlier ones and over the
        built-in answers. ``handler(argv, kwargs)`` may return a
        ``CompletedProcess``, a ``(returncode, stdout, stderr)`` tuple, or
        raise.
        """
        self._responders.append(
            _Responder(tuple(prefix), returncode, stdout, stderr, handler, raises)
        )

    def fail_s3(self, method: str, code: str, *, status: int = 500, times: int = -1) -> None:
        """Make an S3 method raise ``ClientError(code)`` after recording.

        ``times=-1`` means every call.
        """
        self._s3_errors.append((method, code, status, [times]))

    def fail(
        self,
        verb: str | None = None,
        kind: str | None = None,
        name: str | None = None,
        *,
        namespace: str | None = None,
        status: int = 500,
        times: int | None = None,
    ) -> None:
        """Make matching API calls raise ``ApiException(status)`` (after recording)."""
        self._errors.append(
            _InjectedError(
                verb, canonical_kind(kind) if kind else None, name, namespace, status, times
            )
        )

    # -- classification ----------------------------------------------------

    def _scope(
        self,
        kind: str,
        namespace: str | None,
        name: str | None,
        verb: str,
        *,
        custom: bool = False,
        cluster_call: bool = False,
    ) -> str:
        if kind == "configmaps" and namespace == LOCK_NAMESPACE and name == LOCK_CONFIGMAP_NAME:
            return LEASE
        if kind == "namespaces" and name == LOCK_NAMESPACE and verb == "create":
            return LEASE
        own_ns = self.namespace
        if not own_ns:
            return SHARED
        if kind in ("namespaces", "projects") and name == own_ns:
            return OWN
        if kind == "secretclasses" and name in own_secretclass_names(own_ns):
            return OWN
        if cluster_call or namespace != own_ns or kind in CLUSTER_SCOPED_KINDS:
            return SHARED
        if kind in NAMESPACED_KINDS:
            return OWN
        if custom and self._crd_for(kind) == "Namespaced":
            return OWN
        return SHARED

    def _append(self, call: Call) -> int:
        with self._lock:
            self.calls.append(call)
            return len(self.calls) - 1

    def _annotate(self, idx: int, note: str) -> None:
        with self._lock:
            self.calls[idx] = dataclasses.replace(self.calls[idx], note=note)

    def _namespace_bucket_annotation(self, key: str) -> set[str]:
        ns = self.store.get(("namespaces", None, self.namespace or ""))
        raw = (_get(ns, "metadata", "annotations") or {}).get(key, "") if ns is not None else ""
        return {b.strip() for b in str(raw).split(",") if b.strip()}

    def _bucket_scope(self, method: str, bucket: str | None) -> tuple[str, str]:
        """Own only what ownership.py would let this deployment touch.

        A config bucket another deployment tagged is foreign. Tagging a config
        bucket that nobody else tagged is how deploy adopts it, and creating
        a config bucket is deploy's job. Any other
        write needs proof the bucket is ours: our tag, a create in this test,
        or the namespace's created-buckets or adopted-empty-buckets
        annotation. Deleting the bucket itself needs proof we created it.
        """
        from lakebench.deploy.ownership import (
            ANNOTATION_ADOPTED_EMPTY_BUCKETS,
            ANNOTATION_CREATED_BUCKETS,
        )

        if bucket is None or bucket not in self.buckets:
            return SHARED, ""
        tags = self.tags_store.get(bucket) or {}
        owner = tags.get(TAG_DEPLOYMENT)
        mine = self.deployment or self.namespace
        if owner is not None and owner != mine:
            return SHARED, ""
        if method in ("put_bucket_tagging", "create_bucket"):
            return OWN, ""
        created = (
            bucket in self.created_buckets
            or (owner == mine and tags.get(TAG_CREATED) == "true")
            or bucket in self._namespace_bucket_annotation(ANNOTATION_CREATED_BUCKETS)
        )
        if method == "delete_bucket":
            return (OWN, "") if created else (SHARED, "not-created")
        adopted = bucket in self._namespace_bucket_annotation(ANNOTATION_ADOPTED_EMPTY_BUCKETS)
        if owner == mine or created or adopted:
            return OWN, ""
        return SHARED, ""

    def _record_s3(
        self,
        method: str,
        kind: str,
        name: str | None,
        bucket: str | None,
        mutating: bool,
        deleting: bool,
    ) -> None:
        if mutating:
            scope, note = self._bucket_scope(method, bucket)
        else:
            scope, note = READ, ""
        self._append(
            Call(
                api="s3",
                verb=method,
                kind=kind,
                namespace=None,
                name=name,
                lease_held=self.lease_held,
                mutating=mutating,
                deleting=deleting,
                scope=scope,
                method=method,
                note=note,
            )
        )

    # -- Kubernetes API ----------------------------------------------------

    @staticmethod
    def _parse_method(api: str, method: str) -> tuple[str, str, str, bool, bool, str]:
        """``(verb, kind_snake, kind, namespaced, all_namespaces, subresource)``."""
        base = method[: -len("_with_http_info")] if method.endswith("_with_http_info") else method
        m = _API_METHOD_RE.match(base)
        assert m is not None
        verb = m.group(1)
        rest = base[len(verb) + 1 :]
        if verb == "delete" and rest.startswith("collection_"):
            verb, rest = "delete_collection", rest[len("collection_") :]
        if verb == "connect":
            # connect_<http method>_namespaced_pod_exec
            rest = rest.split("_", 1)[1] if "_" in rest else rest
        all_ns = rest.endswith("_for_all_namespaces")
        if all_ns:
            rest = rest[: -len("_for_all_namespaces")]
        namespaced = rest.startswith("namespaced_")
        if namespaced:
            rest = rest[len("namespaced_") :]
        sub = ""
        if api == "CustomObjectsApi" and "custom_object" in rest:
            for s in ("_status", "_scale"):
                if rest.endswith(s):
                    rest, sub = rest[: -len(s)], s[1:]
            return verb, "custom_object", "", namespaced, all_ns, sub
        for s in _CONNECT_SUFFIXES if verb == "connect" else ():
            if rest.endswith(s):
                rest, sub = rest[: -len(s)], s[1:]
                break
        for s in _API_SUBRESOURCES:
            if rest.endswith(s) and rest != s[1:]:
                rest, sub = rest[: -len(s)], s[1:]
                break
        kind = canonical_kind(rest.replace("_", ""))
        return verb, rest, kind, namespaced, all_ns, sub

    def _api_call(self, api: str, method: str, args: tuple[Any, ...], kwargs: dict) -> Any:
        verb, kind_snake, kind, namespaced, all_ns, sub = self._parse_method(api, method)
        a = list(args)

        def take(key: str, *alts: str) -> Any:
            for k in (key, *alts):
                if k in kwargs:
                    return kwargs[k]
            return a.pop(0) if a else None

        name: str | None = None
        namespace: str | None = None
        body: Any = None
        custom = kind_snake == "custom_object"
        if custom:
            take("group")
            take("version")
            if namespaced:
                namespace = take("namespace")
            kind = canonical_kind(str(take("plural", "resource_plural") or ""))
            if verb == "create":
                body = take("body")
                name = _get(body, "metadata", "name")
            elif verb not in ("list", "delete_collection", "watch"):
                name = take("name")
                body = take("body")
        elif kind_snake in _META_KINDS:
            pass
        elif verb == "create" and not sub:
            if namespaced:
                namespace = take("namespace")
            body = take("body")
            name = _get(body, "metadata", "name")
        elif verb in ("list", "delete_collection", "watch"):
            if namespaced:
                namespace = take("namespace")
        else:
            # read/replace/patch/delete/connect, and create of a subresource
            # (create_namespaced_pod_eviction(name, namespace, body)).
            name = take("name")
            if namespaced:
                namespace = take("namespace")
            body = take("body")

        dry = bool(kwargs.get("dry_run"))
        is_review = kind in _REVIEW_KINDS
        mutating = (
            verb in ("create", "patch", "replace", "delete", "delete_collection", "connect")
            and not is_review
            and not dry
        )
        deleting = mutating and (
            verb in ("delete", "delete_collection") or sub in ("eviction", "finalize")
        )
        if all_ns:
            ns_for_scope: str | None = "*"
        elif namespaced:
            ns_for_scope = namespace
        else:
            ns_for_scope = None
        scope = (
            self._scope(
                kind,
                ns_for_scope,
                name,
                verb,
                custom=custom,
                cluster_call=not namespaced and kind not in ("namespaces", "secretclasses"),
            )
            if mutating
            else READ
        )
        idx = self._append(
            Call(
                api=api,
                verb=verb if not sub else f"{verb} {sub}",
                kind=kind,
                namespace=ns_for_scope,
                name=name,
                lease_held=self.lease_held,
                mutating=mutating,
                deleting=deleting,
                scope=scope,
                method=method,
                kwargs={k: v for k, v in kwargs.items() if k != "body"},
            )
        )
        self._maybe_fail(verb, kind, name, ns_for_scope)
        if dry:
            return copy.deepcopy(body)
        with self._lock:
            if custom:
                crd_scope = self._crd_for(kind)
                cluster_call = not namespaced and not all_ns
                lists_all = verb == "list" and cluster_call and crd_scope == "Namespaced"
                if crd_scope is None or (
                    (crd_scope == "Cluster") != cluster_call and not lists_all
                ):
                    raise _api_exception(404, "the server could not find the requested resource")
            if scope == LEASE and kind == "configmaps" and verb in ("replace", "patch", "delete"):
                self._check_lease_takeover(idx, verb)
            result = self._execute(
                api,
                verb,
                kind,
                kind_snake,
                namespace if namespaced else None,
                name,
                body,
                sub,
                kwargs,
                is_review,
                all_ns,
            )
        if method.endswith("_with_http_info"):
            return (result, 200, {})
        return result

    def _check_lease_takeover(self, idx: int, verb: str) -> None:
        """Flag a write to a live lease that is not this process's."""
        current = self.store.get(("configmaps", LOCK_NAMESPACE, LOCK_CONFIGMAP_NAME))
        if current is None:
            return
        data = _get(current, "data") or {}
        holder = data.get("holder", "")
        ours = (
            self._lease is not None
            and holder == self._lease.holder
            and self._lease.thread == threading.get_ident()
        )
        try:
            ttl = int(data.get("ttl-seconds", "0"))
        except ValueError:
            ttl = 0
        live = (
            _parse_iso(data.get("acquired-at", "")) + ttl > datetime.now(timezone.utc).timestamp()
        )
        if live and not ours and not self._allowed_delete(self.calls[idx]):
            self._annotate(idx, "lease-taken")

    def _maybe_fail(self, verb: str, kind: str, name: str | None, ns: str | None) -> None:
        for e in self._errors:
            if e.verb and e.verb != verb:
                continue
            if e.kind and e.kind != kind:
                continue
            if e.name and e.name != name:
                continue
            if e.namespace and e.namespace != ns:
                continue
            if e.times is not None:
                if e.times <= 0:
                    continue
                e.times -= 1
            raise _api_exception(e.status, "injected by recording_k8s")

    def _execute(
        self,
        api: str,
        verb: str,
        kind: str,
        kind_snake: str,
        namespace: str | None,
        name: str | None,
        body: Any,
        sub: str,
        kwargs: dict[str, Any],
        is_review: bool,
        all_ns: bool,
    ) -> Any:
        custom = kind_snake == "custom_object"
        key = (kind, namespace, name or "")
        model = None if custom else (_snake_to_model(kind_snake) or _plural_to_model(kind))

        if api == "VersionApi":
            return SimpleNamespace(git_version="v1.30.0", major="1", minor="30", platform="")
        if kind_snake in _META_KINDS:
            return SimpleNamespace(resources=[], versions=[], groups=[])
        if is_review:
            status = SimpleNamespace(allowed=self.can_i, denied=not self.can_i, reason="")
            if isinstance(body, dict):
                out = copy.deepcopy(body)
                out["status"] = {"allowed": self.can_i}
                return out
            if body is not None:
                body.status = status
                return body
            return SimpleNamespace(status=status)

        if verb in ("read", "get"):
            if sub == "log":
                return self.pod_logs.get((namespace or "", name or ""), "")
            if key not in self.store:
                raise _api_exception(404, "Not Found")
            return copy.deepcopy(self.store[key])

        if verb in ("list", "watch"):
            selector = kwargs.get("label_selector")
            items = [
                copy.deepcopy(obj)
                for (k, ns, _n), obj in sorted(self.store.items(), key=lambda kv: str(kv[0]))
                if k == kind
                and (all_ns or namespace is None or ns == namespace)
                and _labels_match(obj, selector)
            ]
            if custom:
                return {"items": [_serialize(i) if not isinstance(i, dict) else i for i in items]}
            return SimpleNamespace(
                items=items,
                metadata=SimpleNamespace(resource_version=self._next_rv(), _continue=None),
            )

        if verb == "connect":
            return self.exec_output

        if verb == "create":
            if sub == "eviction":
                self.store.pop((kind, namespace, name or ""), None)
                return SimpleNamespace(status="Success")
            if not name:
                raise _api_exception(422, "metadata.name required")
            if namespace and ("namespaces", None, namespace) not in self.store:
                raise _api_exception(404, f"namespaces {namespace!r} not found")
            if key in self.store:
                raise _api_exception(409, "AlreadyExists")
            obj = copy.deepcopy(body) if custom else _to_model(body, model)
            meta: dict[str, Any] = {
                "resource_version": self._next_rv(),
                "uid": f"uid-{kind}-{name}-{self._next_rv()}",
            }
            if not custom:
                meta["creation_timestamp"] = datetime.now(timezone.utc)
            if namespace:
                meta["namespace"] = namespace
            _set_meta(obj, **meta)
            self.store[key] = obj
            self._track_lease(kind, namespace, name, obj, created=True)
            return copy.deepcopy(obj)

        if verb == "replace":
            if key not in self.store:
                raise _api_exception(404, "Not Found")
            current = self.store[key]
            want_rv = _get(body, "metadata", "resource_version")
            if want_rv and want_rv != _get(current, "metadata", "resource_version"):
                raise _api_exception(409, "Conflict")
            obj = copy.deepcopy(body) if custom else _to_model(body, model)
            _set_meta(
                obj,
                resource_version=self._next_rv(),
                uid=_get(current, "metadata", "uid"),
                name=name,
            )
            if namespace:
                _set_meta(obj, namespace=namespace)
            self.store[key] = obj
            self._track_lease(kind, namespace, name, obj, created=True)
            return copy.deepcopy(obj)

        if verb == "patch":
            if key not in self.store:
                raise _api_exception(404, "Not Found")
            current = self.store[key]
            if isinstance(body, list):
                patched = current  # JSON patch: recorded, not applied
            else:
                patch = body if isinstance(body, dict) else _serialize(body)
                if isinstance(current, dict):
                    patched = _deep_merge(current, patch)
                else:
                    merged = _deep_merge(_serialize(current), patch)
                    cls = type(current).__name__
                    try:
                        patched = _deserialize(merged, cls)
                    except Exception:  # noqa: BLE001
                        patched = current
            _set_meta(patched, resource_version=self._next_rv())
            self.store[key] = patched
            return copy.deepcopy(patched)

        if verb == "delete":
            if key not in self.store:
                raise _api_exception(404, "Not Found")
            current = self.store[key]
            pre = _get(body, "preconditions")
            if pre is not None:
                want_rv = _get(pre, "resource_version")
                want_uid = _get(pre, "uid")
                if want_rv and want_rv != _get(current, "metadata", "resource_version"):
                    raise _api_exception(409, "Conflict: resourceVersion precondition")
                if want_uid and want_uid != _get(current, "metadata", "uid"):
                    raise _api_exception(409, "Conflict: uid precondition")
            del self.store[key]
            if kind == "namespaces" and name:
                for k in [k for k in self.store if k[1] == name]:
                    del self.store[k]
            self._track_lease(kind, namespace, name, current, created=False)
            return SimpleNamespace(status="Success")

        if verb == "delete_collection":
            selector = kwargs.get("label_selector")
            for k in [
                k
                for k, obj in self.store.items()
                if k[0] == kind and k[1] == namespace and _labels_match(obj, selector)
            ]:
                del self.store[k]
            return SimpleNamespace(status="Success")

        return None

    def _track_lease(
        self, kind: str, ns: str | None, name: str | None, obj: Any, *, created: bool
    ) -> None:
        if not (kind == "configmaps" and ns == LOCK_NAMESPACE and name == LOCK_CONFIGMAP_NAME):
            return
        holder = (_get(obj, "data") or {}).get("holder", "")
        if created:
            self._lease = _Lease(holder=holder, thread=threading.get_ident())
        elif self._lease is not None and holder == self._lease.holder:
            self._lease = None

    def _stream(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        for k in ("stderr", "stdin", "stdout", "tty", "_request_timeout", "_preload_content"):
            kwargs.pop(k, None)
        command = kwargs.pop("command", None)
        with self._lock:
            out = func(*args, **kwargs)
            if command is not None and self.calls:
                self.calls[-1] = dataclasses.replace(
                    self.calls[-1], argv=tuple(str(c) for c in command)
                )
        return out

    # -- CLI ---------------------------------------------------------------

    def _run_command(
        self, args: Any, kwargs: dict[str, Any], *, popen: bool = False
    ) -> subprocess.CompletedProcess[Any]:
        if isinstance(args, (str, bytes)):
            text_args = args.decode() if isinstance(args, bytes) else args
            argv = ["sh", "-c", text_args] if kwargs.get("shell") else [text_args]
        else:
            argv = [a if isinstance(a, str) else os.fspath(a) for a in args]
        recorded = {k: v for k, v in kwargs.items() if k not in ("env",)}
        if popen:
            recorded["popen"] = True

        results = [self._run_one(cmd, argv, kwargs, recorded) for cmd in _unwrap(argv)]
        rc, out, err = next(((r, o, e) for r, o, e in results if r != 0), results[-1])

        text = bool(
            kwargs.get("text") or kwargs.get("universal_newlines") or kwargs.get("encoding")
        )

        def conv(s: Any) -> Any:
            if s is None:
                return None
            if text:
                return s.decode() if isinstance(s, bytes) else s
            return s.encode() if isinstance(s, str) else s

        captured = popen or kwargs.get("capture_output") or kwargs.get("stdout") is not None
        result = subprocess.CompletedProcess(
            argv, rc, conv(out) if captured else None, conv(err) if captured else None
        )
        if kwargs.get("check") and rc != 0 and not popen:
            raise subprocess.CalledProcessError(rc, argv, result.stdout, result.stderr)
        return result

    def _run_one(
        self, cmd: list[str], argv: list[str], kwargs: dict[str, Any], recorded: dict[str, Any]
    ) -> tuple[int, Any, Any]:
        tool = _tool_name(cmd)
        if tool in ("kubectl", "oc"):
            parsed = _parse_argv([tool, *cmd[1:]], _KUBE_VALUE_FLAGS)
            targets = _kube_targets(parsed, kwargs)
        elif tool == "helm":
            parsed = _parse_argv([tool, *cmd[1:]], _HELM_VALUE_FLAGS)
            targets = [_helm_target(parsed)]
        else:
            parsed = _parse_argv(cmd, frozenset())
            verb = parsed.positionals[0] if parsed.positionals else ""
            if tool in LOCAL_TOOLS:
                targets = [_Target(verb, "", None, None, False, False)]
            elif _is_child_lakebench(cmd):
                targets = [_Target(verb, "", None, None, True, False, True, "child")]
            else:
                targets = [_Target(verb, "", None, None, True, False, True, "unknown-tool")]

        indices = []
        for t in targets:
            if not t.mutating:
                scope = LOCAL if tool in LOCAL_TOOLS else READ
            elif t.force_shared:
                scope = SHARED
            else:
                scope = self._scope(
                    t.kind, t.namespace, t.name, t.verb.split(" ", 1)[0], custom=True
                )
            if scope == LEASE and t.mutating:
                t.note = "lease-cli"  # cluster_lock is API-only; nothing else may touch it
            api = "lakebench" if t.note == "child" else tool
            indices.append(
                self._append(
                    Call(
                        api=api,
                        verb=t.verb,
                        kind=t.kind,
                        namespace=t.namespace,
                        name=t.name,
                        lease_held=self.lease_held,
                        argv=tuple(argv),
                        mutating=t.mutating,
                        deleting=t.deleting,
                        scope=scope,
                        note=t.note,
                        kwargs=recorded,
                    )
                )
            )
        if tool in LOCAL_TOOLS:
            return 0, "", ""
        rc, out, err, answered = self._respond(parsed, cmd, kwargs, targets)
        if not answered and not any(t.mutating for t in targets) and tool in CLUSTER_TOOLS:
            for i in indices:
                self._annotate(i, "unscripted")
            return 1, "", f"recording_k8s: unscripted read {' '.join(cmd)!r}; script it"
        return rc, out, err

    def _respond(
        self, parsed: _Parsed, argv: list[str], kwargs: dict[str, Any], targets: list[_Target]
    ) -> tuple[int, Any, Any, bool]:
        norm = parsed.normalized
        for r in reversed(self._responders):
            if tuple(norm[: len(r.prefix)]) == r.prefix:
                if r.raises is not None:
                    raise r.raises
                if r.handler is not None:
                    out = r.handler(argv, kwargs)
                    if isinstance(out, subprocess.CompletedProcess):
                        return out.returncode, out.stdout, out.stderr, True
                    if isinstance(out, tuple):
                        rc, so, se = (list(out) + [0, "", ""])[:3]
                        return int(rc), so, se, True
                    return 0, "" if out is None else str(out), "", True
                return r.returncode, r.stdout, r.stderr, True
        with self._lock:
            if parsed.tool in ("kubectl", "oc"):
                return self._kubectl_builtin(parsed, targets)
            if parsed.tool == "helm":
                return self._helm_builtin(parsed, targets[0])
        return 0, "", "", any(t.mutating for t in targets)

    def _kubectl_builtin(self, p: _Parsed, targets: list[_Target]) -> tuple[int, str, str, bool]:
        verb = p.positionals[0] if p.positionals else ""
        sub = p.positionals[1] if len(p.positionals) > 1 else ""
        if any(t.mutating for t in targets):
            return 0, "", "", True
        if _dry_run(p):
            return 1, "", "", False  # its rendered output matters: script it
        if verb == "get":
            answers = [self._kubectl_get(p, t) for t in targets]
            if len(answers) == 1 or not all(a[3] for a in answers):
                return answers[0] if len(answers) == 1 else (1, "", "", False)
            failed = next((a for a in answers if a[0] != 0), None)
            if failed is not None:
                return failed
            if (p.last("-o", "--output") or "") == "json":
                items = [json.loads(a[1]) for a in answers]
                return 0, json.dumps({"apiVersion": "v1", "kind": "List", "items": items}), "", True
            return 0, "".join(a[1] for a in answers), "", True
        if verb == "api-resources":
            group = p.last("--api-group")
            rows = ["NAME SHORTNAMES APIVERSION NAMESPACED KIND"]
            for (k, _ns, _n), crd in sorted(self.store.items(), key=lambda kv: str(kv[0])):
                if k != "customresourcedefinitions":
                    continue
                g = _get(crd, "spec", "group")
                if group and g != group:
                    continue
                names = _get(crd, "spec", "names")
                rows.append(
                    f"{_get(names, 'plural')} {g}/v1 "
                    f"{str(_get(crd, 'spec', 'scope') == 'Namespaced').lower()} "
                    f"{_get(names, 'kind')}"
                )
            return 0, "\n".join(rows) + "\n", "", True
        if verb == "auth" and sub == "can-i":
            return (0, "yes\n", "", True) if self.can_i else (1, "no\n", "", True)
        if verb in ("wait", "version", "whoami") or (verb == "rollout" and sub == "status"):
            return 0, "", "", True
        if verb == "config":
            return 0, "test\n", "", True
        if verb in ("logs", "log"):
            name = targets[0].name or (p.positionals[1] if len(p.positionals) > 1 else "")
            ns = p.last("-n", "--namespace") or "default"
            return 0, self.pod_logs.get((ns, name or ""), ""), "", True
        return 1, "", "", False

    def _helm_builtin(self, p: _Parsed, t: _Target) -> tuple[int, str, str, bool]:
        verb = t.verb
        ns = t.namespace or "default"
        name = t.name
        rel = self.releases.get((ns, name or ""))
        as_json = (p.last("-o", "--output") or "") == "json"
        not_found = f"Error: release: not found ({name})"
        if verb in ("repo", "version", "env", "dependency", "dep", "help"):
            return 0, "", "", True
        if verb in ("list", "ls"):
            pattern = p.last("-f", "--filter")
            every = "-A" in p.bools or "--all-namespaces" in p.bools
            rels = [
                r
                for (rns, _), r in sorted(self.releases.items())
                if (every or rns == ns) and (not pattern or re.search(pattern, r.name))
            ]
            rows = [
                {
                    "name": r.name,
                    "namespace": r.namespace,
                    "revision": str(r.revision),
                    "status": r.status,
                    "chart": f"{r.chart}-{r.version}",
                    "app_version": r.version,
                }
                for r in rels
            ]
            if as_json:
                return 0, json.dumps(rows), "", True
            return 0, "\n".join(["NAME"] + [r["name"] for r in rows]) + "\n", "", True
        if verb == "status":
            if rel is None:
                return 1, "", not_found, True
            if as_json:
                doc = {"name": rel.name, "version": rel.revision, "info": {"status": rel.status}}
                return 0, json.dumps(doc), "", True
            return 0, f"NAME: {rel.name}\nSTATUS: {rel.status}\n", "", True
        if verb == "get" and p.positionals[1:2] == ["values"]:
            if rel is None:
                return 1, "", f"Error: release: not found ({name})", True
            if as_json:
                return 0, json.dumps(rel.values), "", True
            import yaml

            return 0, "USER-SUPPLIED VALUES:\n" + yaml.safe_dump(rel.values), "", True
        if verb in ("history", "hist"):
            if rel is None:
                return 1, "", not_found, True
            rows = [{"revision": rel.revision, "status": rel.status, "chart": rel.chart}]
            return 0, json.dumps(rows), "", True
        if not t.mutating:
            # show, template, a dry-run upgrade: the rendered output matters,
            # so the test scripts it.
            return 1, "", "", False
        sets = p.all(*_SET_FLAGS)
        if verb in ("upgrade", "install"):
            install = verb == "install" or "--install" in p.bools or "-i" in p.bools
            if verb == "install" and rel is not None:
                return (
                    1,
                    "",
                    "Error: INSTALL FAILED: cannot re-use a name that is still in use",
                    True,
                )
            if rel is None and not install:
                return 1, "", f'Error: UPGRADE FAILED: "{name}" has no deployed releases', True
            chart = (p.positionals[2] if len(p.positionals) > 2 else name) or ""
            chart = chart.rsplit("/", 1)[-1]
            if rel is None:
                rel = Release(name or "", ns, chart, p.last("--version") or "1.0.0", {}, 0)
                self.releases[(ns, rel.name)] = rel
            reuse = "--reuse-values" in p.bools or "--reset-then-reuse-values" in p.bools
            base = rel.values if reuse else {}
            if "--reset-values" in p.bools:
                base = {}
            rel.values = _apply_helm_sets(base, sets)
            rel.version = p.last("--version") or rel.version
            rel.revision += 1
            watched = (rel.values.get("spark") or {}).get("jobNamespaces")
            if isinstance(watched, list) and ("deployments", ns, "spark-operator-controller") in (
                self.store
            ):
                self._sync_operator(ns, watched)
            return 0, f"Release {rel.name!r} has been upgraded.\n", "", True
        if verb in _HELM_DELETING_VERBS:
            if rel is None:
                return (
                    1,
                    "",
                    f"Error: uninstall: Release not loaded: {name}: release: not found",
                    True,
                )
            del self.releases[(ns, rel.name)]
            return 0, f'release "{rel.name}" uninstalled\n', "", True
        return 0, "", "", True

    def _kubectl_get(self, p: _Parsed, t: _Target) -> tuple[int, str, str, bool]:
        output = p.last("-o", "--output") or ""
        kind = t.kind
        ns = None if kind in CLUSTER_SCOPED_KINDS else (t.namespace or "default")
        ignore = "--ignore-not-found" in p.bools
        if t.name:
            obj = self.store.get((kind, ns, t.name))
            if obj is None and kind == "customresourcedefinitions":
                obj = self.store.get((kind, None, t.name))
            if obj is None:
                if ignore:
                    return 0, "", "", True
                return (
                    1,
                    "",
                    f'Error from server (NotFound): {kind} "{t.name}" not found',
                    True,
                )
            doc: Any = _serialize(obj)
            docs = [doc]
        else:
            selector = p.last("-l", "--selector")
            docs = [
                _serialize(o)
                for (k, n, _), o in sorted(self.store.items(), key=lambda kv: str(kv[0]))
                if k == kind and (ns is None or ns == "*" or n == ns) and _labels_match(o, selector)
            ]
            doc = {"apiVersion": "v1", "kind": "List", "items": docs}
        if output == "json":
            return 0, json.dumps(doc), "", True
        if output.startswith("jsonpath="):
            try:
                return 0, _jsonpath(doc, output[len("jsonpath=") :]), "", True
            except ValueError:
                return 1, "", "", False
        if output == "name":
            return 0, "\n".join(f"{kind}/{d['metadata']['name']}" for d in docs), "", True
        if output in ("", "wide"):
            if not docs:
                return 0, "", "No resources found\n", True
            if ns == "*":  # -A: kubectl puts the namespace first
                rows = [
                    f"{d['metadata'].get('namespace', '')} {d['metadata']['name']}" for d in docs
                ]
                lines = ["NAMESPACE NAME", *rows]
            else:
                lines = ["NAME"] + [d["metadata"]["name"] for d in docs]
            return 0, "\n".join(lines) + "\n", "", True
        return 1, "", "", False

    # -- install -----------------------------------------------------------

    def install(self, monkeypatch: pytest.MonkeyPatch) -> K8sRecorder:
        """Patch every cluster and S3 entry point for the monkeypatch's life."""
        import boto3
        import kubernetes.client as kc
        import kubernetes.config as kconfig
        import kubernetes.stream as kstream

        for attr in dir(kc):
            if attr.endswith("Api") and isinstance(getattr(kc, attr), type):
                fake = _make_fake_api(self, attr)
                monkeypatch.setattr(kc, attr, fake)
                self._fakes[f"kubernetes.client.{attr}"] = fake
        monkeypatch.setattr(kstream, "stream", self._stream)
        monkeypatch.setattr(
            kconfig, "load_kube_config", _fake_load_kube_config(kconfig.load_kube_config)
        )
        monkeypatch.setattr(kconfig, "load_incluster_config", lambda *a, **k: None)

        recorder = self

        def fake_run(args: Any, *a: Any, **kw: Any) -> subprocess.CompletedProcess[Any]:
            return recorder._run_command(args, kw)

        def fake_popen(args: Any, *a: Any, **kw: Any) -> _FakePopen:
            proc = _FakePopen(recorder, args, kw)
            recorder.processes.append(proc)
            return proc

        def fake_boto3_client(service_name: str, *a: Any, **kw: Any) -> Any:
            if service_name != "s3":
                raise NotImplementedError(f"recording_k8s fakes only s3, not {service_name!r}")
            return _FakeS3(recorder)

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setattr(subprocess, "Popen", fake_popen)
        monkeypatch.setattr(boto3, "client", fake_boto3_client)

        # unittest.mock.patch is undone before teardown, so the teardown check
        # cannot see it: catch it when it is entered instead.
        import unittest.mock as um

        guarded = {id(subprocess), id(boto3), id(kc), id(kstream)}
        real_enter = um._patch.__enter__  # type: ignore[attr-defined]

        def guarded_enter(patcher: Any) -> Any:
            result = real_enter(patcher)
            original = getattr(patcher, "temp_original", None)
            fakes = {id(f) for f in self._fakes.values()}
            if id(original) in fakes or id(original) in guarded:
                self._append(
                    Call(
                        api="unittest.mock",
                        verb="patch",
                        kind="",
                        namespace=None,
                        name=f"{getattr(patcher, 'attribute', '?')}",
                        lease_held=self.lease_held,
                        mutating=False,
                        note="patched-over",
                    )
                )
            return result

        monkeypatch.setattr(um._patch, "__enter__", guarded_enter)  # type: ignore[attr-defined]
        self._fakes.update(
            {
                "subprocess.run": fake_run,
                "subprocess.Popen": fake_popen,
                "boto3.client": fake_boto3_client,
            }
        )
        self._installed = True
        return self

    def _replaced_patches(self) -> list[str]:
        import boto3
        import kubernetes.client as kc

        live = {
            "subprocess.run": subprocess.run,
            "subprocess.Popen": subprocess.Popen,
            "boto3.client": boto3.client,
        }
        for key in self._fakes:
            if key.startswith("kubernetes.client."):
                live[key] = getattr(kc, key.rsplit(".", 1)[1], None)
        return sorted(k for k, fake in self._fakes.items() if live.get(k) is not fake)

    # -- queries and assertions --------------------------------------------

    def mutations(self) -> list[Call]:
        return [c for c in self.calls if c.mutating]

    def shared_mutations(self) -> list[Call]:
        return [c for c in self.calls if c.mutating and c.scope == SHARED]

    def cluster_calls(self) -> list[Call]:
        """Every call that reached the fake cluster or S3 (not local tools)."""
        return [c for c in self.calls if c.scope != LOCAL]

    def assert_recorded(self, **match: Any) -> list[Call]:
        """The calls whose fields equal ``match``; raises when there are none.

        Consumers use it to prove the run reached the mutation under test,
        for example ``assert_recorded(api="helm", verb="upgrade",
        lease_held=True)``, so an early error path cannot pass as clean.
        """
        hits = [c for c in self.calls if all(getattr(c, k) == v for k, v in match.items())]
        if not hits:
            lines = "\n".join(f"  - {c.describe()}" for c in self.calls) or "  (none)"
            raise AssertionError(f"recording_k8s: no call matched {match}; recorded:\n{lines}")
        return hits

    def _allowed_delete(self, call: Call) -> bool:
        target = call.target
        short = f"{call.kind}/{call.name or '*'}"
        return any(
            fnmatch.fnmatchcase(target, p) or fnmatch.fnmatchcase(short, p)
            for p in self.allow_delete
        )

    def violations(self) -> list[Violation]:
        out: list[Violation] = []
        for c in self.calls:
            if c.note in _NOTE_REASONS:
                out.append(Violation(c, _NOTE_REASONS[c.note]))
            if not c.mutating or c.scope in (OWN, LEASE, READ, LOCAL):
                continue
            if c.deleting and c.note != "not-created" and not self._allowed_delete(c):
                out.append(Violation(c, REASON_FOREIGN_DELETE))
            if c.api == "s3":
                if not c.deleting:
                    out.append(Violation(c, REASON_FOREIGN_BUCKET))
                continue
            if c.note in ("child", "unknown-tool"):
                continue
            if not c.lease_held:
                out.append(Violation(c, REASON_UNLEASED))
        return out

    def assert_clean(self) -> None:
        if not self._installed:
            raise AssertionError("recording_k8s was never installed; it recorded nothing")
        replaced = self._replaced_patches()
        if replaced:
            raise AssertionError(
                "recording_k8s: another patch replaced the recorder's "
                f"{', '.join(replaced)}; calls made through it were never recorded"
            )
        if not self.cluster_calls() and not self._no_calls_expected:
            raise AssertionError(
                "recording_k8s: no cluster call was recorded; the code under test "
                "either bypassed the recorder or never ran. Use assert_no_calls() "
                "when no call is the expected outcome"
            )
        bad = self.violations()
        if bad:
            lines = "\n".join(f"  - {v}" for v in bad)
            raise AssertionError(
                f"recording_k8s: {len(bad)} violation(s) for namespace {self.namespace!r}:\n{lines}"
            )

    def assert_no_calls(self) -> None:
        self._no_calls_expected = True
        calls = self.cluster_calls()
        if calls:
            lines = "\n".join(f"  - {c.describe()}" for c in calls)
            raise AssertionError(
                f"recording_k8s: expected no cluster calls, got {len(calls)}:\n{lines}"
            )


@contextmanager
def recording(
    namespace: str | None = None,
    *,
    buckets: Iterable[str] = (),
    allow_delete: Iterable[str] = (),
    deployment: str | None = None,
) -> Iterator[K8sRecorder]:
    """Install a recorder for a ``with`` block (no teardown assertion)."""
    with pytest.MonkeyPatch.context() as mp:
        rec = K8sRecorder(
            namespace, buckets=buckets, allow_delete=allow_delete, deployment=deployment
        )
        rec.install(mp)
        yield rec


def _fixture_body(monkeypatch: pytest.MonkeyPatch) -> Iterator[K8sRecorder]:
    rec = K8sRecorder()
    rec.install(monkeypatch)
    yield rec
    if rec.teardown_check:
        rec.assert_clean()


@pytest.fixture
def recording_k8s(monkeypatch: pytest.MonkeyPatch) -> Iterator[K8sRecorder]:
    """The recorder, installed for this test; teardown asserts no violation.

    A test that means to produce violations and assert on them sets
    ``recording_k8s.teardown_check = False``.
    """
    yield from _fixture_body(monkeypatch)
