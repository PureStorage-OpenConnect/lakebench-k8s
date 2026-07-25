"""Runtime protocol and component specification.

``Runtime`` is the substrate a component is deployed onto. ``K8sRuntime``
wraps the existing ``K8sClient`` and is a pass-through: it changes no
behaviour and exists so deployer signatures can stop naming Kubernetes
directly. ``ContainerRuntime`` (podman/docker) is added separately for
local mode.

Scope is deliberately narrow. ``ComponentSpec`` describes what both a
Kubernetes workload and a ``podman run`` need in common: an image, an
environment, ports, mounts, a command, and a readiness signal. Anything
Kubernetes-specific stays behind ``K8sRuntime.k8s``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from lakebench.k8s import K8sClient


class RuntimeKind(str, Enum):
    """Which substrate a runtime targets."""

    KUBERNETES = "kubernetes"
    CONTAINER = "container"


@dataclass(frozen=True)
class ContainerPort:
    """A port a component listens on."""

    container_port: int
    host_port: int | None = None
    name: str = ""


@dataclass(frozen=True)
class Mount:
    """A filesystem mount into a component."""

    source: str
    target: str
    read_only: bool = False


@dataclass
class ComponentSpec:
    """Runtime-neutral description of a deployable component.

    Covers what a Kubernetes workload and a ``podman run`` both need. It is
    intentionally minimal: extend it only when a real component requires it,
    not in anticipation.
    """

    name: str
    image: str
    command: list[str] = field(default_factory=list)
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    ports: list[ContainerPort] = field(default_factory=list)
    mounts: list[Mount] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)
    # Command whose exit code signals readiness. Empty means "started is ready".
    readiness_command: list[str] = field(default_factory=list)
    replicas: int = 1


@runtime_checkable
class Runtime(Protocol):
    """The substrate components are deployed onto."""

    def kind(self) -> RuntimeKind:
        """Return which substrate this runtime targets."""
        ...

    def apply(self, spec: ComponentSpec) -> None:
        """Create or update a component."""
        ...

    def delete(self, name: str) -> None:
        """Remove a component. Must not raise if it is already absent."""
        ...

    def wait_ready(self, name: str, timeout: int = 300) -> bool:
        """Block until the component is ready. Returns False on timeout."""
        ...

    def exec(self, name: str, command: list[str]) -> tuple[int, str]:
        """Run a command inside the component. Returns (exit_code, output)."""
        ...

    def logs(self, name: str) -> str:
        """Return the component's logs."""
        ...


class K8sRuntime:
    """Kubernetes runtime. A pass-through wrapper over ``K8sClient``.

    This exists so deployers can depend on ``Runtime`` rather than naming
    Kubernetes directly. It adds no behaviour of its own. Deployers that
    need Kubernetes-specific operations (StatefulSets, PVCs, SCCs, CRDs)
    use ``.k8s`` and remain Kubernetes-only by design.
    """

    def __init__(self, k8s: K8sClient) -> None:
        self.k8s = k8s

    def kind(self) -> RuntimeKind:
        return RuntimeKind.KUBERNETES

    def apply(self, spec: ComponentSpec) -> None:
        raise NotImplementedError(
            "K8sRuntime.apply() is not used. Kubernetes deployers render Jinja2 "
            "templates and apply them through K8sRuntime.k8s directly."
        )

    def delete(self, name: str) -> None:
        raise NotImplementedError(
            "K8sRuntime.delete() is not used. Kubernetes deployers remove "
            "resources through K8sRuntime.k8s directly."
        )

    def wait_ready(self, name: str, timeout: int = 300) -> bool:
        raise NotImplementedError(
            "K8sRuntime.wait_ready() is not used. Kubernetes deployers use "
            "their own _wait_for_ready() against K8sRuntime.k8s."
        )

    def exec(self, name: str, command: list[str]) -> tuple[int, str]:
        raise NotImplementedError(
            "K8sRuntime.exec() is not used. Kubernetes callers use "
            "K8sRuntime.k8s.exec_command() directly."
        )

    def logs(self, name: str) -> str:
        raise NotImplementedError(
            "K8sRuntime.logs() is not used. Kubernetes callers read logs "
            "through K8sRuntime.k8s directly."
        )

    # Attribute passthrough keeps existing call sites working when a
    # K8sRuntime is supplied where a K8sClient was previously expected.
    def __getattr__(self, item: str) -> Any:
        return getattr(self.k8s, item)


def as_k8s_client(runtime: Runtime | K8sClient | None) -> Any:
    """Return the underlying K8sClient from a runtime or client.

    Accepts a ``K8sRuntime``, a bare ``K8sClient``, or ``None`` so that the
    transition can proceed without changing every call site at once.
    """
    if runtime is None:
        return None
    return getattr(runtime, "k8s", runtime)
