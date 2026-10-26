"""Runtime protocol and component specification.

``Runtime`` is the substrate a component is deployed onto. Local mode's
``ContainerRuntime`` (podman/docker) implements it; Kubernetes deployers
use ``K8sClient`` directly.

``ComponentSpec`` describes what a ``podman run`` needs: an image, an
environment, ports, mounts and a command.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


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


@runtime_checkable
class Runtime(Protocol):
    """The substrate components are deployed onto."""

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
