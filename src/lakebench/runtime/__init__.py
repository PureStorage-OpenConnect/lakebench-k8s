"""Runtime abstraction: where lakebench components are deployed.

Kubernetes is the production runtime. A container runtime (podman or docker)
supports local single-host runs where no cluster is available.

The protocol covers only what deployers genuinely need from their substrate.
It deliberately does NOT model StatefulSets, PVCs, or SCCs: those are
Kubernetes concepts with no local equivalent, and K8s deployers reach them
through ``K8sRuntime.k8s`` rather than through a lowest-common-denominator
abstraction.
"""

from lakebench.runtime.protocol import (
    ComponentSpec,
    ContainerPort,
    Mount,
    Runtime,
    RuntimeKind,
)

__all__ = [
    "ComponentSpec",
    "ContainerPort",
    "Mount",
    "Runtime",
    "RuntimeKind",
]
