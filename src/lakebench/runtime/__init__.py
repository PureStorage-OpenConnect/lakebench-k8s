"""Runtime abstraction: where lakebench components are deployed.

Kubernetes is the production runtime. A container runtime (podman or docker)
supports local single-host runs where no cluster is available.

The protocol does not model StatefulSets, PVCs or SCCs: those are
Kubernetes concepts with no local equivalent, and Kubernetes deployers use
``K8sClient`` directly.
"""

from lakebench.runtime.protocol import (
    ComponentSpec,
    ContainerPort,
    Mount,
    Runtime,
)

__all__ = [
    "ComponentSpec",
    "ContainerPort",
    "Mount",
    "Runtime",
]
