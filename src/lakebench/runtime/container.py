"""Container runtime: podman or docker on a single host.

Used by local mode, where no Kubernetes cluster is available. The whole local
stack is two containers, so this shells out to the container CLI rather than
depending on an orchestrator.

Podman is preferred when both are present: the project already standardises on
podman for image builds, and it needs no daemon.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time

from lakebench.runtime.protocol import ComponentSpec, RuntimeKind

logger = logging.getLogger(__name__)

# Applied to every container so `delete_all()` can find them without guessing
# from name prefixes.
MANAGED_LABEL = "lakebench.managed"


class ContainerRuntimeError(RuntimeError):
    """A container command failed."""


def detect_container_cli() -> str:
    """Return the container CLI to use, preferring podman.

    Raises:
        ContainerRuntimeError: if neither podman nor docker is on PATH.
    """
    for cli in ("podman", "docker"):
        if shutil.which(cli):
            return cli
    raise ContainerRuntimeError(
        "Neither podman nor docker found on PATH. Local mode needs one of them.\n"
        "  Install podman: https://podman.io/docs/installation\n"
        "  Install docker: https://docs.docker.com/engine/install/"
    )


class ContainerRuntime:
    """Runs components as containers via podman or docker.

    Implements the ``Runtime`` protocol. Unlike ``K8sRuntime``, which is a
    pass-through to the Kubernetes client, this one does real work: it is the
    substrate for local mode.
    """

    def __init__(self, cli: str = "", namespace: str = "lakebench") -> None:
        """
        Args:
            cli: Container CLI to use. Auto-detected when empty.
            namespace: Logical grouping, applied as a label and name prefix.
                Containers share a flat namespace on a single host, so this
                keeps concurrent runs from colliding.
        """
        self.cli = cli or detect_container_cli()
        self.namespace = namespace
        # Per-instance: a class-level dict would leak readiness commands
        # between runtimes.
        self._readiness: dict[str, list[str]] = {}

    # -- protocol -----------------------------------------------------------

    def kind(self) -> RuntimeKind:
        return RuntimeKind.CONTAINER

    def apply(self, spec: ComponentSpec, replace: bool = False) -> None:
        """Create the container, or reuse a running one with the same image.

        Reuse matters for correctness, not just speed. Recreating a container
        discards anything held in its writable layer, so a deployer that calls
        ``apply()`` twice would silently reset the component's state. An
        already-running container with the same image is left alone.

        Args:
            spec: Component to create.
            replace: Force remove-and-recreate even when a match is running.
        """
        name = self._qualified(spec.name)

        if not replace and self._is_running(name) and self._image_of(name) == spec.image:
            logger.debug("Container %s already running with image %s, reusing", name, spec.image)
            return

        self.delete(spec.name)

        cmd = [self.cli, "run", "-d", "--name", name]
        cmd += ["--label", f"{MANAGED_LABEL}=true"]
        cmd += ["--label", f"lakebench.namespace={self.namespace}"]
        for key, value in spec.labels.items():
            cmd += ["--label", f"{key}={value}"]
        for key, value in spec.env.items():
            cmd += ["-e", f"{key}={value}"]
        for port in spec.ports:
            host = port.host_port if port.host_port is not None else port.container_port
            cmd += ["-p", f"{host}:{port.container_port}"]
        for mount in spec.mounts:
            # Mount options are comma-separated, not colon-separated:
            # "src:dst:ro,Z" is valid, "src:dst:ro:Z" is not.
            options = []
            if mount.read_only:
                options.append("ro")
            if self.cli == "podman":
                # SELinux relabel, required on RHEL hosts.
                options.append("Z")
            suffix = f":{','.join(options)}" if options else ""
            cmd += ["-v", f"{mount.source}:{mount.target}{suffix}"]
        if spec.command:
            cmd += ["--entrypoint", spec.command[0]]
            if len(spec.command) > 1:
                logger.debug("Extra entrypoint args passed as args: %s", spec.command[1:])
        cmd.append(spec.image)
        if spec.command and len(spec.command) > 1:
            cmd += spec.command[1:]
        cmd += spec.args

        self._run(cmd, what=f"start {spec.name}")

    def delete(self, name: str) -> None:
        """Remove a container. Does not raise when it is already absent."""
        self._run(
            [self.cli, "rm", "-f", self._qualified(name)],
            what=f"remove {name}",
            check=False,
        )

    def wait_ready(self, name: str, timeout: int = 300) -> bool:
        """Block until the container is running and its readiness check passes."""
        qualified = self._qualified(name)
        deadline = time.monotonic() + timeout
        readiness = self._readiness.get(name, [])

        while time.monotonic() < deadline:
            if not self._is_running(qualified):
                time.sleep(2)
                continue
            if not readiness:
                return True
            code, _ = self.exec(name, readiness)
            if code == 0:
                return True
            time.sleep(2)

        logger.warning("Container %s not ready within %ss", qualified, timeout)
        return False

    def exec(self, name: str, command: list[str]) -> tuple[int, str]:
        """Run a command inside the container."""
        result = subprocess.run(  # noqa: S603
            [self.cli, "exec", self._qualified(name), *command],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode, (result.stdout or "") + (result.stderr or "")

    def logs(self, name: str) -> str:
        result = subprocess.run(  # noqa: S603
            [self.cli, "logs", self._qualified(name)],
            capture_output=True,
            text=True,
            check=False,
        )
        return (result.stdout or "") + (result.stderr or "")

    # -- extras beyond the protocol ------------------------------------------

    def register_readiness(self, name: str, command: list[str]) -> None:
        """Record the readiness command for a component.

        Kept separate from ``apply()`` so ``wait_ready()`` can be called
        independently, mirroring how Kubernetes deployers wait after applying.
        """
        self._readiness[name] = command

    def delete_all(self) -> int:
        """Remove every container this namespace manages. Returns the count."""
        names = self._managed_names()
        for name in names:
            self._run([self.cli, "rm", "-f", name], what=f"remove {name}", check=False)
        return len(names)

    def list_managed(self) -> list[str]:
        """Return unqualified names of managed containers."""
        prefix = f"{self.namespace}-"
        return [n[len(prefix) :] if n.startswith(prefix) else n for n in self._managed_names()]

    def host_port(self, name: str, container_port: int) -> int | None:
        """Return the host port mapped to a container port, if any."""
        result = subprocess.run(  # noqa: S603
            [self.cli, "inspect", self._qualified(name), "--format", "{{json .NetworkSettings}}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return None
        try:
            ports = (json.loads(result.stdout) or {}).get("Ports") or {}
            for key, bindings in ports.items():
                if key.startswith(f"{container_port}/") and bindings:
                    return int(bindings[0]["HostPort"])
        except (json.JSONDecodeError, KeyError, ValueError, TypeError, IndexError):
            return None
        return None

    # -- internals ----------------------------------------------------------

    def _qualified(self, name: str) -> str:
        return f"{self.namespace}-{name}"

    def _image_of(self, qualified: str) -> str:
        result = subprocess.run(  # noqa: S603
            [self.cli, "inspect", qualified, "--format", "{{.Config.Image}}"],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.stdout.strip() if result.returncode == 0 else ""

    def _is_running(self, qualified: str) -> bool:
        result = subprocess.run(  # noqa: S603
            [self.cli, "inspect", qualified, "--format", "{{.State.Running}}"],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode == 0 and result.stdout.strip() == "true"

    def _managed_names(self) -> list[str]:
        result = subprocess.run(  # noqa: S603
            [
                self.cli,
                "ps",
                "-a",
                "--filter",
                f"label=lakebench.namespace={self.namespace}",
                "--format",
                "{{.Names}}",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return []
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    def _run(self, cmd: list[str], what: str, check: bool = True) -> str:
        logger.debug("Running: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)  # noqa: S603
        if check and result.returncode != 0:
            raise ContainerRuntimeError(
                f"Failed to {what}: {(result.stderr or result.stdout).strip()}"
            )
        return result.stdout.strip()
