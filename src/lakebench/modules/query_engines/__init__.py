"""Query engine modules for Lakebench."""

from __future__ import annotations

from lakebench.k8s import pinned_kubectl


def call_args_end(sql: str, start: int) -> int | None:
    """The index just past the ``)`` closing a call whose arguments begin at
    ``start``, or None when the parentheses do not balance."""
    depth = 1
    i = start
    while i < len(sql) and depth > 0:
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
        i += 1
    return i if depth == 0 else None


class PodExecutor:
    """Shared by executors that run each query through ``kubectl exec`` into one pod.

    A subclass names its pod by the ``app.kubernetes.io/component`` label
    (``pod_component``) and a display name for the not-found error
    (``pod_label``).
    """

    pod_component: str
    pod_label: str
    health_timeout = 15

    namespace: str
    kube_context: str | None
    _pod: str | None

    def _kubectl_prefix(self) -> list[str]:
        """The ``kubectl`` argv prefix with the configured context pinned.

        With no configured context, the process's active cluster target
        supplies it (``k8s/target.py``), so a current-context switch
        mid-run does not move the queries.
        """
        from lakebench.k8s.target import cli_args

        return ["kubectl", *cli_args("kubectl", self.kube_context)]

    def _discover_pod(self) -> str:
        """Find the engine's pod, once."""
        if self._pod:
            return self._pod
        result = pinned_kubectl(
            self.kube_context,
            [
                "get",
                "pods",
                "-n",
                self.namespace,
                "-l",
                f"app.kubernetes.io/component={self.pod_component}",
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        pod = result.stdout.strip()
        if not pod:
            raise RuntimeError(f"No {self.pod_label} pod found in namespace {self.namespace}")
        self._pod = pod
        return pod

    def health_check(self) -> bool:
        try:
            return self.execute_query("SELECT 1", timeout=self.health_timeout).success  # type: ignore[attr-defined]
        except Exception:
            return False
