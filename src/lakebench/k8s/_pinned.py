"""Context-pinned wrappers around kubectl / helm / oc subprocess calls.

Every kubectl/helm/oc invocation in lakebench must run against the cluster
named by ``platform.kubernetes.context`` in the loaded config, not the
ambient current-context of whichever kubeconfig happens to be on ``$PATH``.
A stale or wrong current-context has, in the past, sent a `helm upgrade`
of the shared spark-operator release to a cluster the operator did not
intend, and destroyed resources in the wrong namespace on another.

Rather than sprinkle ``--context=<value>``/``--kube-context=<value>``
literals across the ~30 subprocess sites, callers thread a small
``context`` value through and let these helpers append the correct flag
form. They also give the repo-wide lint one entry point to whitelist,
so a new bare ``subprocess.run(["kubectl", ...])`` fails CI instead of
silently reading the ambient context.

The helpers accept, uniformly:

* a ``LakebenchConfig`` (any object with ``platform.kubernetes.context``),
* a raw ``str`` context name (or ``""`` for "use current context"),
* ``None`` for "no config, use current context".

Empty and ``None`` both mean "do not append the flag" -- kubectl and helm
already fall back to the current context in that case.
"""

from __future__ import annotations

import subprocess
from typing import Any


def _resolve_context(cfg_or_context: Any) -> str | None:
    """Return the configured context string, or None if none is set.

    Accepts a full ``LakebenchConfig`` (duck-typed via
    ``.platform.kubernetes.context``), a plain string, or ``None``.
    An empty string means "no explicit context"; a missing attribute
    chain is treated the same way rather than raising -- calls made
    from paths that never had a config loaded (module-level helpers,
    ad-hoc scripts) must still work.
    """
    if cfg_or_context is None:
        return None
    if isinstance(cfg_or_context, str):
        return cfg_or_context or None
    ctx: Any = cfg_or_context
    for attr in ("platform", "kubernetes", "context"):
        ctx = getattr(ctx, attr, None)
        if ctx is None:
            return None
    if isinstance(ctx, str) and ctx:
        return ctx
    return None


def _pinned_argv(tool: str, cfg_or_context: Any, args: list[str]) -> list[str]:
    """Assemble the argv for ``tool`` with ``--context``/``--kube-context`` pinned."""
    context = _resolve_context(cfg_or_context)
    if not context:
        return [tool, *args]
    if tool == "helm":
        return [tool, "--kube-context", context, *args]
    # kubectl and oc share the same flag spelling.
    return [tool, "--context", context, *args]


def pinned_kubectl(
    cfg_or_context: Any,
    args: list[str],
    **kwargs: Any,
) -> subprocess.CompletedProcess[Any]:
    """Run ``kubectl`` with the configured kube-context.

    ``args`` is the argv after the ``kubectl`` executable itself (do not
    include the string ``"kubectl"`` in ``args``). All ``**kwargs`` are
    forwarded to :func:`subprocess.run` unchanged.
    """
    return subprocess.run(_pinned_argv("kubectl", cfg_or_context, args), **kwargs)  # noqa: S603


def pinned_helm(
    cfg_or_context: Any,
    args: list[str],
    **kwargs: Any,
) -> subprocess.CompletedProcess[Any]:
    """Run ``helm`` with the configured kube-context.

    ``args`` is the argv after the ``helm`` executable itself. Kwargs are
    forwarded to :func:`subprocess.run`. Helm's flag is spelt
    ``--kube-context``, not ``--context``; this helper handles the
    difference so callers do not.
    """
    return subprocess.run(_pinned_argv("helm", cfg_or_context, args), **kwargs)  # noqa: S603


def pinned_oc(
    cfg_or_context: Any,
    args: list[str],
    **kwargs: Any,
) -> subprocess.CompletedProcess[Any]:
    """Run ``oc`` with the configured kube-context.

    OpenShift's ``oc`` accepts the same ``--context=<name>`` flag as
    kubectl. Kwargs are forwarded to :func:`subprocess.run`.
    """
    return subprocess.run(_pinned_argv("oc", cfg_or_context, args), **kwargs)  # noqa: S603


def pinned_kubectl_popen(
    cfg_or_context: Any,
    args: list[str],
    **kwargs: Any,
) -> subprocess.Popen[Any]:
    """Streaming ``kubectl`` (e.g. ``kubectl logs -f``, ``kubectl port-forward``).

    Analogous to :func:`pinned_kubectl` but backed by :class:`subprocess.Popen`
    so callers can read the pipe or terminate the process. The lint expects
    every ``["kubectl", ...]`` Popen argv to go through this helper.
    """
    return subprocess.Popen(_pinned_argv("kubectl", cfg_or_context, args), **kwargs)  # noqa: S603


__all__ = [
    "pinned_helm",
    "pinned_kubectl",
    "pinned_kubectl_popen",
    "pinned_oc",
]
