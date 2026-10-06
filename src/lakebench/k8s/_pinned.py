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

Empty and ``None`` both mean "no configured context": the flag then names
the context of the process's active ``ClusterTarget`` (``k8s/target.py``),
and is left off only when no target is active.
"""

from __future__ import annotations

import logging
import os
import re
import signal
import subprocess
from typing import Any

from lakebench.k8s.lease_state import LeaseHoldExceeded, lease_held, lease_hold_remaining

logger = logging.getLogger(__name__)


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
    """Assemble the argv for ``tool`` with ``--context``/``--kube-context`` pinned.

    With no configured context the flag comes from the process's active
    :class:`~lakebench.k8s.target.ClusterTarget`, so a subprocess
    and the API clients use the same context even when the kubeconfig's
    current context changes mid-run.
    """
    from lakebench.k8s.target import cli_args

    return [tool, *cli_args(tool, _resolve_context(cfg_or_context)), *args]


# helm's own --timeout is this much shorter than the subprocess timeout, so
# helm gives up (and records the failure) before Python ever stops it.
HELM_TIMEOUT_MARGIN_S = 30
# Kept back from a mutating helm call's subprocess timeout for the graceful
# stop below and a later status-and-rollback step (DESIGN ch01 3.7).
HELM_RECOVERY_RESERVE_S = 60
# A mutating helm call is not started with less than this left (3.7).
HELM_MIN_START_S = 60
# On a timeout or an abort the child gets SIGTERM and this long to stop:
# helm cancels the operation and marks the release failed, rather than the
# pending-upgrade a SIGKILL leaves.
TERM_GRACE_S = 20
_HELM_DEFAULT_TIMEOUT_S = 300
_HELM_MUTATING_VERBS = frozenset({"install", "upgrade", "uninstall", "delete", "rollback"})
_HELM_GLOBAL_VALUE_FLAGS = frozenset({"-n", "--namespace", "--kube-context", "--kubeconfig"})
# Flags whose next word is a value (so a value is never read as a flag).
_HELM_VALUE_FLAGS = _HELM_GLOBAL_VALUE_FLAGS | frozenset(
    {"--set", "--set-string", "--set-file", "--set-json", "--set-literal", "-f", "--values"}
)


class LeasedCommandTimeout(subprocess.TimeoutExpired):
    """A kubectl/helm/oc call made under the cluster lease ran out of time."""

    def __str__(self) -> str:
        cmd = self.cmd if isinstance(self.cmd, str) else " ".join(map(str, self.cmd))
        text = (
            f"{cmd!r} did not finish within {self.timeout:.0f} s while holding the "
            "cluster lease and was stopped"
        )
        if "helm" in str(self.cmd):
            text += (
                "; the release may be left failed or pending-upgrade: run `lakebench "
                "admin repair-operator`, which rolls a stale pending release back when "
                "the revision is safe (a `helm rollback` by hand skips that check and "
                "the lease)"
            )
        return text


_DURATION_RE = re.compile(r"(\d+(?:\.\d+)?)(h|ms|m|s)")


def _helm_duration_s(text: str) -> float | None:
    """Seconds in a helm/Go duration (``5m``, ``300s``, ``1h2m``); None if unparseable."""
    text = text.strip()
    if text.isdigit():
        return float(text)
    parts = _DURATION_RE.findall(text)
    if not parts or "".join(n + u for n, u in parts) != text:
        return None
    scale = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 0.001}
    return sum(float(n) * scale[u] for n, u in parts)


def _bound_helm_timeout(args: list[str], limit_s: int) -> list[str]:
    """``args`` with helm's ``--timeout`` at most ``limit_s`` seconds.

    Only the flag itself is touched (``--timeout X`` or ``--timeout=X``,
    never a ``--set`` value). Without one, helm's own 300 s default is kept
    when it fits.
    """
    out = list(args)
    i = 0
    while i < len(out):
        a = out[i]
        if a in _HELM_VALUE_FLAGS:
            i += 2  # the next word is that flag's value, even if it looks like a flag
            continue
        if a == "--timeout" and i + 1 < len(out):
            cur = _helm_duration_s(out[i + 1])
            if cur is None or cur > limit_s:
                out[i + 1] = f"{limit_s}s"
            return out
        if a.startswith("--timeout="):
            cur = _helm_duration_s(a.split("=", 1)[1])
            if cur is None or cur > limit_s:
                out[i] = f"--timeout={limit_s}s"
            return out
        i += 1
    return [*out, "--timeout", f"{min(limit_s, _HELM_DEFAULT_TIMEOUT_S)}s"]


def _helm_verb(args: list[str]) -> str:
    """The helm subcommand: the first word that is not a global flag or its value."""
    skip = False
    for a in args:
        if skip:
            skip = False
            continue
        if a in _HELM_GLOBAL_VALUE_FLAGS:
            skip = True
            continue
        if a.startswith("-"):
            continue
        return a
    return ""


def _is_dry_run(args: list[str]) -> bool:
    for a in args:
        if a == "--dry-run":
            return True
        if a.startswith("--dry-run="):
            return a.split("=", 1)[1].lower() not in ("none", "false")
    return False


def _under_lease(tool: str, args: list[str], kwargs: dict[str, Any]) -> tuple[list[str], bool]:
    """Apply the lease rules to one call; returns the args and whether they applied.

    Under the cluster lease the child runs in its own session (a terminal
    Ctrl-C goes to the foreground process group and would otherwise kill a
    helm upgrade half way, DESIGN ch01 3.6) with a subprocess timeout from
    the hold budget. A mutating helm call keeps ``HELM_RECOVERY_RESERVE_S``
    of the budget back, is refused with ``LeaseHoldExceeded`` when less than
    ``HELM_MIN_START_S`` would be left for it, and gets its own ``--timeout``
    ``HELM_TIMEOUT_MARGIN_S`` shorter. Any call is refused once the budget
    is spent. A caller's ``start_new_session`` and a shorter caller timeout
    are kept.
    """
    remaining = lease_hold_remaining()
    if remaining is None:
        return args, False
    mutating_helm = (
        tool == "helm" and _helm_verb(args) in _HELM_MUTATING_VERBS and not _is_dry_run(args)
    )
    budget = remaining - (HELM_RECOVERY_RESERVE_S if mutating_helm else 0)
    if remaining <= 0 or (mutating_helm and budget < HELM_MIN_START_S):
        raise LeaseHoldExceeded(
            f"{tool} {' '.join(args[:3])}: {max(remaining, 0):.0f} s of the cluster lease "
            "hold budget left; not started"
        )
    kwargs.setdefault("start_new_session", True)
    given = kwargs.get("timeout")
    timeout = budget if given is None else min(float(given), budget)
    kwargs["timeout"] = timeout
    if mutating_helm:
        args = _bound_helm_timeout(args, max(1, int(timeout) - HELM_TIMEOUT_MARGIN_S))
    return args, True


# After SIGKILL, how long to collect what the child printed. A grandchild
# holding the pipes open (an exec credential plugin, a post-renderer) must
# not keep the lease waiting.
_DRAIN_S = 5


def _signal_child(proc: subprocess.Popen[Any], sig: int) -> None:
    """Signal the child's own process group when it leads one, else the child."""
    try:
        pid = getattr(proc, "pid", 0) or 0
        if pid > 0 and hasattr(os, "killpg") and os.getpgid(pid) == pid:
            os.killpg(pid, sig)
        else:
            proc.send_signal(sig)
    except (OSError, ValueError):
        pass  # already gone


def _stop_gently(proc: subprocess.Popen[Any]) -> tuple[Any, Any]:
    """SIGTERM, ``TERM_GRACE_S`` to stop, then SIGKILL; returns what it printed.

    Bounded: at most ``TERM_GRACE_S + _DRAIN_S`` seconds. An abort during
    the grace sends SIGKILL at once and re-raises.
    """
    _signal_child(proc, signal.SIGTERM)
    try:
        return proc.communicate(timeout=TERM_GRACE_S)
    except subprocess.TimeoutExpired:
        pass
    except BaseException:
        # An abort (the third interrupt) during the grace: do not leave the
        # child running, or Popen.__exit__ waits for it without a bound.
        _signal_child(proc, getattr(signal, "SIGKILL", signal.SIGTERM))
        raise
    _signal_child(proc, getattr(signal, "SIGKILL", signal.SIGTERM))
    try:
        return proc.communicate(timeout=_DRAIN_S)
    except subprocess.TimeoutExpired:
        for pipe in (proc.stdout, proc.stderr, proc.stdin):
            try:
                if pipe is not None:
                    pipe.close()
            except OSError:
                pass
        return None, None


def _run_leased(argv: list[str], kwargs: dict[str, Any]) -> subprocess.CompletedProcess[Any]:
    """``subprocess.run`` for the lease, stopping the child gently.

    ``subprocess.run`` SIGKILLs the child on a timeout or an interrupt,
    which leaves a helm release pending-upgrade; here the child gets SIGTERM
    first (``_stop_gently``).
    """
    kwargs = dict(kwargs)
    timeout = kwargs.pop("timeout")
    check = kwargs.pop("check", False)
    data = kwargs.pop("input", None)
    if kwargs.pop("capture_output", False):
        kwargs["stdout"] = subprocess.PIPE
        kwargs["stderr"] = subprocess.PIPE
    if data is not None:
        kwargs["stdin"] = subprocess.PIPE
    with subprocess.Popen(argv, **kwargs) as proc:  # noqa: S603
        try:
            out, err = proc.communicate(data, timeout=timeout)
        except subprocess.TimeoutExpired as e:
            out, err = _stop_gently(proc)
            exc = LeasedCommandTimeout(argv, timeout, output=out, stderr=err)
            logger.error("%s", exc)
            raise exc from e
        except BaseException:  # an abort (third interrupt) inside the lease
            _stop_gently(proc)
            raise
        rc = proc.returncode if proc.returncode is not None else proc.wait()
    if check and rc:
        raise subprocess.CalledProcessError(rc, argv, out, err)
    return subprocess.CompletedProcess(argv, rc, out, err)


def _run_pinned(
    tool: str, cfg_or_context: Any, args: list[str], kwargs: dict[str, Any]
) -> subprocess.CompletedProcess[Any]:
    args, leased = _under_lease(tool, list(args), kwargs)
    argv = _pinned_argv(tool, cfg_or_context, args)
    if leased:
        return _run_leased(argv, kwargs)
    return subprocess.run(argv, **kwargs)  # noqa: S603


def pinned_kubectl(
    cfg_or_context: Any,
    args: list[str],
    **kwargs: Any,
) -> subprocess.CompletedProcess[Any]:
    """Run ``kubectl`` with the configured kube-context.

    ``args`` is the argv after the ``kubectl`` executable itself (do not
    include the string ``"kubectl"`` in ``args``). All ``**kwargs`` are
    forwarded to :func:`subprocess.run`; under the cluster lease the call
    also gets a new session and a timeout (``_under_lease``).
    """
    return _run_pinned("kubectl", cfg_or_context, args, kwargs)


def pinned_helm(
    cfg_or_context: Any,
    args: list[str],
    **kwargs: Any,
) -> subprocess.CompletedProcess[Any]:
    """Run ``helm`` with the configured kube-context.

    ``args`` is the argv after the ``helm`` executable itself. Kwargs are
    forwarded to :func:`subprocess.run`. Helm's flag is spelt
    ``--kube-context``, not ``--context``; this helper handles the
    difference so callers do not. Under the cluster lease the call also
    gets a new session, a timeout, and a helm ``--timeout`` 30 s shorter.
    """
    return _run_pinned("helm", cfg_or_context, args, kwargs)


def pinned_oc(
    cfg_or_context: Any,
    args: list[str],
    **kwargs: Any,
) -> subprocess.CompletedProcess[Any]:
    """Run ``oc`` with the configured kube-context.

    OpenShift's ``oc`` accepts the same ``--context=<name>`` flag as
    kubectl. Kwargs are forwarded to :func:`subprocess.run`; under the
    cluster lease the call also gets a new session and a timeout.
    """
    return _run_pinned("oc", cfg_or_context, args, kwargs)


def pinned_kubectl_popen(
    cfg_or_context: Any,
    args: list[str],
    **kwargs: Any,
) -> subprocess.Popen[Any]:
    """Streaming ``kubectl`` (e.g. ``kubectl logs -f``, ``kubectl port-forward``).

    Analogous to :func:`pinned_kubectl` but backed by :class:`subprocess.Popen`
    so callers can read the pipe or terminate the process. The lint expects
    every ``["kubectl", ...]`` Popen argv to go through this helper. Under
    the cluster lease the child starts in its own session (a Popen has no
    timeout; its caller owns its lifetime).
    """
    if lease_held():
        kwargs.setdefault("start_new_session", True)
    return subprocess.Popen(_pinned_argv("kubectl", cfg_or_context, args), **kwargs)  # noqa: S603


__all__ = [
    "HELM_TIMEOUT_MARGIN_S",
    "LeaseHoldExceeded",
    "LeasedCommandTimeout",
    "pinned_helm",
    "pinned_kubectl",
    "pinned_kubectl_popen",
    "pinned_oc",
]
