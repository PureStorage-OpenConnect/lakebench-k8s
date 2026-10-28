"""Capture streaming SparkApplication driver logs across pod rotation.

When a streaming driver pod fails inside the window, the Spark Operator deletes
the pod as it transitions the SparkApplication through FAILING->PENDING_RERUN.
``kubectl logs <pod>`` after that returns ``pod not found``, so the diagnostic
log for the first (failing) driver is lost: the gate refuses the window, but no
evidence of why survives.

This module runs ``kubectl logs -f --timestamps`` for each streaming driver
pod as soon as the pod enters ``Running`` and writes to a file inside the run
directory. If the pod is replaced (resubmission after a crash), the capture
loop detects the new pod and starts a second file with a rotation suffix.
Everything is best-effort: a missing kubectl, an RBAC error or a pod that
never reaches Running does NOT fail the pipeline; it only means less evidence
if a driver fails.

Interface:

    cap = DriverLogCapturer(namespace, run_dir, context=cfg.platform.kubernetes.context or None)
    cap.watch(["lakebench-silver-stream", "lakebench-bronze-ingest"])
    # ... run the pipeline ...
    cap.close()  # best-effort shutdown, non-blocking beyond a short deadline

The output path per pod is ``<run_dir>/drivers/<app>.driver.log`` (first pod)
and ``<run_dir>/drivers/<app>.driver.rN.log`` for restarts N=1,2,... .

Call ``close()`` from ``finally:`` of ``_stop_streams``; it never raises.
"""

from __future__ import annotations

import logging
import re
import subprocess
import threading
from pathlib import Path
from typing import Any

from lakebench.k8s._pinned import pinned_kubectl, pinned_kubectl_popen

logger = logging.getLogger(__name__)

# How often to poll for new/replaced driver pods. 2s is slow enough not to
# spam the API server, fast enough that a resubmit (which takes ~5-10s for
# the operator to recreate the pod) is caught.
_POLL_INTERVAL_S = 2.0

# How long to wait for a terminating kubectl subprocess to exit before SIGKILL.
_TERMINATE_GRACE_S = 2.0

# The RFC 3339 stamp ``kubectl logs --timestamps`` puts before each line.
_STAMP = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d) ")

# Lines matched at the end of the kept copy to find where the live log
# continues it (a single line can repeat).
_OVERLAP_LINES = 3


def merge_logs(kept: str | None, live: str | None) -> str | None:
    """The driver's whole log from *kept* (the capture since the pod
    started) and *live* (the pod log now, which kubelet rotation trims at
    the front): the kept lines, then the live lines after the last ones the
    two share. With no shared lines the live log is appended whole."""
    if not kept:
        return live
    if not live:
        return kept
    k = kept.splitlines()
    v = live.splitlines()
    tail = k[-_OVERLAP_LINES:]
    for i in range(len(v) - len(tail), -1, -1):
        if v[i : i + len(tail)] == tail:
            rest = v[i + len(tail) :]
            return "\n".join(k + rest) + "\n"
    if "\n".join(v) in kept:
        return kept
    return "\n".join(k + v) + "\n"


class DriverLogCapturer:
    """Watches streaming driver pods and tails their logs into run_dir/drivers/.

    Thread-safe: ``watch`` starts a background poller thread, ``close`` stops
    it and all its spawned ``kubectl logs`` subprocesses. Idempotent: calling
    ``close`` twice is safe, calling ``watch`` twice replaces the pod list.
    """

    def __init__(self, namespace: str, run_dir: Path, *, context: Any = None):
        self._namespace = namespace
        self._context = context
        self._drivers_dir = Path(run_dir) / "drivers"
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # Map "<app_name>" -> list of (pod_uid, Popen, Path). A new pod_uid
        # on an existing app name is a resubmit: a new entry is appended.
        self._captures: dict[str, list[tuple[str, subprocess.Popen, Path]]] = {}
        self._apps: list[str] = []
        self._lock = threading.Lock()

    def watch(self, app_names: list[str]) -> None:
        """Begin watching the named SparkApplication driver pods.

        Starts a background thread that polls every ``_POLL_INTERVAL_S``.
        Safe to call before any pod exists; the loop waits until one does.
        """
        with self._lock:
            self._apps = list(app_names)
        if self._thread is not None and self._thread.is_alive():
            return
        try:
            self._drivers_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.warning("driver log capture: cannot create %s: %s", self._drivers_dir, e)
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._poll_loop, name="lb-driver-log-capturer", daemon=True
        )
        self._thread.start()

    def text(self, app: str) -> str | None:
        """The kept log of *app*'s current driver pod (its newest capture),
        without the ``--timestamps`` stamps; None when none was kept."""
        with self._lock:
            captures = list(self._captures.get(app, []))
        if not captures:
            return None
        try:
            raw = captures[-1][2].read_text(errors="replace")
        except OSError:
            return None
        return "".join(_STAMP.sub("", line, count=1) for line in raw.splitlines(keepends=True))

    def drain(self, app: str, timeout: float) -> None:
        """Wait up to *timeout* seconds for *app*'s newest tail to end.

        ``kubectl logs -f`` exits by itself when the container does, after
        streaming what was written; reading the file before that can miss the
        driver's last lines. Returns at once when no tail was started.
        """
        with self._lock:
            captures = list(self._captures.get(app, []))
        if not captures:
            return
        try:
            captures[-1][1].wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            pass

    def close(self) -> None:
        """Stop watching and terminate every kubectl log process.

        Also snapshots ``kubectl describe pod`` and ``kubectl get events``
        for each captured driver pod into ``<run_dir>/drivers/`` so that
        container-level kills (OOMKilled -- exit 137 with no final log line)
        are diagnosable; stdout alone misses them because the SIGKILL leaves
        no "shutting down" trail (M2 in review).

        Never raises. Returns within ~7s even if a subprocess is unresponsive.
        """
        self._stop.set()
        # First join: give the poller a chance to notice _stop between polls
        # and before any Popen it's about to spawn (poller re-checks _stop
        # before and after Popen; a late-spawned child is self-terminated).
        if self._thread is not None:
            self._thread.join(timeout=_POLL_INTERVAL_S + 1.0)
        # Snapshot everything the poller recorded under the lock so a
        # straggler Popen that reached the final append does not escape.
        with self._lock:
            captures = [c for cs in self._captures.values() for c in cs]
            captured_apps = {app: list(cs) for app, cs in self._captures.items()}
            self._captures.clear()
        # Save a describe + events snapshot for each captured pod BEFORE
        # terminating kubectl logs: a container-level kill leaves no final
        # log line, but the pod spec and events record the terminated state.
        for app, cs in captured_apps.items():
            for i, (_uid, _proc, _path) in enumerate(cs):
                self._snapshot_pod(app, i, _uid)
        for _uid, proc, _path in captures:
            self._terminate(proc)
        # Second join: a straggler poller that was wedged inside kubectl
        # above its own 10s inner timeout now has nothing left to append to
        # (captures is cleared and the poller re-checks _stop) and will exit
        # on the next re-check. Bounded to not hang shutdown.
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _snapshot_pod(self, app: str, index: int, pod_uid: str) -> None:
        """Save ``kubectl describe pod`` and recent events for the pod uid.

        Best-effort: a missing pod (already deleted) or RBAC error is logged
        at debug and ignored. The output files are named for the capture
        index (0 = first driver, 1 = first resubmit, ...) so they line up
        with the ``.driver.log`` / ``.driver.r1.log`` naming.
        """
        suffix = "" if index == 0 else f".r{index}"
        describe_path = self._drivers_dir / f"{app}.driver{suffix}.describe.txt"
        events_path = self._drivers_dir / f"{app}.driver{suffix}.events.txt"
        # Look the pod up again by uid to be sure we're describing the right
        # one even if a redeploy reused the name; fall back to label match.
        pod_name = self._pod_name_by_uid(app, pod_uid)
        if not pod_name:
            return
        self._write_kubectl_output(
            ["describe", "pod", "-n", self._namespace, pod_name], describe_path
        )
        self._write_kubectl_output(
            [
                "get",
                "events",
                "-n",
                self._namespace,
                "--field-selector",
                f"involvedObject.name={pod_name}",
                "-o",
                "yaml",
            ],
            events_path,
        )

    def _pod_name_by_uid(self, app: str, pod_uid: str) -> str | None:
        try:
            result = pinned_kubectl(
                self._context,
                [
                    "get",
                    "pods",
                    "-n",
                    self._namespace,
                    "-l",
                    f"sparkoperator.k8s.io/app-name={app},spark-role=driver",
                    "-o",
                    'jsonpath={range .items[*]}{.metadata.name}|{.metadata.uid}{"\\n"}{end}',
                    "--request-timeout=5s",
                ],
                timeout=10,
                check=False,
                capture_output=True,
                text=True,
            )
        except Exception as e:  # noqa: BLE001
            logger.debug("driver log capture: pod lookup for snapshot failed: %s", e)
            return None
        if result.returncode != 0:
            return None
        for line in result.stdout.splitlines():
            parts = line.split("|")
            if len(parts) == 2 and parts[1] == pod_uid:
                return parts[0]
        return None

    def _write_kubectl_output(self, args: list[str], path: Path) -> None:
        try:
            result = pinned_kubectl(
                self._context,
                args + ["--request-timeout=5s"],
                timeout=10,
                check=False,
                capture_output=True,
                text=True,
            )
        except Exception as e:  # noqa: BLE001
            logger.debug("driver log capture: snapshot kubectl failed for %s: %s", path.name, e)
            return
        try:
            path.write_text(result.stdout or "")
        except OSError as e:
            logger.warning("driver log capture: cannot write %s: %s", path, e)

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._poll_once()
            except Exception as e:  # noqa: BLE001  (never fail the pipeline)
                logger.warning("driver log capture: poll error: %s", e)
            self._stop.wait(_POLL_INTERVAL_S)

    def _poll_once(self) -> None:
        with self._lock:
            apps = list(self._apps)
        for app in apps:
            # Honour an in-flight close() between apps as well as between
            # pods: a close() that lands while we are iterating must not
            # leak a kubectl spawned after it (M3 in review).
            if self._stop.is_set():
                return
            pod_name, pod_uid, phase = self._get_driver_pod(app)
            if not pod_name or not pod_uid:
                continue
            if phase and phase not in ("Running", "Succeeded", "Failed"):
                continue
            # If we already captured THIS uid, the tail for it may still be
            # live, or it may have died while the pod kept running: a
            # ``kubectl logs -f`` is severed at the kubelet's
            # streamingConnectionIdleTimeout (4h on this cluster), after which
            # nothing restarts it and the file stops at ~4h of a long window
            # (the 18h-blind-record bug). Restart a severed tail in place with
            # --since-time so the file is continuous; a still-live tail is left
            # alone. A new uid is a resubmit and starts a new file below.
            with self._lock:
                existing = self._captures.get(app, [])
                known = next(
                    ((i, p) for i, (u, p, _f) in enumerate(existing) if u == pod_uid),
                    None,
                )
            if known is not None:
                idx, known_proc = known
                if known_proc.poll() is None:
                    continue  # tail still live
                if phase == "Running":
                    self._resume_tail(app, idx, pod_name)
                continue
            with self._lock:
                existing = self._captures.get(app, [])
                suffix = "" if not existing else f".r{len(existing)}"
            path = self._drivers_dir / f"{app}.driver{suffix}.log"
            # Final check right before the Popen: close() may have landed
            # between the lock release above and this line. Without this,
            # close() can return before the thread spawns the kubectl, and
            # the spawned child orphans.
            if self._stop.is_set():
                return
            proc = self._start_tail(pod_name, path)
            if proc is not None:
                with self._lock:
                    if self._stop.is_set():
                        # close() landed between the Popen and this append;
                        # terminate the just-spawned child ourselves rather
                        # than hand it to the shutdown drain, which has
                        # already read the captures dict.
                        self._terminate(proc)
                        return
                    self._captures.setdefault(app, []).append((pod_uid, proc, path))

    def _get_driver_pod(self, app: str) -> tuple[str | None, str | None, str | None]:
        """Return ``(name, uid, phase)`` of the driver pod for *app*, or Nones."""
        # jsonpath-returns-first behaviour: pick by label match; the operator
        # labels its driver pod with sparkoperator.k8s.io/app-name=<app>.
        try:
            result = pinned_kubectl(
                self._context,
                [
                    "get",
                    "pods",
                    "-n",
                    self._namespace,
                    "-l",
                    f"sparkoperator.k8s.io/app-name={app},spark-role=driver",
                    "-o",
                    'jsonpath={range .items[*]}{.metadata.name}|{.metadata.uid}|{.status.phase}{"\\n"}{end}',
                    "--request-timeout=5s",
                ],
                timeout=10,
                check=False,
                capture_output=True,
                text=True,
            )
        except Exception as e:  # noqa: BLE001
            logger.debug("driver log capture: kubectl get failed for %s: %s", app, e)
            return (None, None, None)
        if result.returncode != 0:
            return (None, None, None)
        # Pick the newest-looking pod (last line, since the operator creates
        # fresh ones on top of deleted ones; UID differs across generations).
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        if not lines:
            return (None, None, None)
        parts = lines[-1].split("|")
        if len(parts) != 3:
            return (None, None, None)
        return (parts[0] or None, parts[1] or None, parts[2] or None)

    def _resume_tail(self, app: str, idx: int, pod_name: str) -> None:
        """Restart a tail that the kubelet severed while the pod kept running.

        ``kubectl logs -f`` is cut at the kubelet streamingConnectionIdleTimeout
        (4h here). The capture file then stops mid-window with no restart, so a
        24h record is built from ~4h of logs (ingest ratio read 0.24 on a run
        where bronze kept up). Resume by appending a fresh ``-f`` from the last
        timestamp already in the file (``--since-time``), so the file stays one
        continuous log per pod. Best-effort: a failure leaves the file as-is.
        """
        with self._lock:
            entry = self._captures.get(app, [])
            if idx >= len(entry):
                return
            _uid, _old, path = entry[idx]
        since = self._last_stamp(path)
        proc = self._start_tail(pod_name, path, since=since, append=True)
        if proc is None:
            return
        with self._lock:
            entry = self._captures.get(app, [])
            if idx < len(entry) and entry[idx][2] == path:
                entry[idx] = (entry[idx][0], proc, path)
            else:
                # close() cleared captures between the Popen and here; do not
                # leak the child into a drain that has already run.
                self._terminate(proc)

    @staticmethod
    def _last_stamp(path: Path) -> str | None:
        """The RFC3339 timestamp of the last ``--timestamps`` line in *path*,
        for ``kubectl logs --since-time``. None if the file has no stamped
        line (then the resumed tail falls back to the whole available log)."""
        try:
            raw = path.read_text(errors="replace")
        except OSError:
            return None
        for line in reversed(raw.splitlines()):
            if _STAMP.match(line):
                return line.split(" ", 1)[0]
        return None

    def _start_tail(
        self, pod_name: str, path: Path, *, since: str | None = None, append: bool = False
    ) -> subprocess.Popen | None:
        try:
            # Open the file ourselves so Popen inherits a real fd (not a pipe
            # we would need to drain). --timestamps so a FAIL log is directly
            # comparable to run.log timestamps. append=True resumes a severed
            # tail without truncating what was already captured.
            fh = open(path, "ab" if append else "wb")  # noqa: SIM115
        except OSError as e:
            logger.warning("driver log capture: cannot open %s: %s", path, e)
            return None
        args = [
            "logs",
            "-f",
            "--timestamps",
            "-n",
            self._namespace,
            pod_name,
        ]
        if since is not None:
            # --since-time has 1s granularity, so the seam may repeat up to a
            # second of lines. Downstream consumers key on distinct batch
            # numbers / stamps, so a few repeated lines are harmless; a gap
            # would not be. Prefer the small overlap.
            args += [f"--since-time={since}"]
        try:
            proc = pinned_kubectl_popen(
                self._context,
                args,
                stdout=fh,
                stderr=subprocess.STDOUT,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("driver log capture: cannot start kubectl logs for %s: %s", pod_name, e)
            try:
                fh.close()
            except OSError:
                pass
            return None
        # Popen inherits the fd; we can close our handle without losing writes.
        try:
            fh.close()
        except OSError:
            pass
        return proc

    @staticmethod
    def _terminate(proc: subprocess.Popen) -> None:
        """Best-effort termination of a kubectl logs process."""
        if proc.poll() is not None:
            return
        try:
            proc.terminate()
            try:
                proc.wait(timeout=_TERMINATE_GRACE_S)
                return
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    proc.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    pass
        except Exception as e:  # noqa: BLE001
            logger.debug("driver log capture: terminate error: %s", e)


# How long a finished stage's tail may take to stream the driver's last lines.
_DRAIN_S = 15.0


def start_stage_capture(
    namespace: str, run_dir: Path, app: str, *, context: Any = None
) -> DriverLogCapturer | None:
    """Start following *app*'s driver log into ``run_dir/drivers/``, for a
    batch stage. Best-effort: None when the capture could not start, and the
    stage then keeps the pod log alone."""
    try:
        capturer = DriverLogCapturer(namespace, run_dir, context=context)
        capturer.watch([app])
        return capturer
    except Exception as e:  # noqa: BLE001 -- a missing capture never fails the stage
        logger.warning("driver log capture: could not start for %s: %s", app, e)
        return None


def finish_stage_capture(
    capturer: DriverLogCapturer | None, app: str, live: str | None
) -> str | None:
    """The stage's whole driver log: the capture kept since the pod started,
    joined to *live* (the pod log read after the job ended, which kubelet
    rotation trims at the front once the driver has written enough). Stops the
    capture. Never raises; with no capture it returns *live*."""
    if capturer is None:
        return live
    try:
        capturer.drain(app, _DRAIN_S)
        kept = capturer.text(app)
    except Exception as e:  # noqa: BLE001
        logger.warning("driver log capture: could not read the kept log of %s: %s", app, e)
        kept = None
    capturer.close()
    return merge_logs(kept, live)


__all__ = ["DriverLogCapturer", "finish_stage_capture", "start_stage_capture"]
