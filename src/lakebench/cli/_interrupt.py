"""Ctrl-C and SIGTERM for ``lakebench run``: seal the record, stop this run's jobs.

``run`` and the continuous runner each make one :class:`RunInterrupt` before
their main ``try``. It does three things:

- **Signals.** SIGINT and SIGTERM get a handler that raises
  ``KeyboardInterrupt`` on the first signal, so the run's ``except
  KeyboardInterrupt`` branch seals the record INTERRUPTED. From then on, and
  for the whole of the run's ``finally`` (:meth:`RunInterrupt.begin_seal`),
  a signal no longer raises outside the cleanup: the record is still
  written. A second signal during the cleanup stops it (the rest is
  recorded as skipped); a third restores the previous handlers and raises
  at once. Inside the cluster lease (``deploy.cluster_lock``) these handlers
  are saved, the signal is held back until the shared change has finished
  and the lease is released, and is then delivered to them; nothing here
  runs under the lease.
- **Ownership.** Every SparkApplication the run submits is registered with
  the uid in the API server's create response, and every datagen Job with
  the uid read back right after its create. On an interrupt each one the run
  has not seen finish is deleted with that uid as a precondition, so an
  object of the same name created since by another invocation is never
  deleted: the API server answers 409 and it is left.
- **Record.** :meth:`RunInterrupt.seal` returns the ``interrupted`` block of
  the run record: ``{signal, at_stage, at_utc, prior_failure, stopped, left,
  skipped}``.

SIGHUP (a closed terminal) is not handled: the process ends as before, and
a run that must survive a dropped session runs under ``nohup`` or ``tmux``.
SIGKILL and a lost host are out of reach. A later run that submits the same
stage, or deploys datagen, deletes a left object by name first.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from lakebench._clock import utc_now

logger = logging.getLogger(__name__)

#: The signals ``run`` handles.
INTERRUPT_SIGNALS: tuple[int, ...] = (signal.SIGINT, signal.SIGTERM)

SPARK_APPLICATION = "SparkApplication"
JOB = "Job"
DATAGEN_JOB_NAME = "lakebench-datagen"
#: The SparkApplication API, as ``SparkJobManager.submit_job`` creates them.
SPARK_GROUP = "sparkoperator.k8s.io"
SPARK_VERSION = "v1beta2"
SPARK_PLURAL = "sparkapplications"

#: Seconds the whole cleanup may take. Each API call's read timeout is cut
#: to what is left of it, and the cleanup's client does not retry, so an
#: unreachable API server costs at most about this long.
CLEANUP_DEADLINE_S = 60.0
#: Connect and read timeout of one cleanup call (the read part is capped by
#: the deadline).
CLEANUP_CONNECT_TIMEOUT_S = 5.0
CLEANUP_READ_TIMEOUT_S = 15.0

_INTERRUPTED_CREATE = (
    "uid unknown: interrupted while it was being created; a later run deletes it by name"
)
_NOT_PROVEN = "exists, but this run cannot show it created it (no matching LB_RUN_ID)"

_Handler = Callable[[int, Any], Any] | int | None


@dataclass
class Owned:
    """One object this run created: kind, name, the uid the API server gave it."""

    kind: str
    name: str
    uid: str | None = None
    #: The run saw it finish (a stage that completed): kept, not deleted.
    finished: bool = False
    #: Why the uid is unknown, when it is.
    note: str = _INTERRUPTED_CREATE

    @property
    def key(self) -> str:
        return f"{self.kind}/{self.name}"


def signal_name(signum: int) -> str:
    try:
        return signal.Signals(signum).name
    except ValueError:
        return str(signum)


class RunInterrupt:
    """Signals, the objects this run owns, and the interrupt record (module docstring)."""

    def __init__(self, namespace: str, run_id: str = "") -> None:
        self.namespace = namespace
        self.run_id = run_id
        self.owned: dict[str, Owned] = {}
        #: Every signal received, by name, in order.
        self.received: list[str] = []
        #: Set by the first interrupt and for the run's finally: a signal
        #: then does not raise outside the cleanup.
        self.sealing = False
        #: A second signal while sealing: the rest of the cleanup is skipped.
        self.skip = False
        self._in_cleanup = False
        self._saved: dict[int, _Handler] = {}

    # -- signals ------------------------------------------------------------

    def install(self) -> None:
        """Install the handlers (main thread only; an ignored signal stays ignored).

        A handler an earlier run left installed (an error inside its finally
        skipped :meth:`restore`) is replaced, not chained: this run saves the
        handler that one had saved.
        """
        if threading.current_thread() is not threading.main_thread():
            logger.debug("run: not on the main thread; SIGTERM is not handled")
            return
        for sig in INTERRUPT_SIGNALS:
            try:
                previous = signal.getsignal(sig)
                owner = getattr(previous, "__self__", None)
                if isinstance(owner, RunInterrupt) and owner is not self:
                    previous = owner._saved.get(sig, signal.SIG_DFL)
                if previous == signal.SIG_IGN:
                    continue
                self._saved[sig] = previous
                signal.signal(sig, self._on_signal)
            except (ValueError, OSError) as e:  # not the main interpreter
                logger.debug("run: cannot handle %s: %s", sig, e)

    def restore(self) -> None:
        """Put the handlers that were there before :meth:`install` back."""
        for sig, handler in self._saved.items():
            try:
                signal.signal(sig, handler if handler is not None else signal.SIG_DFL)
            except (ValueError, OSError, TypeError) as e:
                logger.debug("run: could not restore the handler for %s: %s", sig, e)
        self._saved.clear()

    def _on_signal(self, signum: int, _frame: Any) -> None:
        self.received.append(signal_name(signum))
        if not self.sealing:
            # Sealing from here on, so a second signal landing before the
            # except branch starts cannot escape it.
            self.sealing = True
            raise KeyboardInterrupt
        if not self.skip:
            self.skip = True
            if self._in_cleanup:
                # Cut the delete in flight; stop_owned records the rest.
                raise KeyboardInterrupt
            _say(
                "Interrupt received; the run record is still being written. "
                "Interrupt once more to stop now (the record may be lost)."
            )
            return
        self.restore()
        raise KeyboardInterrupt

    def begin_seal(self) -> None:
        """From the run's except branch or finally: signals no longer raise
        (outside the cleanup) until the third."""
        self.sealing = True

    def late_signal(self) -> bool:
        """A signal arrived during the run's finally, after it began to seal
        a run that was not interrupted."""
        return bool(self.received)

    def interrupt_signal(self, exc: BaseException | None) -> str:
        """The signal that stopped the run.

        ``LeaseAbort`` (the third signal inside the cluster lease, or one
        during its acquire) carries its own; otherwise the first signal this
        handler saw; otherwise SIGINT (Python's own handler, when this one
        could not be installed).
        """
        signum = getattr(exc, "signum", None)
        if isinstance(signum, int):
            return signal_name(signum)
        return self.received[0] if self.received else "SIGINT"

    # -- ownership ----------------------------------------------------------

    def creating(self, kind: str, name: str) -> None:
        """Before a create: an interrupt inside it leaves the object named."""
        entry = Owned(kind, name)
        self.owned.pop(entry.key, None)  # re-created: newest last
        self.owned[entry.key] = entry

    def created(self, kind: str, name: str, uid: str | None, note: str = "") -> None:
        entry = self.owned.get(f"{kind}/{name}")
        if entry is None:
            entry = Owned(kind, name)
            self.owned[entry.key] = entry
        entry.uid = uid or None
        entry.finished = False
        if note:
            entry.note = note

    def not_created(self, kind: str, name: str) -> None:
        """The create failed: nothing of this run's to stop."""
        self.owned.pop(f"{kind}/{name}", None)

    def submitted(self, status: Any) -> None:
        """After ``submit_job``: register its SparkApplication, or drop a failed submit."""
        from lakebench.spark.job import JobState

        name = getattr(status, "name", "")
        if getattr(status, "state", None) == JobState.FAILED:
            self.not_created(SPARK_APPLICATION, name)
            return
        self.created(SPARK_APPLICATION, name, getattr(status, "uid", None))

    def datagen_created(self) -> None:
        """After a successful datagen deploy: the Job's uid, read back by name."""
        uid = None
        try:
            from kubernetes import client as k8s_client

            job = k8s_client.BatchV1Api().read_namespaced_job(
                DATAGEN_JOB_NAME,
                self.namespace,
                _request_timeout=(CLEANUP_CONNECT_TIMEOUT_S, CLEANUP_READ_TIMEOUT_S),
            )
            uid = getattr(getattr(job, "metadata", None), "uid", None)
        except Exception as e:  # noqa: BLE001 -- left by name on an interrupt
            logger.warning("Could not read the datagen Job's uid: %s", e)
        self.created(
            JOB,
            DATAGEN_JOB_NAME,
            uid if isinstance(uid, str) else None,
            note="uid unknown: it could not be read after the create",
        )

    def finished(self, kind: str, name: str) -> None:
        """The run saw it complete: kept on an interrupt (its logs stay readable)."""
        entry = self.owned.get(f"{kind}/{name}")
        if entry is not None:
            entry.finished = True

    # -- the record ---------------------------------------------------------

    def seal(
        self, *, at_stage: str, prior_failure: bool, exc: BaseException | None = None
    ) -> dict[str, Any]:
        """The ``interrupted`` record before the cleanup: every object to stop is
        ``skipped`` until :meth:`stop_owned` reaches it."""
        self.begin_seal()
        return {
            "signal": self.interrupt_signal(exc),
            "at_stage": at_stage,
            "at_utc": utc_now().isoformat(),
            "prior_failure": bool(prior_failure),
            "stopped": [],
            "left": [],
            "skipped": [e.key for e in self.owned.values() if not e.finished],
        }

    def stop_owned(self, record: dict[str, Any], console: Any = None) -> None:
        """Delete every object this run created and has not seen finish.

        Each delete carries the object's uid as a precondition. 404 is
        stopped (already gone); 409 is left (recreated since, not ours); any
        other error is left with its text. ``record`` is updated as each
        object is handled, so whatever a second signal cuts off stays in
        ``skipped``.
        """
        todo = [e for e in self.owned.values() if not e.finished]
        if not todo:
            return
        _print(console, "Interrupted: stopping this run's jobs (Ctrl-C again to skip)")
        deadline = time.monotonic() + CLEANUP_DEADLINE_S
        api_client = _cleanup_api_client()
        self._in_cleanup = True
        try:
            for entry in todo:
                if self.skip:
                    break
                if _timeout(deadline) is None:
                    _move(record, entry.key, "left", "not reached: the cleanup deadline passed")
                    continue
                outcome, reason = self._stop_one(entry, api_client, deadline)
                _move(record, entry.key, outcome, reason)
                if outcome == "stopped":
                    _print(console, f"  stopped {entry.key}")
                elif outcome == "left":
                    _print(console, f"  left {entry.key}: {reason}")
        except KeyboardInterrupt:
            pass  # the second signal: what was not reached stays skipped
        finally:
            self._in_cleanup = False
            _close(api_client)
        kinds = {SPARK_APPLICATION: "sparkapplication", JOB: "job"}
        rest = [x["object"] for x in record["left"]] + list(record["skipped"])
        for key in rest:
            kind, name = key.split("/", 1)
            _print(
                console,
                f"  may still be running: kubectl delete {kinds.get(kind, kind)} {name} "
                f"-n {self.namespace}",
            )

    def _stop_one(self, entry: Owned, api_client: Any, deadline: float) -> tuple[str, str]:
        """``("stopped" | "left" | "absent", reason)``; "absent": nothing was there."""
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        uid = entry.uid
        try:
            if uid is None:
                found = self._uid_of_ours(entry, api_client, _timeout(deadline) or (1.0, 1.0))
                if found is None:
                    return "left", entry.note
                if found == "absent":
                    return "absent", ""
                if found == "not proven":
                    return "left", _NOT_PROVEN
                uid = found
            timeout = _timeout(deadline)
            if timeout is None:
                return "left", "not reached: the cleanup deadline passed"
            body = k8s_client.V1DeleteOptions(
                preconditions=k8s_client.V1Preconditions(uid=uid),
                propagation_policy="Background",
            )
            if entry.kind == SPARK_APPLICATION:
                k8s_client.CustomObjectsApi(api_client=api_client).delete_namespaced_custom_object(
                    group=SPARK_GROUP,
                    version=SPARK_VERSION,
                    namespace=self.namespace,
                    plural=SPARK_PLURAL,
                    name=entry.name,
                    body=body,
                    _request_timeout=timeout,
                )
            elif entry.kind == JOB:
                k8s_client.BatchV1Api(api_client=api_client).delete_namespaced_job(
                    name=entry.name,
                    namespace=self.namespace,
                    body=body,
                    _request_timeout=timeout,
                )
            else:
                return "left", f"unknown kind {entry.kind}"
        except ApiException as e:
            if e.status == 404:
                return "stopped", ""
            if e.status == 409:
                return "left", "not ours: recreated since this run created it"
            return "left", f"delete failed: {e.status} {e.reason}"
        except Exception as e:  # noqa: BLE001 -- transport errors, timeouts
            return "left", f"delete failed: {e}"
        return "stopped", ""

    def _uid_of_ours(
        self, entry: Owned, api_client: Any, timeout: tuple[float, float]
    ) -> str | None:
        """For an object whose uid the run does not have: its uid when it
        carries this run's ``LB_RUN_ID``, "absent" when there is none, "not
        proven" when it exists without it, None when it cannot be read."""
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        try:
            if entry.kind == SPARK_APPLICATION:
                obj = k8s_client.CustomObjectsApi(
                    api_client=api_client
                ).get_namespaced_custom_object(
                    group=SPARK_GROUP,
                    version=SPARK_VERSION,
                    namespace=self.namespace,
                    plural=SPARK_PLURAL,
                    name=entry.name,
                    _request_timeout=timeout,
                )
                if not isinstance(obj, dict):
                    return None
                env = ((obj.get("spec") or {}).get("driver") or {}).get("env") or []
                uid = (obj.get("metadata") or {}).get("uid")
            elif entry.kind == JOB:
                job = k8s_client.BatchV1Api(api_client=api_client).read_namespaced_job(
                    entry.name, self.namespace, _request_timeout=timeout
                )
                pod_spec = getattr(
                    getattr(getattr(job, "spec", None), "template", None), "spec", None
                )
                env = [
                    {"name": getattr(v, "name", None), "value": getattr(v, "value", None)}
                    for c in (getattr(pod_spec, "containers", None) or [])
                    for v in (getattr(c, "env", None) or [])
                ]
                uid = getattr(getattr(job, "metadata", None), "uid", None)
            else:
                return None
        except ApiException as e:
            return "absent" if e.status == 404 else None
        except Exception:  # noqa: BLE001
            return None
        run_ids = {
            str(v.get("value") or "")
            for v in env
            if isinstance(v, dict) and v.get("name") == "LB_RUN_ID"
        }
        ours = bool(self.run_id) and any(
            r == self.run_id or r.startswith(f"{self.run_id}-c") for r in run_ids
        )
        if not ours or not isinstance(uid, str) or not uid:
            return "not proven"
        return uid


def _timeout(deadline: float) -> tuple[float, float] | None:
    """(connect, read) for one cleanup call, both within the deadline; None
    when less than a second of it is left."""
    left_s = deadline - time.monotonic()
    if left_s <= 1.0:
        return None
    connect = min(CLEANUP_CONNECT_TIMEOUT_S, left_s / 2)
    return connect, min(CLEANUP_READ_TIMEOUT_S, left_s - connect)


def _cleanup_api_client() -> Any:
    """An API client for the cleanup that does not retry, so a call costs at
    most its timeout (the default client lets urllib3 retry three times)."""
    try:
        from kubernetes import client as k8s_client

        cfg = k8s_client.Configuration.get_default_copy()
        cfg.retries = False
        return k8s_client.ApiClient(cfg)
    except Exception as e:  # noqa: BLE001 -- fall back to the default client
        logger.debug("run: no separate cleanup client: %s", e)
        return None


def _close(api_client: Any) -> None:
    try:
        if api_client is not None:
            api_client.close()
    except Exception:  # noqa: BLE001
        pass


def _move(record: dict[str, Any], key: str, outcome: str, reason: str) -> None:
    if key in record["skipped"]:
        record["skipped"].remove(key)
    if outcome == "stopped":
        record["stopped"].append(key)
    elif outcome == "left":
        record["left"].append({"object": key, "reason": reason})
    # "absent": the create never landed; nothing to record.


def _print(console: Any, line: str) -> None:
    try:
        if console is not None:
            console.print(line)
        else:
            _say(line)
    except Exception:  # noqa: BLE001 -- a closed terminal must not stop the cleanup
        pass


def _say(message: str) -> None:
    import sys

    try:
        sys.stderr.write(message + "\n")
        sys.stderr.flush()
    except Exception:  # noqa: BLE001
        pass
