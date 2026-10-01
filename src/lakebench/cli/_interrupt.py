"""Ctrl-C and SIGTERM for ``lakebench run``: seal the record, stop this run's jobs.

``run`` and the continuous runner each make one :class:`RunInterrupt` before
their main ``try``. It does three things:

- **Signals.** SIGINT and SIGTERM get a handler that raises
  ``KeyboardInterrupt`` on the first signal, so the run's ``except
  KeyboardInterrupt`` branch seals the record INTERRUPTED. From then on the
  run is sealing: a second signal only asks to skip the remaining deletes,
  and a third stops at once. Inside the cluster lease
  (``deploy.cluster_lock``) these handlers are saved, the signal is held back
  until the shared change has finished and the lease is released, and is then
  delivered to them; nothing here runs under the lease.
- **Ownership.** Every SparkApplication and datagen Job the run creates is
  registered with the uid of the object the API server created. On an
  interrupt each one the run has not seen finish is deleted with that uid as
  a precondition, so an object of the same name created since by another
  invocation is never deleted: the API server answers 409 and it is left.
- **Record.** :meth:`RunInterrupt.seal` returns the ``interrupted`` block of
  the run record: ``{signal, at_stage, at_utc, prior_failure, stopped, left,
  skipped}``.

SIGKILL and a lost host are out of reach; the next ``run`` deletes a left
object by name before it submits its own.
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

#: The signals ``run`` handles. SIGHUP is not one: a hung-up terminal ends
#: the process as before.
INTERRUPT_SIGNALS: tuple[int, ...] = (signal.SIGINT, signal.SIGTERM)

SPARK_APPLICATION = "SparkApplication"
JOB = "Job"
DATAGEN_JOB_NAME = "lakebench-datagen"

#: Per API call during the cleanup: (connect, read) seconds. The client
#: retries a timed-out call, so the whole cleanup also has a deadline.
CLEANUP_REQUEST_TIMEOUT: tuple[int, int] = (5, 15)
#: Seconds the whole cleanup may take; an object not reached by then is left.
CLEANUP_DEADLINE_S = 60.0

_UNKNOWN_UID = (
    "uid unknown: interrupted while it was being created; the next run deletes it by name"
)

_Handler = Callable[[int, Any], Any] | int | None

# The instance whose handlers are installed, so a run that ended without
# restoring them (an error inside its finally) cannot chain into the next.
_ACTIVE: RunInterrupt | None = None


@dataclass
class Owned:
    """One object this run created: kind, name, the uid the API server gave it."""

    kind: str
    name: str
    uid: str | None = None
    #: The run saw it finish (a batch stage that completed): kept, not deleted.
    finished: bool = False

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
        #: True once the first interrupt was raised: later signals do not raise.
        self.sealing = False
        #: A second signal while sealing: the remaining deletes are skipped.
        self.skip = False
        self._saved: dict[int, _Handler] = {}
        self._installed = False

    # -- signals ------------------------------------------------------------

    def install(self) -> None:
        """Install the handlers (main thread only; an ignored signal stays ignored)."""
        global _ACTIVE
        if threading.current_thread() is not threading.main_thread():
            logger.debug("run: not on the main thread; SIGTERM is not handled")
            return
        if _ACTIVE is not None and _ACTIVE is not self:
            _ACTIVE.restore()
        for sig in INTERRUPT_SIGNALS:
            try:
                previous = signal.getsignal(sig)
                if previous == signal.SIG_IGN:
                    continue
                self._saved[sig] = previous
                signal.signal(sig, self._on_signal)
            except (ValueError, OSError) as e:  # not the main interpreter
                logger.debug("run: cannot handle %s: %s", sig, e)
        self._installed = True
        _ACTIVE = self

    def restore(self) -> None:
        """Put the handlers that were there before :meth:`install` back."""
        global _ACTIVE
        for sig, handler in self._saved.items():
            try:
                signal.signal(sig, handler if handler is not None else signal.SIG_DFL)
            except (ValueError, OSError, TypeError) as e:
                logger.debug("run: could not restore the handler for %s: %s", sig, e)
        self._saved.clear()
        self._installed = False
        if _ACTIVE is self:
            _ACTIVE = None

    def _on_signal(self, signum: int, _frame: Any) -> None:
        self.received.append(signal_name(signum))
        if not self.sealing:
            # Sealing from here on, so a second signal landing before the
            # except branch starts cannot escape it.
            self.sealing = True
            raise KeyboardInterrupt
        if not self.skip:
            self.skip = True
            _say(
                "Skipping the rest of the cleanup; the run record is still written. "
                "Interrupt once more to stop now."
            )
            return
        self.restore()
        raise KeyboardInterrupt

    def begin_seal(self) -> None:
        """Called first in the run's ``except KeyboardInterrupt`` branch."""
        self.sealing = True

    def interrupt_signal(self, exc: BaseException | None) -> str:
        """The signal that stopped the run.

        ``LeaseAbort`` (the third signal inside the cluster lease) carries its
        own; otherwise the first signal this handler saw; otherwise SIGINT
        (Python's own handler, when this one could not be installed).
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

    def created(self, kind: str, name: str, uid: str | None) -> None:
        entry = self.owned.get(f"{kind}/{name}")
        if entry is None:
            entry = Owned(kind, name)
            self.owned[entry.key] = entry
        entry.uid = uid or None
        entry.finished = False

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
        """After a datagen deploy: the Job's uid, read back from the API server."""
        uid = None
        try:
            from kubernetes import client as k8s_client

            job = k8s_client.BatchV1Api().read_namespaced_job(
                DATAGEN_JOB_NAME, self.namespace, _request_timeout=CLEANUP_REQUEST_TIMEOUT
            )
            uid = getattr(getattr(job, "metadata", None), "uid", None)
        except Exception as e:  # noqa: BLE001 -- left by name on an interrupt
            logger.warning("Could not read the datagen Job's uid: %s", e)
        self.created(JOB, DATAGEN_JOB_NAME, uid)

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
        record: dict[str, Any] = {
            "signal": self.interrupt_signal(exc),
            "at_stage": at_stage,
            "at_utc": utc_now().isoformat(),
            "prior_failure": bool(prior_failure),
            "stopped": [],
            "left": [],
            "skipped": [e.key for e in self.owned.values() if not e.finished],
        }
        if type(exc).__name__ == "LeaseAbort":
            record["left"].append(
                {
                    "object": "Spark Operator watch-list change",
                    "reason": (
                        "aborted inside the cluster lease; if a helm upgrade was running, "
                        "check `helm history` and run `lakebench admin repair-operator`"
                    ),
                }
            )
        return record

    def stop_owned(self, record: dict[str, Any], console: Any = None) -> None:
        """Delete every object this run created and has not seen finish.

        Each delete carries the object's uid as a precondition. 404 is
        stopped (already gone); 409 is left (recreated since, not ours); any
        other error is left with its text. ``record`` is updated as each
        object is handled, so whatever a third signal cuts off stays in
        ``skipped``.
        """
        todo = [e for e in self.owned.values() if not e.finished]
        if not todo:
            return
        _print(console, "Interrupted: stopping this run's jobs (Ctrl-C again to skip)")
        deadline = time.monotonic() + CLEANUP_DEADLINE_S
        for entry in todo:
            if self.skip:
                break
            if time.monotonic() >= deadline:
                _move(record, entry.key, "left", "not reached: the cleanup deadline passed")
                continue
            outcome, reason = self._stop_one(entry)
            _move(record, entry.key, outcome, reason)
            if outcome == "stopped":
                _print(console, f"  stopped {entry.key}")
            else:
                _print(console, f"  left {entry.key}: {reason}")
        left = [x["object"] for x in record["left"] if "/" in x["object"]]
        if left or record["skipped"]:
            kinds = {SPARK_APPLICATION: "sparkapplication", JOB: "job"}
            for key in left + list(record["skipped"]):
                kind, name = key.split("/", 1)
                _print(
                    console,
                    f"  still running? kubectl delete {kinds.get(kind, kind)} {name} "
                    f"-n {self.namespace}",
                )

    def _stop_one(self, entry: Owned) -> tuple[str, str]:
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        uid = entry.uid
        try:
            if uid is None:
                uid = self._uid_of_ours(entry)
                if uid is None:
                    return "left", _UNKNOWN_UID
                if uid == "absent":
                    return "stopped", ""
            body = k8s_client.V1DeleteOptions(
                preconditions=k8s_client.V1Preconditions(uid=uid),
                propagation_policy="Background",
            )
            if entry.kind == SPARK_APPLICATION:
                k8s_client.CustomObjectsApi().delete_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=self.namespace,
                    plural="sparkapplications",
                    name=entry.name,
                    body=body,
                    _request_timeout=CLEANUP_REQUEST_TIMEOUT,
                )
            elif entry.kind == JOB:
                k8s_client.BatchV1Api().delete_namespaced_job(
                    name=entry.name,
                    namespace=self.namespace,
                    body=body,
                    _request_timeout=CLEANUP_REQUEST_TIMEOUT,
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

    def _uid_of_ours(self, entry: Owned) -> str | None:
        """For an object created while the interrupt landed: its uid when the
        object carries this run's ``LB_RUN_ID``, "absent" when there is none,
        None when it cannot be shown to be ours."""
        if entry.kind != SPARK_APPLICATION or not self.run_id:
            return None
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        try:
            obj = k8s_client.CustomObjectsApi().get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                name=entry.name,
                _request_timeout=CLEANUP_REQUEST_TIMEOUT,
            )
        except ApiException as e:
            if e.status == 404:
                return "absent"
            return None
        if not isinstance(obj, dict):
            return None
        env = ((obj.get("spec") or {}).get("driver") or {}).get("env") or []
        run_ids = {
            str(v.get("value") or "")
            for v in env
            if isinstance(v, dict) and v.get("name") == "LB_RUN_ID"
        }
        ours = any(r == self.run_id or r.startswith(f"{self.run_id}-c") for r in run_ids)
        uid = (obj.get("metadata") or {}).get("uid")
        return uid if ours and uid else None


def _move(record: dict[str, Any], key: str, outcome: str, reason: str) -> None:
    if key in record["skipped"]:
        record["skipped"].remove(key)
    if outcome == "stopped":
        record["stopped"].append(key)
    else:
        record["left"].append({"object": key, "reason": reason})


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
