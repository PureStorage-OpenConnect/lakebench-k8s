"""LB-200: datagen progress must be scoped to the current Job incarnation.

A re-generate after an OOM reuses the datagen name/label, so a prior job's
OOMKilled pod (still Terminating) would false-abort the new, healthy generation
via get_progress -> oom_pods. _pod_owned_by_job filters by the owning Job uid.
"""

from __future__ import annotations

from types import SimpleNamespace

from lakebench.deploy.datagen import _pod_owned_by_job


def _pod(owner_refs):
    return SimpleNamespace(metadata=SimpleNamespace(owner_references=owner_refs))


def _ref(kind, uid):
    return SimpleNamespace(kind=kind, uid=uid)


def test_pod_owned_by_current_job():
    pod = _pod([_ref("Job", "uid-current")])
    assert _pod_owned_by_job(pod, "uid-current") is True


def test_stale_pod_from_prior_job_is_excluded():
    pod = _pod([_ref("Job", "uid-previous")])
    assert _pod_owned_by_job(pod, "uid-current") is False


def test_pod_with_no_owner_refs_is_excluded():
    assert _pod_owned_by_job(_pod(None), "uid-current") is False
    assert _pod_owned_by_job(_pod([]), "uid-current") is False


def test_non_job_owner_is_excluded():
    pod = _pod([_ref("ReplicaSet", "uid-current")])
    assert _pod_owned_by_job(pod, "uid-current") is False
