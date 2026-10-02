"""The Spark Operator controller's /tmp (spark-submit's Ivy cache).

Live 2026-09-27: lakebench sets spark.jars.ivy=/tmp/.ivy2, the operator runs
spark-submit in its controller, and the chart's 1Gi /tmp emptyDir filled with
Maven jars. The kubelet evicted the controller 19 times in ~100 minutes, and
each eviction cost every tenant a leader election, a cold cache and a
SUBMISSION_FAILED retry. These tests pin the admin fix and its diagnosis.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from lakebench.cli._admin import admin_app
from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager
from lakebench.modules.pipeline_engines.spark.operator_scratch import (
    DEFAULT_CONTROLLER_TMP_SIZE,
    TmpVolume,
    diagnose,
    helm_set_args,
    parse_quantity,
    tmp_volume,
    validate_size,
)

runner = CliRunner()
_RUN = "lakebench.modules.pipeline_engines.spark.operator.subprocess.run"


def _deployment(size_limit: str | None = "1Gi", name: str = "tmp") -> dict:
    empty_dir = {} if size_limit is None else {"sizeLimit": size_limit}
    return {"spec": {"template": {"spec": {"volumes": [{"name": name, "emptyDir": empty_dir}]}}}}


class _FakeCluster:
    """Answers the helm/kubectl calls SparkOperatorManager makes."""

    def __init__(self, *, release: bool | None, chart: str = "spark-operator-2.5.1"):
        self.release = release
        self.chart = chart
        self.tmp_after_upgrade = DEFAULT_CONTROLLER_TMP_SIZE
        self.tmp_before = "1Gi"
        self.upgraded = False
        self.ready = "1"
        self.stored_values: dict = {}
        self.calls: list[list[str]] = []

    def helm_upgrades(self) -> list[list[str]]:
        return [c for c in self.calls if c[:2] == ["helm", "upgrade"]]

    def helm_writes(self) -> list[list[str]]:
        return [c for c in self.calls if c[:2] in (["helm", "upgrade"], ["helm", "install"])]

    def __call__(self, cmd, **kwargs):
        self.calls.append(list(cmd))
        ok = MagicMock(returncode=0, stdout="", stderr="")
        if cmd[:2] == ["helm", "status"]:
            if self.release is None:
                return MagicMock(returncode=1, stdout="", stderr="Error: Unauthorized")
            if not self.release:
                return MagicMock(returncode=1, stdout="", stderr="Error: release: not found")
            return ok
        if cmd[:3] == ["helm", "get", "values"]:
            return MagicMock(returncode=0, stdout=json.dumps(self.stored_values))
        if cmd[:2] == ["helm", "list"] and self.chart is None:
            return MagicMock(returncode=1, stdout="", stderr="Error: Unauthorized")
        if cmd[:2] == ["helm", "list"]:
            return MagicMock(
                returncode=0, stdout=json.dumps([{"name": "spark-operator", "chart": self.chart}])
            )
        if cmd[:2] in (["helm", "upgrade"], ["helm", "install"]):
            self.upgraded = True
            return ok
        if cmd[:3] == ["kubectl", "api-resources", "--api-group=security.openshift.io"]:
            return MagicMock(returncode=1, stdout="", stderr="")
        if (
            cmd[:3] == ["kubectl", "get", "deployment"]
            and "-o" in cmd
            and cmd[cmd.index("-o") + 1] == "json"
        ):
            size = self.tmp_after_upgrade if self.upgraded else self.tmp_before
            return MagicMock(returncode=0, stdout=json.dumps(_deployment(size)))
        if cmd[:2] == ["kubectl", "get"] and "crd" in cmd:
            return MagicMock(returncode=0, stdout="sparkapplications")
        if cmd[:3] == ["kubectl", "get", "deployment"] and "-A" in cmd:
            return MagicMock(
                returncode=0, stdout="NAMESPACE NAME\nspark-operator spark-operator-controller"
            )
        if cmd[:3] == ["kubectl", "get", "deployment"]:
            return MagicMock(returncode=0, stdout=self.ready)  # readyReplicas
        return ok


def _set_values(cmd: list[str]) -> list[str]:
    return [cmd[i + 1] for i, a in enumerate(cmd) if a == "--set"]


class TestHelmValues:
    def test_full_list_element_is_written(self):
        """Helm replaces lists wholesale: without the name the chart's /tmp
        mount would reference a volume that no longer exists."""
        assert _set_values(helm_set_args("8Gi")) == [
            "controller.volumes[0].name=tmp",
            "controller.volumes[0].emptyDir.sizeLimit=8Gi",
        ]

    def test_default_clears_the_floor(self):
        assert parse_quantity(DEFAULT_CONTROLLER_TMP_SIZE) >= 4 * 1024**3
        assert validate_size(DEFAULT_CONTROLLER_TMP_SIZE) == DEFAULT_CONTROLLER_TMP_SIZE

    @pytest.mark.parametrize("bad", ["1Gi", "2048Mi", "8G", "eight", ""])
    def test_rejects_small_or_odd_sizes(self, bad):
        with pytest.raises(ValueError):
            validate_size(bad)

    @pytest.mark.parametrize(
        ("q", "n"), [("1Gi", 1024**3), ("500Mi", 500 * 1024**2), ("4G", 4 * 1000**3), ("x", None)]
    )
    def test_parse_quantity(self, q, n):
        assert parse_quantity(q) == n


class TestInstall:
    """install() is the fresh install admin install runs; never an upgrade."""

    def test_fresh_install_sizes_tmp(self):
        fake = _FakeCluster(release=False)
        with patch(_RUN, side_effect=fake), patch("time.sleep"):
            assert SparkOperatorManager(version="2.5.1").install() is True
        (cmd,) = fake.helm_writes()
        assert cmd[:2] == ["helm", "install"]
        assert "--reuse-values" not in cmd
        assert "controller.volumes[0].emptyDir.sizeLimit=8Gi" in _set_values(cmd)
        assert cmd[cmd.index("--version") + 1] == "2.5.1"

    def test_existing_release_is_never_upgraded(self):
        """An installed operator keeps its chart and watch list: install()
        on an existing release refuses. Reverted (v1.6), it ran helm upgrade
        --reuse-values to the given version."""
        fake = _FakeCluster(release=True)
        with patch(_RUN, side_effect=fake), patch("time.sleep"):
            mgr = SparkOperatorManager(version="2.4.0", job_namespace="tenant-a")
            assert mgr.install() is False
        assert fake.helm_writes() == []

    def test_no_version_refuses_an_unpinned_install(self):
        fake = _FakeCluster(release=False)
        with patch(_RUN, side_effect=fake), patch("time.sleep"):
            assert SparkOperatorManager().install() is False
        assert fake.helm_writes() == []

    def test_refuses_when_release_state_unreadable(self):
        fake = _FakeCluster(release=None)
        with patch(_RUN, side_effect=fake):
            assert SparkOperatorManager(version="2.5.1").install() is False
        assert fake.helm_writes() == []

    def test_fails_when_the_spec_did_not_take_the_size(self):
        fake = _FakeCluster(release=False)
        fake.tmp_after_upgrade = "1Gi"
        with patch(_RUN, side_effect=fake), patch("time.sleep"):
            assert SparkOperatorManager(version="2.5.1").install() is False


class TestApplyTmpSize:
    def test_resize_pins_installed_chart_and_reuses_values(self):
        fake = _FakeCluster(release=True, chart="spark-operator-2.5.1")
        with patch(_RUN, side_effect=fake):
            # The config pins an older chart; the resize must not move the release.
            assert SparkOperatorManager(version="2.4.0").apply_controller_tmp_size("8Gi") is True
        (cmd,) = fake.helm_upgrades()
        assert "--install" not in cmd
        assert "--reuse-values" in cmd
        assert cmd[cmd.index("--version") + 1] == "2.5.1"
        assert "controller.volumes[0].emptyDir.sizeLimit=8Gi" in _set_values(cmd)
        assert not any(v.startswith("spark.jobNamespaces") for v in _set_values(cmd))
        # The resize waits for the new ReplicaSets before reporting done.
        assert any(c[:3] == ["kubectl", "rollout", "status"] for c in fake.calls)

    def test_watch_list_edit_never_touches_the_volume(self):
        """Watch-list edits run on every deploy; they carry the stored size
        forward with --reuse-values and must not override it."""
        fake = _FakeCluster(release=True)
        with patch(_RUN, side_effect=fake):
            pin = SparkOperatorManager(version="2.5.1")._watch_list_pin()
        assert pin is not None
        assert not any("controller.volumes" in a for a in pin)


class TestDiagnose:
    _EVICT_MSG = 'Usage of EmptyDir volume "tmp" exceeds the limit "1Gi". '

    def _pod(self, name, reason=None, message="", restarts=0):
        return {
            "metadata": {"name": name},
            "status": {
                "reason": reason,
                "message": message,
                "startTime": "2026-09-27T15:05:00Z",
                "containerStatuses": [{"restartCount": restarts}],
            },
        }

    def _event(self, name, message, at="2026-09-27T15:05:41Z"):
        return {
            "reason": "Evicted",
            "involvedObject": {"kind": "Pod", "name": name},
            "message": message,
            "lastTimestamp": at,
        }

    def test_live_incident_is_diagnosed(self):
        diag = diagnose(
            _deployment("1Gi"),
            [self._pod("spark-operator-controller-a", "Evicted", self._EVICT_MSG)],
            [self._event("spark-operator-controller-a", self._EVICT_MSG)],
        )
        assert not diag.healthy
        assert len(diag.storage_evictions) == 1  # pod and event are one eviction
        text = " ".join(diag.problems)
        assert "1Gi" in text and "evicted for storage" in text

    def test_undersized_alone_is_a_problem(self):
        assert not diagnose(_deployment("1Gi"), [], []).healthy

    def test_sized_and_quiet_is_healthy(self):
        assert diagnose(_deployment("8Gi"), [self._pod("c", restarts=2)], []).healthy
        assert diagnose(_deployment(None), [], []).healthy  # unbounded emptyDir

    def test_other_evictions_and_webhook_are_not_storage(self):
        diag = diagnose(
            _deployment("8Gi"),
            [self._pod("spark-operator-controller-b", "Evicted", "The node was low on memory")],
            [self._event("spark-operator-webhook-x", self._EVICT_MSG)],
        )
        assert diag.healthy
        assert diag.other_evictions == 1

    def test_missing_volume(self):
        vol = tmp_volume(_deployment("1Gi", name="scratch"))
        assert not vol.found and not vol.undersized


@contextmanager
def _admin_mgr(vol: TmpVolume, watched=None):
    core = MagicMock()
    page = MagicMock(items=[])
    page.metadata._continue = None
    core.list_namespace.return_value = page
    with (
        patch("lakebench.cli._admin._get_core_v1", return_value=core),
        patch("lakebench.modules.pipeline_engines.spark.operator.SparkOperatorManager") as cls,
        patch("lakebench.deploy.cluster_lock.cluster_lock") as lock,
    ):
        mgr = cls.return_value
        mgr.controller_tmp_volume.return_value = vol
        mgr._get_watched_namespaces.return_value = watched
        mgr._get_active_namespaces.return_value = watched
        mgr.release_state.return_value = ("deployed", 3)
        mgr.apply_controller_tmp_size.return_value = True
        yield mgr, lock


class TestRepairOperatorTmp:
    def test_small_tmp_is_resized_under_the_lease(self):
        with _admin_mgr(TmpVolume(True, "1Gi")) as (mgr, lock):
            r = runner.invoke(admin_app, ["repair-operator"])
        assert r.exit_code == 0, r.output
        assert lock.called
        mgr.apply_controller_tmp_size.assert_called_once_with("8Gi")
        assert "1Gi -> 8Gi" in r.output

    def test_dry_run_does_not_resize(self):
        with _admin_mgr(TmpVolume(True, "1Gi")) as (mgr, lock):
            r = runner.invoke(admin_app, ["repair-operator", "--dry-run"])
        assert r.exit_code == 0
        assert not lock.called
        mgr.apply_controller_tmp_size.assert_not_called()

    def test_large_enough_tmp_is_left_alone(self):
        # The lease is taken to read inside it; nothing is applied.
        with _admin_mgr(TmpVolume(True, "8Gi")) as (mgr, lock):
            r = runner.invoke(admin_app, ["repair-operator"])
        assert r.exit_code == 0
        assert lock.called
        mgr.apply_controller_tmp_size.assert_not_called()
        mgr._set_watch_list_impl.assert_not_called()

    def test_failed_resize_exits_nonzero(self):
        with _admin_mgr(TmpVolume(True, "1Gi")) as (mgr, _lock):
            mgr.apply_controller_tmp_size.return_value = False
            r = runner.invoke(admin_app, ["repair-operator"])
        assert r.exit_code == 1


class TestInstallCommand:
    def test_passes_version_and_size(self):
        """The old verb, now an alias of admin install, still passes --version
        and --controller-tmp-size to a fresh install."""
        from lakebench.deploy.shared_components import ComponentStatus, SparkOperator

        absent = ComponentStatus(installed=False, detail="no Spark Operator")
        ready = ComponentStatus(installed=True, version="2.5.1", ready=True)
        with (
            _admin_mgr(TmpVolume(True, "1Gi")) as (mgr, _lock),
            patch.object(SparkOperator, "status", side_effect=[absent, absent, ready]),
        ):
            mgr.install.return_value = True
            mgr.refresh_chart_repo.return_value = None
            r = runner.invoke(
                admin_app,
                ["install-spark-operator", "--version", "2.5.1", "--controller-tmp-size", "16Gi"],
            )
        assert r.exit_code == 0, r.output
        mgr.install.assert_called_once_with(version="2.5.1", tmp_size="16Gi", add_repo=False)

    def test_rejects_a_size_below_the_floor(self):
        with _admin_mgr(TmpVolume(True, "1Gi")) as (mgr, _lock):
            r = runner.invoke(admin_app, ["install-spark-operator", "--controller-tmp-size", "1Gi"])
        assert r.exit_code == 2
        mgr.install.assert_not_called()


class TestDoctorAndStatus:
    def _diag(self):
        return diagnose(
            _deployment("1Gi"),
            [],
            [
                {
                    "reason": "Evicted",
                    "involvedObject": {"kind": "Pod", "name": "spark-operator-controller-z"},
                    "message": TestDiagnose._EVICT_MSG,
                    "lastTimestamp": "2026-09-27T15:17:06Z",
                }
            ],
        )

    def test_doctor_names_the_condition_and_the_repair(self):
        from lakebench.deploy import prereqs

        core = MagicMock()
        ok = [
            prereqs.PrereqOutcome(p, prereqs.PrereqResult(prereqs.PrereqStatus.OK, "fine"))
            for p in prereqs.PREREQS
        ]
        with (
            patch("lakebench.cli._admin._get_core_v1", return_value=core),
            patch("lakebench.deploy.prereqs.run_prereqs", return_value=ok),
            patch("lakebench.deploy.cluster_lock.read_cluster_lock", return_value=None),
            patch("lakebench.cli._admin._read_operator_scratch", return_value=self._diag()),
        ):
            r = runner.invoke(admin_app, ["doctor"])
        assert r.exit_code == 0, r.output
        out = " ".join(r.output.split())
        assert "sizeLimit is 1Gi" in out
        assert "evicted for storage" in out
        assert "lakebench admin repair-operator" in out

    def test_status_reports_a_healthy_controller(self):
        core = MagicMock()
        core.list_namespace.return_value = MagicMock(items=[])
        with (
            patch("lakebench.cli._admin._get_core_v1", return_value=core),
            patch("lakebench.deploy.cluster_lock.read_cluster_lock", return_value=None),
            patch(
                "lakebench.cli._admin._read_operator_scratch",
                return_value=diagnose(_deployment("8Gi"), [], []),
            ),
        ):
            r = runner.invoke(admin_app, ["status"])
        assert r.exit_code == 0
        assert "controller /tmp: 8Gi" in " ".join(r.output.split())


class TestReviewFixes:
    def test_refuses_to_drop_other_stored_controller_volumes(self):
        fake = _FakeCluster(release=True)
        fake.stored_values = {"controller": {"volumes": [{"name": "tmp"}, {"name": "ca-bundle"}]}}
        with patch(_RUN, side_effect=fake), patch("time.sleep"):
            assert SparkOperatorManager(version="2.5.1").install() is False
            assert SparkOperatorManager().apply_controller_tmp_size("8Gi") is False
        assert fake.helm_upgrades() == []

    def test_upgrade_without_a_size_keeps_a_larger_one(self):
        fake = _FakeCluster(release=True)
        fake.tmp_before = fake.tmp_after_upgrade = "16Gi"
        with patch(_RUN, side_effect=fake), patch("time.sleep"):
            assert SparkOperatorManager().apply_controller_tmp_size(None) is True
        (cmd,) = fake.helm_upgrades()
        assert "controller.volumes[0].emptyDir.sizeLimit=16Gi" in _set_values(cmd)

    def test_unreadable_installed_version_refuses_unpinned_upgrade(self):
        fake = _FakeCluster(release=True, chart=None)
        with patch(_RUN, side_effect=fake), patch("time.sleep"):
            assert SparkOperatorManager().install() is False
            # The config's pin is not a stand-in for the installed chart.
            assert SparkOperatorManager(version="2.4.0").apply_controller_tmp_size("8Gi") is False
        assert fake.helm_upgrades() == []

    def test_evictions_before_a_resize_are_history(self):
        msg = TestDiagnose._EVICT_MSG

        def evicted(name, limit):
            return {
                "metadata": {"name": name},
                "spec": {"volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": limit}}]},
                "status": {"reason": "Evicted", "message": msg},
            }

        old = evicted("spark-operator-controller-79cc857c77-kmn94", "1Gi")
        diag = diagnose(_deployment("8Gi"), [old], [])
        assert diag.healthy
        assert diag.past_storage_evictions == 1

    def test_evictions_at_the_current_size_stay_current_across_replicasets(self):
        """Every watch-list edit makes a new ReplicaSet; an eviction under
        the current size is still a problem after one."""
        msg = TestDiagnose._EVICT_MSG
        pod = {
            "metadata": {"name": "spark-operator-controller-aaa-1"},
            "spec": {"volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "8Gi"}}]},
            "status": {"reason": "Evicted", "message": msg},
        }
        live = {
            "metadata": {"name": "spark-operator-controller-bbb-2"},
            "status": {"phase": "Running"},
        }
        diag = diagnose(_deployment("8Gi"), [pod, live], [])
        assert not diag.healthy
        # An eviction known only from its event counts as current.
        event = {
            "reason": "Evicted",
            "involvedObject": {"kind": "Pod", "name": "spark-operator-controller-ccc-3"},
            "message": msg,
            "lastTimestamp": "2026-09-27T15:05:41Z",
        }
        assert not diagnose(_deployment("8Gi"), [live], [event]).healthy

    def test_unbounded_tmp_is_kept_on_upgrade(self):
        fake = _FakeCluster(release=True)
        fake.tmp_before = fake.tmp_after_upgrade = None
        fake.stored_values = {"controller": {"volumes": [{"name": "tmp", "emptyDir": {}}]}}
        with patch(_RUN, side_effect=fake), patch("time.sleep"):
            assert SparkOperatorManager().apply_controller_tmp_size(None) is True
        (cmd,) = fake.helm_upgrades()
        assert not any("controller.volumes" in v for v in _set_values(cmd))


class TestThirdPass:
    _MSG = 'Usage of EmptyDir volume "tmp" exceeds the limit "1Gi". '

    def test_events_left_after_repair_and_pod_cleanup_are_history(self):
        live = {
            "metadata": {
                "name": "spark-operator-controller-new-1",
                "creationTimestamp": "2026-09-27T16:00:00Z",
            },
            "status": {"phase": "Running"},
        }
        event = {
            "reason": "Evicted",
            "involvedObject": {"kind": "Pod", "name": "spark-operator-controller-old-1"},
            "message": self._MSG,
            "lastTimestamp": "2026-09-27T15:17:06Z",
        }
        diag = diagnose(_deployment("8Gi"), [live], [event])
        assert diag.healthy and diag.past_storage_evictions == 1
        event["lastTimestamp"] = "2026-09-27T16:10:00Z"
        assert not diagnose(_deployment("8Gi"), [live], [event]).healthy

    def test_hand_patched_unbounded_deployment_is_not_trusted(self):
        """Only the stored values keep an unbounded /tmp; without them the
        upgrade sets the default rather than re-render the chart's 1Gi."""
        fake = _FakeCluster(release=True)
        fake.tmp_before = None
        with patch(_RUN, side_effect=fake), patch("time.sleep"):
            assert SparkOperatorManager().apply_controller_tmp_size(None) is True
        (cmd,) = fake.helm_upgrades()
        assert "controller.volumes[0].emptyDir.sizeLimit=8Gi" in _set_values(cmd)
