"""The Spark Operator controller's /tmp (spark-submit's Ivy cache): the admin resize,
its install guards and the diagnosis of storage evictions.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from lakebench.cli._admin import admin_app
from lakebench.modules.pipeline_engines.spark.operator import ReleaseState, SparkOperatorManager
from lakebench.modules.pipeline_engines.spark.operator_scratch import (
    DEFAULT_CONTROLLER_TMP_SIZE,
    TmpVolume,
    diagnose,
    helm_set_args,
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

    @pytest.mark.parametrize("bad", ["1Gi", "2048Mi", "8G", "eight", "x", ""])
    def test_rejects_small_or_odd_sizes(self, bad):
        with pytest.raises(ValueError):
            validate_size(bad)


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

    def test_refuses_to_drop_other_stored_controller_volumes(self):
        fake = _FakeCluster(release=True)
        fake.stored_values = {"controller": {"volumes": [{"name": "tmp"}, {"name": "ca-bundle"}]}}
        with patch(_RUN, side_effect=fake), patch("time.sleep"):
            assert SparkOperatorManager(version="2.5.1").install() is False
            assert SparkOperatorManager().apply_controller_tmp_size("8Gi") is False
        assert fake.helm_upgrades() == []

    def test_unreadable_installed_version_refuses_unpinned_upgrade(self):
        fake = _FakeCluster(release=True, chart=None)
        with patch(_RUN, side_effect=fake), patch("time.sleep"):
            assert SparkOperatorManager().install() is False
            # The config's pin is not a stand-in for the installed chart.
            assert SparkOperatorManager(version="2.4.0").apply_controller_tmp_size("8Gi") is False
        assert fake.helm_upgrades() == []


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

    @pytest.mark.parametrize(
        ("before", "after", "stored", "expected_set"),
        [
            (
                "16Gi",
                "16Gi",
                None,
                "controller.volumes[0].emptyDir.sizeLimit=16Gi",
            ),  # keeps a larger one
            # only the stored values keep an unbounded /tmp
            (None, None, {"controller": {"volumes": [{"name": "tmp", "emptyDir": {}}]}}, None),
            # hand-patched unbounded without stored values gets the default, not the chart's 1Gi
            (None, "unset", None, "controller.volumes[0].emptyDir.sizeLimit=8Gi"),
        ],
    )
    def test_upgrade_without_a_size_never_shrinks_tmp(self, before, after, stored, expected_set):
        fake = _FakeCluster(release=True)
        fake.tmp_before = before
        if after != "unset":
            fake.tmp_after_upgrade = after
        if stored is not None:
            fake.stored_values = stored
        with patch(_RUN, side_effect=fake), patch("time.sleep"):
            assert SparkOperatorManager().apply_controller_tmp_size(None) is True
        (cmd,) = fake.helm_upgrades()
        if expected_set is None:
            assert not any("controller.volumes" in v for v in _set_values(cmd))
        else:
            assert expected_set in _set_values(cmd)


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

    @staticmethod
    def _evicted_pod(name, limit=None):
        pod = {
            "metadata": {"name": name},
            "status": {"reason": "Evicted", "message": TestDiagnose._EVICT_MSG},
        }
        if limit:
            pod["spec"] = {"volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": limit}}]}
        return pod

    @staticmethod
    def _live_pod(name, created=None):
        meta = {"name": name}
        if created:
            meta["creationTimestamp"] = created
        return {"metadata": meta, "status": {"phase": "Running"}}

    @pytest.mark.parametrize(
        ("case", "healthy", "past"),
        [
            # Evicted under the old 1Gi size, since resized to 8Gi: history.
            ("evicted_before_resize", True, 1),
            # Every watch-list edit makes a new ReplicaSet; an eviction under
            # the current size stays a problem after one.
            ("evicted_at_current_size_across_replicasets", False, None),
            # An eviction known only from its event counts as current.
            ("event_only_eviction", False, None),
            # Events left behind by a repair and pod cleanup are history.
            ("event_older_than_live_pod", True, 1),
            ("event_newer_than_live_pod", False, None),
        ],
    )
    def test_eviction_history_versus_current(self, case, healthy, past):
        live = self._live_pod("spark-operator-controller-bbb-2")
        event = self._event("spark-operator-controller-ccc-3", self._EVICT_MSG)
        pods, events = {
            "evicted_before_resize": (
                [self._evicted_pod("spark-operator-controller-79cc857c77-kmn94", "1Gi")],
                [],
            ),
            "evicted_at_current_size_across_replicasets": (
                [self._evicted_pod("spark-operator-controller-aaa-1", "8Gi"), live],
                [],
            ),
            "event_only_eviction": ([live], [event]),
            "event_older_than_live_pod": (
                [self._live_pod("spark-operator-controller-new-1", "2026-09-27T16:00:00Z")],
                [
                    self._event(
                        "spark-operator-controller-old-1", self._EVICT_MSG, "2026-09-27T15:17:06Z"
                    )
                ],
            ),
            "event_newer_than_live_pod": (
                [self._live_pod("spark-operator-controller-new-1", "2026-09-27T16:00:00Z")],
                [
                    self._event(
                        "spark-operator-controller-old-1", self._EVICT_MSG, "2026-09-27T16:10:00Z"
                    )
                ],
            ),
        }[case]
        diag = diagnose(_deployment("8Gi"), pods, events)
        assert diag.healthy is healthy
        if past is not None:
            assert diag.past_storage_evictions == past

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
        mgr.release_state.return_value = ReleaseState("deployed", 3)
        mgr.apply_controller_tmp_size.return_value = True
        yield mgr, lock


class TestRepairOperatorTmp:
    @pytest.mark.parametrize(
        ("size", "args", "applied", "lock_taken"),
        [
            ("1Gi", [], True, True),
            ("1Gi", ["--dry-run"], False, False),
            # The lease is taken to read inside it; nothing is applied.
            ("8Gi", [], False, True),
        ],
        ids=["small_is_resized", "dry_run_does_not_resize", "large_enough_is_left_alone"],
    )
    def test_resize_decision(self, size, args, applied, lock_taken):
        with _admin_mgr(TmpVolume(True, size)) as (mgr, lock):
            r = runner.invoke(admin_app, ["repair-operator", *args])
        assert r.exit_code == 0, r.output
        assert bool(lock.called) is lock_taken
        if applied:
            mgr.apply_controller_tmp_size.assert_called_once_with("8Gi")
        else:
            mgr.apply_controller_tmp_size.assert_not_called()
            mgr._set_watch_list_impl.assert_not_called()


class TestInstallCommand:
    def test_rejects_a_size_below_the_floor(self):
        with _admin_mgr(TmpVolume(True, "1Gi")) as (mgr, _lock):
            r = runner.invoke(admin_app, ["install-spark-operator", "--controller-tmp-size", "1Gi"])
        assert r.exit_code == 2
        mgr.install.assert_not_called()
