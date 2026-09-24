"""Deploy must refuse fast when the Stackable Hive operator is not running
(2026-09-24 live run: CRDs were present, helm leaves them behind on
uninstall, but no operator ran, so deploy waited out a 600 s timeout)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lakebench.modules.catalogs.hive.deployer import HiveDeployer


def _deployer():
    d = HiveDeployer.__new__(HiveDeployer)
    return d


def test_crds_present_but_operator_absent_is_unavailable():
    d = _deployer()
    with (
        patch.object(
            HiveDeployer,
            "_check_stackable_crds_raw",
            return_value=dict.fromkeys(HiveDeployer._REQUIRED_CRDS, True),
        ),
        patch("kubernetes.client.CoreV1Api") as core,
    ):
        core.return_value.list_pod_for_all_namespaces.side_effect = lambda **kw: MagicMock(
            items=[] if "hive-operator" in kw["label_selector"] else [object()]
        )
        assert d._is_stackable_available() is False
        status = d._check_stackable_crds()
    assert status["hiveclusters.hive.stackable.tech"] is False
    assert status["secretclasses.secrets.stackable.tech"] is True


def test_operator_state_unreadable_does_not_block():
    d = _deployer()
    with (
        patch.object(
            HiveDeployer,
            "_check_stackable_crds_raw",
            return_value=dict.fromkeys(HiveDeployer._REQUIRED_CRDS, True),
        ),
        patch("kubernetes.client.CoreV1Api") as core,
    ):
        core.return_value.list_pod_for_all_namespaces.side_effect = RuntimeError("forbidden")
        assert d._is_stackable_available() is True
