"""Tests for ContainerRuntime and GarageDeployer.

Mocked, so they run in the default suite. Both components were also live-tested
against podman during development; the bugs those tests encode were found that
way, not by inspection.
"""

from unittest import mock

import pytest

from lakebench.deploy.garage import GarageDeployer, GarageDeployError
from lakebench.runtime.container import (
    ContainerRuntime,
    ContainerRuntimeError,
    detect_container_cli,
)
from lakebench.runtime.protocol import ComponentSpec, ContainerPort, Mount, Runtime, RuntimeKind


def _completed(returncode=0, stdout="", stderr=""):
    return mock.Mock(returncode=returncode, stdout=stdout, stderr=stderr)


class TestDetectContainerCli:
    def test_prefers_podman(self):
        with mock.patch("shutil.which", side_effect=lambda c: f"/usr/bin/{c}"):
            assert detect_container_cli() == "podman"

    def test_falls_back_to_docker(self):
        with mock.patch(
            "shutil.which", side_effect=lambda c: "/usr/bin/docker" if c == "docker" else None
        ):
            assert detect_container_cli() == "docker"

    def test_actionable_error_when_neither_present(self):
        with mock.patch("shutil.which", return_value=None):
            with pytest.raises(ContainerRuntimeError) as exc:
                detect_container_cli()
        assert "podman" in str(exc.value) and "docker" in str(exc.value)
        assert "install" in str(exc.value).lower(), "error should tell the user what to do"


class TestRuntimeProtocol:
    def test_satisfies_protocol(self):
        assert isinstance(ContainerRuntime(cli="podman"), Runtime)

    def test_kind_is_container(self):
        assert ContainerRuntime(cli="podman").kind() is RuntimeKind.CONTAINER

    def test_namespace_qualifies_names(self):
        rt = ContainerRuntime(cli="podman", namespace="proj")
        assert rt._qualified("garage") == "proj-garage"

    def test_readiness_is_per_instance(self):
        """A class-level dict would leak readiness commands between runtimes."""
        a = ContainerRuntime(cli="podman")
        b = ContainerRuntime(cli="podman")
        a.register_readiness("x", ["true"])
        assert b._readiness == {}


class TestApplyCommandConstruction:
    def _capture(self, spec, cli="podman", running=False, image=""):
        rt = ContainerRuntime(cli=cli, namespace="lb")
        rt._is_running = mock.Mock(return_value=running)  # type: ignore[method-assign]
        rt._image_of = mock.Mock(return_value=image)  # type: ignore[method-assign]
        with mock.patch("subprocess.run", return_value=_completed()) as run:
            rt.apply(spec)
        return [c.args[0] for c in run.call_args_list if "run" in c.args[0]]

    def test_mount_options_are_comma_separated(self):
        """podman rejects 'src:dst:ro:Z'; options must be 'ro,Z'."""
        spec = ComponentSpec(
            name="g",
            image="img",
            mounts=[Mount(source="/h/f", target="/c/f", read_only=True)],
        )
        cmd = self._capture(spec)[0]
        arg = cmd[cmd.index("-v") + 1]
        assert arg == "/h/f:/c/f:ro,Z"
        assert ":ro:Z" not in arg

    def test_docker_omits_selinux_relabel(self):
        spec = ComponentSpec(
            name="g", image="img", mounts=[Mount(source="/h", target="/c", read_only=True)]
        )
        cmd = self._capture(spec, cli="docker")[0]
        assert cmd[cmd.index("-v") + 1] == "/h:/c:ro"

    def test_read_write_mount_on_podman_still_relabels(self):
        spec = ComponentSpec(name="g", image="img", mounts=[Mount(source="/h", target="/c")])
        cmd = self._capture(spec)[0]
        assert cmd[cmd.index("-v") + 1] == "/h:/c:Z"

    def test_ports_env_and_labels(self):
        spec = ComponentSpec(
            name="g",
            image="img",
            ports=[ContainerPort(container_port=3900, host_port=3910)],
            env={"K": "V"},
            labels={"a": "b"},
        )
        cmd = self._capture(spec)[0]
        assert "3910:3900" in cmd
        assert "K=V" in cmd
        assert "a=b" in cmd
        assert "lakebench.namespace=lb" in cmd

    def test_port_defaults_to_same_host_port(self):
        spec = ComponentSpec(name="g", image="img", ports=[ContainerPort(container_port=3900)])
        assert "3900:3900" in self._capture(spec)[0]


class TestApplyReuse:
    """Recreating a container discards its state, so reuse is correctness."""

    def test_reuses_running_container_with_same_image(self):
        rt = ContainerRuntime(cli="podman", namespace="lb")
        rt._is_running = mock.Mock(return_value=True)  # type: ignore[method-assign]
        rt._image_of = mock.Mock(return_value="img")  # type: ignore[method-assign]
        with mock.patch("subprocess.run", return_value=_completed()) as run:
            rt.apply(ComponentSpec(name="g", image="img"))
        assert run.call_count == 0, "must not recreate a healthy matching container"

    def test_recreates_when_image_differs(self):
        rt = ContainerRuntime(cli="podman", namespace="lb")
        rt._is_running = mock.Mock(return_value=True)  # type: ignore[method-assign]
        rt._image_of = mock.Mock(return_value="old")  # type: ignore[method-assign]
        with mock.patch("subprocess.run", return_value=_completed()) as run:
            rt.apply(ComponentSpec(name="g", image="new"))
        assert run.call_count > 0

    def test_replace_forces_recreate(self):
        rt = ContainerRuntime(cli="podman", namespace="lb")
        rt._is_running = mock.Mock(return_value=True)  # type: ignore[method-assign]
        rt._image_of = mock.Mock(return_value="img")  # type: ignore[method-assign]
        with mock.patch("subprocess.run", return_value=_completed()) as run:
            rt.apply(ComponentSpec(name="g", image="img"), replace=True)
        assert run.call_count > 0


class TestLifecycle:
    def test_delete_does_not_raise_when_absent(self):
        rt = ContainerRuntime(cli="podman")
        with mock.patch("subprocess.run", return_value=_completed(1, stderr="no such container")):
            rt.delete("missing")  # must not raise

    def test_apply_failure_raises_with_stderr(self):
        rt = ContainerRuntime(cli="podman")
        rt._is_running = mock.Mock(return_value=False)  # type: ignore[method-assign]
        with mock.patch("subprocess.run", return_value=_completed(125, stderr="port in use")):
            with pytest.raises(ContainerRuntimeError, match="port in use"):
                rt.apply(ComponentSpec(name="g", image="img"))

    def test_exec_returns_code_and_merged_output(self):
        rt = ContainerRuntime(cli="podman")
        with mock.patch("subprocess.run", return_value=_completed(3, "out", "err")):
            assert rt.exec("g", ["ls"]) == (3, "outerr")

    def test_wait_ready_returns_false_on_timeout(self):
        rt = ContainerRuntime(cli="podman")
        rt._is_running = mock.Mock(return_value=False)  # type: ignore[method-assign]
        with mock.patch("time.sleep"):
            assert rt.wait_ready("g", timeout=0) is False

    def test_wait_ready_true_when_running_and_no_probe(self):
        rt = ContainerRuntime(cli="podman")
        rt._is_running = mock.Mock(return_value=True)  # type: ignore[method-assign]
        assert rt.wait_ready("g", timeout=5) is True

    def test_wait_ready_runs_registered_probe(self):
        rt = ContainerRuntime(cli="podman")
        rt._is_running = mock.Mock(return_value=True)  # type: ignore[method-assign]
        rt.register_readiness("g", ["/garage", "status"])
        rt.exec = mock.Mock(return_value=(0, "ok"))  # type: ignore[method-assign]
        assert rt.wait_ready("g", timeout=5) is True
        rt.exec.assert_called_with("g", ["/garage", "status"])

    def test_list_managed_strips_namespace_prefix(self):
        rt = ContainerRuntime(cli="podman", namespace="lb")
        with mock.patch("subprocess.run", return_value=_completed(0, "lb-garage\nlb-duckdb\n")):
            assert rt.list_managed() == ["garage", "duckdb"]

    def test_host_port_parses_inspect_json(self):
        rt = ContainerRuntime(cli="podman", namespace="lb")
        payload = '{"Ports": {"3900/tcp": [{"HostPort": "3910"}]}}'
        with mock.patch("subprocess.run", return_value=_completed(0, payload)):
            assert rt.host_port("garage", 3900) == 3910

    def test_host_port_returns_none_on_bad_json(self):
        rt = ContainerRuntime(cli="podman")
        with mock.patch("subprocess.run", return_value=_completed(0, "not json")):
            assert rt.host_port("g", 3900) is None


class TestGarageDeployer:
    def _runtime(self):
        rt = mock.MagicMock(spec=ContainerRuntime)
        rt.wait_ready.return_value = True
        return rt

    def test_raises_when_container_never_ready(self, tmp_path):
        rt = self._runtime()
        rt.wait_ready.return_value = False
        rt.logs.return_value = "bind: address already in use"
        dep = GarageDeployer(rt, config_dir=str(tmp_path))
        with pytest.raises(GarageDeployError, match="did not become ready"):
            dep.deploy(timeout=1)

    def test_config_and_data_dirs_created(self, tmp_path):
        rt = self._runtime()
        rt.exec.return_value = (0, "Key ID: GKabc\nSecret key: s3cret\n")
        GarageDeployer(rt, config_dir=str(tmp_path)).deploy()
        assert (tmp_path / "garage.toml").exists()
        assert (tmp_path / "meta").is_dir(), "bind-mount target must exist on the host"
        assert (tmp_path / "data").is_dir()

    def test_config_carries_region_and_port(self, tmp_path):
        rt = self._runtime()
        rt.exec.return_value = (0, "Key ID: GKabc\nSecret key: s3cret\n")
        GarageDeployer(rt, config_dir=str(tmp_path), region="eu-west-1", port=4000).deploy()
        toml = (tmp_path / "garage.toml").read_text()
        assert 's3_region = "eu-west-1"' in toml
        assert 'api_bind_addr = "[::]:4000"' in toml

    def test_reuses_existing_key_instead_of_creating_duplicate(self, tmp_path):
        """garage key create is NOT idempotent: it silently makes duplicates."""
        rt = self._runtime()
        calls = []

        def fake_exec(_name, command):
            calls.append(command)
            if command[:3] == ["/garage", "node", "id"]:
                return 0, "abc123@127.0.0.1:3901"
            if command[:3] == ["/garage", "key", "list"]:
                return 0, "List of keys:\n  GK1a2b3c4d5e6f7890abcdef12  lakebench\n"
            if command[:3] == ["/garage", "key", "info"]:
                return 0, "Key ID: GK1a2b3c4d5e6f7890abcdef12\nSecret key: oldsecret\n"
            return 0, ""

        rt.exec.side_effect = fake_exec
        creds = GarageDeployer(rt, config_dir=str(tmp_path)).deploy()

        assert creds.access_key == "GK1a2b3c4d5e6f7890abcdef12"
        assert creds.secret_key == "oldsecret"
        assert not any(c[:3] == ["/garage", "key", "create"] for c in calls), (
            "must not create a second key with the same name"
        )

    def test_creates_key_when_none_exists(self, tmp_path):
        rt = self._runtime()

        def fake_exec(_name, command):
            if command[:3] == ["/garage", "node", "id"]:
                return 0, "abc123@127.0.0.1:3901"
            if command[:3] == ["/garage", "key", "list"]:
                return 0, "List of keys:\n"
            if command[:3] == ["/garage", "key", "create"]:
                return 0, "Key ID: GKnew\nSecret key: news3cret\n"
            return 0, ""

        rt.exec.side_effect = fake_exec
        creds = GarageDeployer(rt, config_dir=str(tmp_path)).deploy()
        assert creds.access_key == "GKnew"

    def test_unparseable_credentials_raise(self, tmp_path):
        rt = self._runtime()
        rt.exec.return_value = (0, "unexpected output")
        with pytest.raises(GarageDeployError, match="Could not parse"):
            GarageDeployer(rt, config_dir=str(tmp_path)).deploy()

    def test_buckets_created_and_granted(self, tmp_path):
        rt = self._runtime()
        seen = []

        def fake_exec(_name, command):
            seen.append(command)
            if command[:3] == ["/garage", "node", "id"]:
                return 0, "abc123@127.0.0.1:3901"
            if command[:3] == ["/garage", "key", "list"]:
                return 0, "List of keys:\n"
            if command[:3] == ["/garage", "key", "create"]:
                return 0, "Key ID: GKnew\nSecret key: s\n"
            return 0, ""

        rt.exec.side_effect = fake_exec
        GarageDeployer(rt, config_dir=str(tmp_path), buckets=("b1", "b2")).deploy()
        created = [c for c in seen if c[:3] == ["/garage", "bucket", "create"]]
        assert {c[3] for c in created} == {"b1", "b2"}
        grants = [c for c in seen if c[:3] == ["/garage", "bucket", "allow"]]
        assert len(grants) == 2
        assert all("GKnew" in c for c in grants), "grants must address the key by ID"

    def test_destroy_removes_container(self, tmp_path):
        rt = self._runtime()
        GarageDeployer(rt, config_dir=str(tmp_path)).destroy()
        rt.delete.assert_called_once_with("garage")

    def test_persists_state_via_host_mounts(self, tmp_path):
        """Without these, a recreate silently mints new credentials."""
        rt = self._runtime()
        rt.exec.return_value = (0, "Key ID: GKa\nSecret key: s\n")
        GarageDeployer(rt, config_dir=str(tmp_path)).deploy()
        spec = rt.apply.call_args.args[0]
        targets = {m.target for m in spec.mounts}
        assert "/var/lib/garage/meta" in targets
        assert "/var/lib/garage/data" in targets
