"""Tests for the Runtime abstraction.

The Runtime protocol lets deployers depend on a substrate rather than naming
Kubernetes directly, so a container runtime can be added for local mode.
K8sRuntime is a pass-through: these tests pin that it changes no behaviour.
"""

from unittest import mock

import pytest

from lakebench.runtime import ComponentSpec, ContainerPort, Mount, Runtime, RuntimeKind
from lakebench.runtime.protocol import K8sRuntime, as_k8s_client


class TestComponentSpec:
    """Runtime-neutral component description."""

    def test_minimal_spec(self):
        spec = ComponentSpec(name="garage", image="dxflrs/garage:v1.0.1")
        assert spec.name == "garage"
        assert spec.replicas == 1
        assert spec.env == {}
        assert spec.ports == []

    def test_spec_carries_ports_and_mounts(self):
        spec = ComponentSpec(
            name="garage",
            image="dxflrs/garage:v1.0.1",
            ports=[ContainerPort(container_port=3900, host_port=3900)],
            mounts=[Mount(source="/tmp/g", target="/etc/garage.toml", read_only=True)],
            env={"RUST_LOG": "info"},
        )
        assert spec.ports[0].host_port == 3900
        assert spec.mounts[0].read_only is True
        assert spec.env["RUST_LOG"] == "info"

    def test_defaults_are_not_shared_between_instances(self):
        """Mutable defaults must not leak across specs."""
        a = ComponentSpec(name="a", image="x")
        b = ComponentSpec(name="b", image="y")
        a.env["only_a"] = "1"
        assert b.env == {}


class TestK8sRuntimeIsPassThrough:
    """K8sRuntime must not alter existing Kubernetes behaviour."""

    def test_kind_is_kubernetes(self):
        assert K8sRuntime(mock.MagicMock()).kind() == RuntimeKind.KUBERNETES

    def test_exposes_underlying_client(self):
        client = mock.MagicMock()
        assert K8sRuntime(client).k8s is client

    def test_attribute_access_delegates_to_client(self):
        """Existing call sites using K8sClient methods keep working."""
        client = mock.MagicMock()
        client.namespace_exists.return_value = True
        runtime = K8sRuntime(client)
        assert runtime.namespace_exists("lakebench") is True
        client.namespace_exists.assert_called_once_with("lakebench")

    def test_container_methods_raise_rather_than_silently_noop(self):
        """A silent no-op would be worse than an explicit failure."""
        runtime = K8sRuntime(mock.MagicMock())
        spec = ComponentSpec(name="x", image="y")
        for call in (
            lambda: runtime.apply(spec),
            lambda: runtime.delete("x"),
            lambda: runtime.wait_ready("x"),
            lambda: runtime.exec("x", ["ls"]),
            lambda: runtime.logs("x"),
        ):
            with pytest.raises(NotImplementedError):
                call()

    def test_satisfies_runtime_protocol(self):
        assert isinstance(K8sRuntime(mock.MagicMock()), Runtime)


class TestAsK8sClient:
    """Transitional helper: accept a Runtime or a bare client."""

    def test_unwraps_k8s_runtime(self):
        client = mock.MagicMock()
        assert as_k8s_client(K8sRuntime(client)) is client

    def test_passes_through_bare_client(self):
        client = mock.MagicMock(spec=["namespace_exists"])
        assert as_k8s_client(client) is client

    def test_handles_none(self):
        assert as_k8s_client(None) is None


class TestGetEngineAcceptsBoth:
    """get_engine() must work with a Runtime or a bare client."""

    def _cfg(self):
        from lakebench.config import load_config

        return load_config("examples/hive-iceberg-spark-trino.yaml")

    def test_accepts_bare_k8s_client(self):
        from lakebench.engine import get_engine

        engine = get_engine(self._cfg(), mock.MagicMock())
        assert engine.engine_name() == "spark"

    def test_accepts_k8s_runtime_and_unwraps_it(self):
        """The job manager must receive the client, not the wrapper."""
        from lakebench.engine import get_engine

        client = mock.MagicMock()
        engine = get_engine(self._cfg(), K8sRuntime(client))
        assert engine.engine_name() == "spark"
        assert engine.k8s is client
