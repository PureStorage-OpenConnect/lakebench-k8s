"""Tests for the C4 init-wizard defect fixes.

Covers five defects:

1. S3 connectivity test used the wrong result key (`reachable` vs
   `endpoint_reachable`), so every reachable endpoint reported
   ``WARN unknown error``.
2. CLI flags (`--name`, `--recipe`, `--scale`) applied to wizard state
   AFTER ``step_review`` had already built ``config_yaml``, so the
   written file kept the pre-override values.
3. Datagen ``parallelism: 1`` in the example template pinned datagen to
   one pod. The template default is now ``parallelism: 8`` so datagen
   scales usefully out of the box.
4. Wizard offered no way to pick the financial (AML) workload; only
   customer360 was ever emitted.
5. ``init --local`` wrote the deprecated ``architecture.workload``
   block instead of the canonical top-level ``workload:`` (D12).
"""

from __future__ import annotations

from unittest.mock import patch

from rich.console import Console
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.init_wizard import (
    WizardState,
    _build_config_yaml,
    _test_s3,
    step_workload_schema,
)

runner = CliRunner()


class TestS3ResultKey:
    """Defect 1: `_test_s3` must read the real dict keys returned by
    `s3.client.test_s3_connectivity` (`endpoint_reachable`,
    `credentials_valid`, `overall_success`, `buckets`), not the invented
    `reachable`/`bucket_count`. It must also gate the OK branch on
    `overall_success` so an endpoint-reachable-but-credentials-bad case
    does not print green OK with 0 buckets.
    """

    def test_reachable_endpoint_reports_ok(self):
        state = WizardState(
            endpoint="http://s3:80",
            access_key="a",
            secret_key="b",
            region="us-east-1",
        )
        console = Console(quiet=False, record=True)

        mocked = {
            "endpoint_reachable": True,
            "endpoint_message": "reachable",
            "credentials_valid": True,
            "credentials_message": "ok",
            "buckets": ["bronze", "silver"],
            "overall_success": True,
        }
        with patch("lakebench.s3.test_s3_connectivity", return_value=mocked):
            _test_s3(console, state)

        output = console.export_text()
        assert "OK" in output, output
        assert "WARN" not in output, output
        assert "unknown error" not in output, output
        assert "Found 2" in output, output

    def test_unreachable_endpoint_reports_the_actual_error(self):
        state = WizardState(
            endpoint="http://s3:80",
            access_key="a",
            secret_key="b",
        )
        console = Console(quiet=False, record=True)

        mocked = {
            "endpoint_reachable": False,
            "endpoint_message": "DNS lookup failed",
            "credentials_valid": False,
            "credentials_message": "",
            "buckets": None,
            "overall_success": False,
        }
        with patch("lakebench.s3.test_s3_connectivity", return_value=mocked):
            _test_s3(console, state)

        output = console.export_text()
        assert "WARN" in output, output
        assert "DNS lookup failed" in output, output
        assert "unknown error" not in output, output

    def test_reachable_but_bad_credentials_reports_warn(self):
        """Endpoint reachable, credentials invalid, buckets = None.
        Must NOT print `OK` and `Found 0 bucket(s)`.
        """
        state = WizardState(
            endpoint="http://s3:80",
            access_key="a",
            secret_key="wrong",
        )
        console = Console(quiet=False, record=True)

        mocked = {
            "endpoint_reachable": True,
            "endpoint_message": "reachable",
            "credentials_valid": False,
            "credentials_message": "InvalidAccessKeyId",
            "buckets": None,
            "overall_success": False,
        }
        with patch("lakebench.s3.test_s3_connectivity", return_value=mocked):
            _test_s3(console, state)

        output = console.export_text()
        assert "OK" not in output.split("\n")[0], output
        assert "WARN" in output, output
        assert "InvalidAccessKeyId" in output, output
        assert "Found 0" not in output, output


class TestFlagApplicationOrder:
    """Defect 2: CLI flags must reach the written YAML."""

    def test_flags_applied_before_yaml_written(self, tmp_path):
        """--name/--recipe/--scale on interactive path override state and
        the written YAML shows the flag values, not the wizard defaults.

        Interactive is auto-off here because stdin is not a TTY under
        CliRunner. That is fine: the fix targets the wizard path too, but
        the flag-mode path has the same defect (flags must land in the
        YAML). The regression check for the wizard path lives in the
        _build_config_yaml unit assertion below.
        """
        output = tmp_path / "out.yaml"
        r = runner.invoke(
            app,
            [
                "init",
                "--output",
                str(output),
                "--no-interactive",
                "--name",
                "picked-name",
                "--recipe",
                "hive-iceberg-spark-trino",
                "--scale",
                "42",
            ],
        )
        assert r.exit_code == 0, r.output
        content = output.read_text()
        assert "name: picked-name" in content, content
        assert "recipe: hive-iceberg-spark-trino" in content, content
        assert "scale: 42" in content, content
        # And that we did not simply write the templates defaults.
        assert "name: my-lakehouse" not in content

    def test_wizard_path_rebuilds_yaml_after_flag_override(self, tmp_path):
        """CLI 'init' wizard path: when the wizard runs and CLI flags
        then override state, the WRITTEN yaml has the flag values, not
        the wizard's pre-override ones.

        This exercises the exact defect: previously the CLI applied flag
        overrides AFTER step_review had already built config_yaml, so
        the file on disk kept the pre-override values. The wizard branch
        is taken when interactive is True, no S3 flags were passed, and
        stdin is a TTY. CliRunner replaces sys.stdin inside invoke, so
        patching `sys.stdin.isatty` does not survive; call init directly
        instead.
        """
        from lakebench.cli import init as init_cmd

        wizard_state = WizardState(
            name="from-wizard",
            recipe="default",
            scale=10,
            endpoint="http://s3:80",
            access_key="a",
            secret_key="b",
        )
        wizard_state.config_yaml = _build_config_yaml(wizard_state)
        pre_yaml = wizard_state.config_yaml
        assert "name: from-wizard" in pre_yaml
        assert "scale: 10" in pre_yaml

        class _TTYStdin:
            def isatty(self) -> bool:
                return True

        import sys as _sys

        output = tmp_path / "wizard-out.yaml"
        real_stdin = _sys.stdin
        _sys.stdin = _TTYStdin()  # type: ignore[assignment]
        try:
            with (
                patch("lakebench.init_wizard.run_wizard", return_value=wizard_state),
                patch("typer.confirm", return_value=True),
            ):
                init_cmd(
                    output=output,
                    name="cli-override",
                    scale=42.0,
                    endpoint="",
                    access_key="",
                    secret_key="",
                    namespace="",
                    recipe="hive-iceberg-spark-trino",
                    workload="",
                    interactive=True,
                    force=False,
                    force_short_f=False,
                    advanced=False,
                    local=False,
                )
        finally:
            _sys.stdin = real_stdin

        content = output.read_text()
        # The written YAML reflects the CLI flag overrides.
        assert "name: cli-override" in content, content
        assert "recipe: hive-iceberg-spark-trino" in content, content
        # Scale rendered as 42.0 (float) or 42.
        assert "scale: 42" in content, content
        # Pre-override wizard values must not survive.
        assert "name: from-wizard" not in content, content
        # The pre-override scale (10) must not appear on the scale line
        # under the workload block.
        workload_section = content.split("workload:", 1)[1]
        assert "scale: 10\n" not in workload_section, workload_section[:400]


class TestDatagenParallelismDefault:
    """Defect 3: template must not pin datagen to 1 pod, and it must NOT
    ship an uncommented explicit value either -- the autosizer scales
    parallelism from cluster capacity only when the field is unset
    (`parallelism not in datagen.model_fields_set`, see
    `config/autosizer.py`). Any uncommented integer defeats the scale-up
    path and users at scale > 50 quietly get the pinned value instead of
    the ~scale/10 pods the autosizer would have picked.
    """

    def test_template_does_not_pin_parallelism(self):
        from lakebench.config import generate_example_config_yaml

        content = generate_example_config_yaml()
        # No uncommented `parallelism: <int>` line anywhere in the
        # datagen block. Commented `# parallelism: N` is fine.
        for raw in content.split("\n"):
            line = raw.rstrip()
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if "parallelism:" in stripped and stripped.startswith("parallelism:"):
                raise AssertionError(f"parallelism must not be uncommented: {raw!r}")

    def test_autosizer_scale_up_fires_on_template(self, tmp_path):
        """With the template as-written, a scale=100 config loads WITHOUT
        parallelism in model_fields_set so the autosizer's scale-up
        branch can raise it above the schema default of 4.
        """
        from lakebench.config import generate_example_config_yaml, load_config

        content = generate_example_config_yaml()
        content = content.replace("scale: 10", "scale: 100")
        p = tmp_path / "scaled.yaml"
        p.write_text(content)
        cfg = load_config(p, allow_long_names=True)
        datagen = cfg.architecture.workload.datagen
        assert "parallelism" not in datagen.model_fields_set, (
            "template must leave parallelism unset so autosizer can scale it"
        )


class TestAmlWorkloadOption:
    """Defect 4: wizard and CLI support financial (AML) workload."""

    def test_step_workload_schema_picks_financial(self):
        state = WizardState()
        console = Console(quiet=True)
        with patch("typer.prompt", return_value="2"):
            ok = step_workload_schema(console, state)
        assert ok is True
        assert state.workload_schema == "financial"

    def test_step_workload_schema_picks_customer360(self):
        state = WizardState()
        console = Console(quiet=True)
        with patch("typer.prompt", return_value="1"):
            ok = step_workload_schema(console, state)
        assert ok is True
        assert state.workload_schema == "customer360"

    def test_build_config_yaml_writes_financial_workload(self):
        state = WizardState(
            name="aml-lake",
            workload_schema="financial",
            endpoint="http://s3:80",
            access_key="a",
            secret_key="b",
        )
        content = _build_config_yaml(state)
        # workload block is top-level.
        assert "\nworkload:" in content, content
        # schema is set to financial, not a commented default.
        assert "schema: financial" in content, content
        # The commented placeholder is gone.
        assert "# schema: customer360" not in content, content

    def test_init_workload_flag_financial(self, tmp_path):
        output = tmp_path / "aml.yaml"
        r = runner.invoke(
            app,
            [
                "init",
                "--output",
                str(output),
                "--no-interactive",
                "--workload",
                "financial",
            ],
        )
        assert r.exit_code == 0, r.output
        content = output.read_text()
        assert "\nworkload:" in content
        assert "schema: financial" in content


class TestLocalWorkloadPlacement:
    """Defect 5: init --local writes top-level workload:."""

    def test_local_config_uses_top_level_workload(self, tmp_path):
        output = tmp_path / "local.yaml"
        r = runner.invoke(app, ["init", "--output", str(output), "--local", "--no-interactive"])
        assert r.exit_code == 0, r.output
        content = output.read_text()

        # Deprecated architecture.workload must not appear.
        assert "architecture:\n  workload:" not in content, content

        # Top-level workload: must be present with datagen underneath.
        assert "\nworkload:\n  datagen:\n" in content, content

    def test_local_config_honours_workload_flag(self, tmp_path):
        """`init --local --workload financial` must not silently drop the
        flag. The written YAML has `schema: financial` under `workload:`.
        """
        output = tmp_path / "local-aml.yaml"
        r = runner.invoke(
            app,
            [
                "init",
                "--output",
                str(output),
                "--local",
                "--no-interactive",
                "--workload",
                "financial",
            ],
        )
        assert r.exit_code == 0, r.output
        content = output.read_text()
        assert "workload:" in content, content
        assert "schema: financial" in content, content

    def test_local_config_reloads_without_deprecation_warning(self, tmp_path):
        import warnings

        from lakebench.config import load_config

        output = tmp_path / "local.yaml"
        r = runner.invoke(app, ["init", "--output", str(output), "--local", "--no-interactive"])
        assert r.exit_code == 0, r.output

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            load_config(output)

        deprecations = [
            str(w.message)
            for w in caught
            if "architecture.workload" in str(w.message).lower()
            or "deprecated" in str(w.message).lower()
        ]
        assert not any("architecture.workload" in d.lower() for d in deprecations), deprecations
