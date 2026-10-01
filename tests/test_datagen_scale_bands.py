"""Datagen file size is fixed at 64mb and scale is banded per workload
(owner decisions 2026-09-29, LB-204)."""

from __future__ import annotations

import pytest
import typer
from pydantic import ValidationError

from lakebench.config.schema import DatagenConfig
from lakebench.config.support import (
    DATAGEN_SCALE_BANDS,
    SUPPORTED,
    UNSUPPORTED,
    UNVERIFIED,
    Validation,
    datagen_scale_problem,
    support_state,
)

COMBO = ("hive", "iceberg", "spark", "trino")


@pytest.mark.parametrize("value", ["64mb", "64MB", " 64Mb "])
def test_file_size_accepts_any_spelling_of_64mb(value: str) -> None:
    assert DatagenConfig(file_size=value).file_size == "64mb"


@pytest.mark.parametrize("value", ["128mb", "32mb", "1gb", 64])
def test_file_size_refuses_every_other_size(value: object) -> None:
    with pytest.raises(ValidationError, match="fixed at 64mb"):
        DatagenConfig(file_size=value)


def test_cleanup_loaders_tolerate_an_old_file_size() -> None:
    """destroy and clean must still load a config written with an old size."""
    with pytest.warns(DeprecationWarning, match="fixed at 64mb"):
        dg = DatagenConfig.model_validate({"file_size": "128mb"}, context={"purpose": "teardown"})
    assert dg.file_size == "64mb"


def test_file_size_default_is_64mb() -> None:
    assert DatagenConfig().file_size == "64mb"


@pytest.mark.parametrize("workload", sorted(DATAGEN_SCALE_BANDS))
def test_band_edges(workload: str) -> None:
    supported_max, ceiling = DATAGEN_SCALE_BANDS[workload]
    assert 0 < supported_max <= ceiling
    assert datagen_scale_problem(workload, supported_max) is None
    assert datagen_scale_problem(workload, ceiling)[0] == UNVERIFIED  # type: ignore[index]
    assert datagen_scale_problem(workload, ceiling + 1)[0] == UNSUPPORTED  # type: ignore[index]


def test_bands_are_per_workload(monkeypatch: pytest.MonkeyPatch) -> None:
    """c360 has no world term, so it must be judged by its own band, never
    the financial one."""
    import lakebench.config.support as support

    monkeypatch.setitem(support.DATAGEN_SCALE_BANDS, "financial", (10.0, 20.0))
    assert datagen_scale_problem("financial", 21)[0] == UNSUPPORTED  # type: ignore[index]
    assert datagen_scale_problem("customer360", 21) is None


def test_unknown_workload_or_scale_has_no_band() -> None:
    assert datagen_scale_problem("nosuch", 1e9) is None
    assert datagen_scale_problem("financial", None) is None


def _record(workload: str) -> dict[tuple[str, str, str], Validation]:
    from lakebench.config.support import recipe_for

    recipe = str(recipe_for(*COMBO))
    return {(workload, recipe, "batch"): Validation(workload, recipe, "batch", "abc", ("run-x",))}


def test_support_state_refuses_above_ceiling_even_when_validated() -> None:
    ceiling = DATAGEN_SCALE_BANDS["financial"][1]
    out = support_state(
        "financial", *COMBO, "batch", record=_record("financial"), scale=ceiling + 1
    )
    assert out["state"] == UNSUPPORTED
    assert "ceiling" in out["basis"]
    assert "validation_runs" not in out


def test_support_state_downgrades_validated_run_above_measured_scale() -> None:
    supported_max, ceiling = DATAGEN_SCALE_BANDS["financial"]
    if ceiling == supported_max:
        pytest.skip("no unverified band")
    out = support_state("financial", *COMBO, "batch", record=_record("financial"), scale=ceiling)
    assert out["state"] == UNVERIFIED
    assert out["scale_note"] == out["basis"]


def test_support_state_keeps_supported_within_band() -> None:
    supported_max, _ = DATAGEN_SCALE_BANDS["financial"]
    out = support_state(
        "financial", *COMBO, "batch", record=_record("financial"), scale=supported_max
    )
    assert out["state"] == SUPPORTED
    assert "scale_note" not in out


def test_local_runs_are_not_banded() -> None:
    out = support_state("customer360", *COMBO, "batch", system="local", scale=1e9)
    assert "scale_note" not in out


class _Datagen:
    def __init__(self, scale: float) -> None:
        self._scale = scale

    def get_effective_scale(self) -> float:
        return self._scale


class _Cfg:
    def __init__(self, workload: str, scale: float) -> None:
        class _Schema:
            value = workload

        class _Workload:
            schema_type = _Schema()
            datagen = _Datagen(scale)

        class _Arch:
            pass

        self.architecture = _Arch()
        self.architecture.workload = _Workload()  # type: ignore[attr-defined]


def test_check_datagen_scale_refuses_above_ceiling() -> None:
    from lakebench.cli._helpers import check_datagen_scale

    ceiling = DATAGEN_SCALE_BANDS["financial"][1]
    with pytest.raises(typer.Exit):
        check_datagen_scale(_Cfg("financial", ceiling + 1))


def test_check_datagen_scale_passes_within_ceiling() -> None:
    from lakebench.cli._helpers import check_datagen_scale

    supported_max, ceiling = DATAGEN_SCALE_BANDS["financial"]
    check_datagen_scale(_Cfg("financial", supported_max))
    check_datagen_scale(_Cfg("financial", ceiling))
