"""The c360 silver build stops on an empty bronze whenever it measured the
size, including when a size override of 0 sent it to the profile."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.mark.parametrize("override", [None, "0", "-1"])
def test_measured_empty_bronze_exits_1(load_script, monkeypatch, override):
    common = load_script("common")
    monkeypatch.setattr(common, "profile_bronze_data", lambda _spark, _path: (0.0, 1.0))
    if override is None:
        monkeypatch.delenv("LB_SILVER_SIZE_GB", raising=False)
    else:
        monkeypatch.setenv("LB_SILVER_SIZE_GB", override)
    spark = MagicMock()
    spark.conf.get.side_effect = lambda _key, default=None: default
    with pytest.raises(SystemExit) as exc:
        common.silver_input(spark, "s3a://lb-bronze/customer/interactions/")
    assert exc.value.code == 1
    spark.stop.assert_called_once()


def test_size_override_skips_the_profile(load_script, monkeypatch):
    common = load_script("common")
    monkeypatch.setattr(common, "profile_bronze_data", MagicMock())
    monkeypatch.setenv("LB_SILVER_SIZE_GB", "250")
    spark = MagicMock()
    spark.conf.get.side_effect = lambda _key, default=None: default
    assert common.silver_input(spark, "s3a://b/") == (250.0, 1.0, "operator_override")
    common.profile_bronze_data.assert_not_called()
