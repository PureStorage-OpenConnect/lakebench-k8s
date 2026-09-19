"""Regression tests for datagen/generate.py.

These pin the properties the Customer 360 datagen has to preserve for
determinism, downstream schema, and cross-pod correctness. Every test
here corresponds to a real defect that was either fixed recently or
could plausibly land unnoticed without the test:

- `test_timezone_utc_only`               <- fixed 2026-09-18 TZ leak
- `test_parquet_compression_*`           <- fixed 2026-09-18 codec-parity bug
- `test_bytes_per_row_codec_aware`       <- fixed 2026-09-18 ZSTD file-size
                                            regression that reintroduced the
                                            small-file problem
- `test_generator_determinism_*`         <- guards the (seed, file_id, scale)
                                            -> same-content contract
- `test_schema_columns_*`                <- guards silver/downstream reads
- `test_config_defaults`                 <- guards silent flag drift

Run with: pytest datagen/tests/ -v
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
import pytest


# ---------------------------------------------------------------------------
# _parquet_compression -- default, valid codecs, range guard
# ---------------------------------------------------------------------------


@pytest.fixture
def clean_env(monkeypatch):
    """Ensure DG_COMPRESSION doesn't leak between tests."""
    monkeypatch.delenv("DG_COMPRESSION", raising=False)
    yield


def test_parquet_compression_default_is_zstd1(clean_env):
    from generate import _parquet_compression

    assert _parquet_compression() == ("zstd", 1)


@pytest.mark.parametrize(
    "env_val,expected",
    [
        ("snappy", ("snappy", None)),
        ("zstd", ("zstd", 1)),
        ("zstd1", ("zstd", 1)),
        ("zstd3", ("zstd", 3)),
        ("zstd9", ("zstd", 9)),
        ("zstd22", ("zstd", 22)),
        ("lz4", ("lz4", None)),
        ("none", ("none", None)),
        ("uncompressed", ("none", None)),
        # case + whitespace tolerance
        ("  ZSTD3 ", ("zstd", 3)),
        ("SNAPPY", ("snappy", None)),
    ],
)
def test_parquet_compression_valid(env_val, expected, monkeypatch):
    monkeypatch.setenv("DG_COMPRESSION", env_val)
    from generate import _parquet_compression

    assert _parquet_compression() == expected


@pytest.mark.parametrize("bad", ["zstd0", "zstd23", "zstd99", "zstd-1"])
def test_parquet_compression_out_of_range(bad, monkeypatch):
    monkeypatch.setenv("DG_COMPRESSION", bad)
    from generate import _parquet_compression

    with pytest.raises(ValueError, match="out of range|could not parse"):
        _parquet_compression()


@pytest.mark.parametrize("bad", ["brotli", "gzip", "garbage", "zst"])
def test_parquet_compression_unknown(bad, monkeypatch):
    monkeypatch.setenv("DG_COMPRESSION", bad)
    from generate import _parquet_compression

    with pytest.raises(ValueError, match="expected snappy"):
        _parquet_compression()


# ---------------------------------------------------------------------------
# generate_timestamps -- UTC only (regression: naive fromtimestamp leaked TZ)
# ---------------------------------------------------------------------------


def test_timezone_utc_only():
    """`generate_timestamps` must return tz-aware UTC datetimes so that two
    pods running under different `TZ` environment settings produce the same
    wall-clock strings from the same seed."""
    from generate import generate_timestamps

    # Minimal Config-shaped object -- avoid full Config construction.
    class C:
        timestamp_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        timestamp_end = datetime(2024, 12, 31, tzinfo=timezone.utc)

    rng = np.random.default_rng(42)
    ts = generate_timestamps(10, rng, C)
    assert len(ts) == 10
    for t in ts:
        assert isinstance(t, datetime)
        assert t.tzinfo is not None, "timestamp must be tz-aware, got naive datetime"
        assert t.utcoffset().total_seconds() == 0, (
            f"timestamp must be UTC, got offset {t.utcoffset()}"
        )


def test_timestamps_deterministic_given_seed():
    """Same seed -> same timestamps. Regressions here would break the
    (seed, file_id) -> same-content contract."""
    from generate import generate_timestamps

    class C:
        timestamp_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        timestamp_end = datetime(2024, 12, 31, tzinfo=timezone.utc)

    a = generate_timestamps(100, np.random.default_rng(42), C)
    b = generate_timestamps(100, np.random.default_rng(42), C)
    assert a == b


def test_timestamps_in_range():
    from generate import generate_timestamps

    class C:
        timestamp_start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        timestamp_end = datetime(2024, 12, 31, tzinfo=timezone.utc)

    ts = generate_timestamps(1000, np.random.default_rng(42), C)
    for t in ts:
        assert C.timestamp_start <= t <= C.timestamp_end


# ---------------------------------------------------------------------------
# Config bytes-per-row -- codec-aware (regression: SNAPPY-era 4200 with
# ZSTD default made files ~1/2 the target size).
# ---------------------------------------------------------------------------


def test_bytes_per_row_codec_aware(monkeypatch):
    """Under ZSTD-1 (default), rows_per_file for customer360 must reflect
    the smaller compressed size (~2200 bytes/row), not the SNAPPY-era
    4200. Otherwise files land at ~1/2 the target size."""
    from generate import Config

    # ZSTD-1 default
    monkeypatch.delenv("DG_COMPRESSION", raising=False)
    cfg_zstd = Config(
        target_tb=0.001, bucket="b", prefix="p",
        checkpoint_file="/tmp/x", file_size_mb=512, schema_name="customer360",
    )
    # 512 MiB / 2200 bytes/row = ~244,190 rows/file
    assert 200_000 <= cfg_zstd.rows_per_file <= 280_000, (
        f"ZSTD-1 rows_per_file {cfg_zstd.rows_per_file} outside expected 200K-280K range"
    )

    # SNAPPY
    monkeypatch.setenv("DG_COMPRESSION", "snappy")
    cfg_snappy = Config(
        target_tb=0.001, bucket="b", prefix="p",
        checkpoint_file="/tmp/x", file_size_mb=512, schema_name="customer360",
    )
    # 512 MiB / 4200 bytes/row = ~127,919 rows/file
    assert 100_000 <= cfg_snappy.rows_per_file <= 150_000, (
        f"SNAPPY rows_per_file {cfg_snappy.rows_per_file} outside expected 100K-150K range"
    )

    # ZSTD gives roughly 2x rows per file vs SNAPPY (the whole point of the fix).
    ratio = cfg_zstd.rows_per_file / cfg_snappy.rows_per_file
    assert 1.6 <= ratio <= 2.3, (
        f"ZSTD/SNAPPY row ratio {ratio:.2f} not in [1.6, 2.3] -- codec-aware sizing may be broken"
    )


@pytest.mark.parametrize(
    "codec,c360_min,c360_max,fin_min,fin_max",
    [
        # 512 MiB / bytes_per_row = expected rows/file. Bounds sit within
        # ~9% of the measured value; measurements are reproducible to <1%,
        # so any drift of a codec or a schema column that shifts rows/file
        # by more than ~10% trips this test, while normal noise doesn't.
        # Measured 2026-09-18 via scratchpad/measure_bpr.py:
        #   customer360 snappy 4332 -> 124k; zstd 2233 -> 240k;
        #                lz4    4356 -> 123k; none  4399 -> 122k.
        #   financial   snappy  199 -> 2.7M; zstd  133 -> 4.0M;
        #                lz4     207 -> 2.6M; none  292 -> 1.8M.
        ("snappy", 114_000, 133_000, 2_484_000, 2_912_000),
        ("zstd1",  221_000, 259_000, 3_720_000, 4_364_000),
        ("lz4",    113_000, 132_000, 2_395_000, 2_809_000),
        ("none",   112_000, 130_000, 1_697_000, 1_990_000),
    ],
)
def test_bytes_per_row_all_codecs(monkeypatch, codec, c360_min, c360_max, fin_min, fin_max):
    """Every codec entry in _BYTES_PER_ROW must produce plausible
    rows_per_file. Regression guard: LZ4 and none values were previously
    guesses (4000, 6500) rather than measurements; the "none" guess was
    off by 47% and would have made files ~50% oversized under an operator
    who explicitly picked uncompressed output."""
    from generate import Config

    monkeypatch.setenv("DG_COMPRESSION", codec)
    c360 = Config(
        target_tb=0.001, bucket="b", prefix="p",
        checkpoint_file="/tmp/x", file_size_mb=512, schema_name="customer360",
    )
    fin = Config(
        target_tb=0.001, bucket="b", prefix="p",
        checkpoint_file="/tmp/x", file_size_mb=512, schema_name="financial",
    )
    assert c360_min <= c360.rows_per_file <= c360_max, (
        f"customer360 {codec} rows_per_file={c360.rows_per_file} outside "
        f"[{c360_min}, {c360_max}] -- codec table likely miscalibrated"
    )
    assert fin_min <= fin.rows_per_file <= fin_max, (
        f"financial {codec} rows_per_file={fin.rows_per_file} outside "
        f"[{fin_min}, {fin_max}] -- codec table likely miscalibrated"
    )


def test_bytes_per_row_financial_smaller_than_customer360(monkeypatch):
    """Sanity: financial's per-row size must stay much smaller than
    Customer 360's (financial has no 2KB payload column)."""
    from generate import Config

    monkeypatch.delenv("DG_COMPRESSION", raising=False)
    c360 = Config(target_tb=0.001, bucket="b", prefix="p",
                  checkpoint_file="/tmp/x", file_size_mb=32, schema_name="customer360")
    fin = Config(target_tb=0.001, bucket="b", prefix="p",
                 checkpoint_file="/tmp/x", file_size_mb=32, schema_name="financial")
    # Financial should fit ~10-20x more rows per file than customer360
    assert fin.rows_per_file > 10 * c360.rows_per_file, (
        f"financial rows/file {fin.rows_per_file} not >> c360 rows/file {c360.rows_per_file}"
    )


# ---------------------------------------------------------------------------
# Generator determinism -- same (seed, file_id, scale) -> same table
# ---------------------------------------------------------------------------


def _cfg(**overrides):
    """Small Config with sensible defaults for generator tests. `scale` is
    a CLI arg, not a Config field -- callers should not pass it."""
    from generate import Config

    kwargs = dict(
        target_tb=0.0001, workers=1, seed=42, node_id=0, total_nodes=1,
        payload_kb=1, file_size_mb=4, bucket="b", prefix="p",
        checkpoint_file="/tmp/x", schema_name="customer360",
    )
    kwargs.update(overrides)
    return Config(**kwargs)


def test_customer360_generator_deterministic():
    """Same file_id + seed -> byte-identical parquet buffer. This is the
    contract the recipe explicitly documents (`same (seed, file_id, scale)
    -> same content`) and it was previously untested for Customer 360.

    Compares sha256 of serialized parquet bytes. This is a stronger check
    than `pyarrow.Table.equals()` (which treats NaN as not-equal per IEEE
    754 and false-alarms on any float column with NaNs), and it is what
    actually matters for downstream reproducibility -- if the emitted
    parquet bytes are identical then the pipeline is bit-reproducible."""
    import hashlib
    import io
    import pyarrow.parquet as pq
    from generate import create_generator

    cfg = _cfg(seed=42)

    def emit_and_hash():
        g = create_generator("customer360", cfg)
        g.ensure_loyalty()
        t = g.generate_file_data(0)
        buf = io.BytesIO()
        pq.write_table(t, buf, compression="none")
        return hashlib.sha256(buf.getvalue()).hexdigest(), t.num_rows

    h1, n1 = emit_and_hash()
    h2, n2 = emit_and_hash()
    assert n1 == n2
    assert h1 == h2, (
        f"customer360 not deterministic across runs: sha256 h1={h1[:16]}... h2={h2[:16]}..."
    )


def test_customer360_generator_different_file_ids_differ():
    """Different file_ids must produce different content (otherwise a
    multi-file run is just N copies of the same file). event_id is the
    per-row UUID; it must differ across file_ids."""
    from generate import create_generator

    cfg = _cfg(seed=42)
    g = create_generator("customer360", cfg)
    g.ensure_loyalty()
    t0 = g.generate_file_data(0)
    t1 = g.generate_file_data(1)

    assert t0.column("event_id").to_pylist() != t1.column("event_id").to_pylist(), (
        "event_id (per-row UUID) identical across file_ids -- file_id has no effect on content"
    )


# ---------------------------------------------------------------------------
# Schema stability -- silver/downstream reads specific column names
# ---------------------------------------------------------------------------


def test_customer360_schema_has_core_columns():
    """The Customer 360 silver pipeline reads a fixed set of column names.
    A rename here silently breaks silver_build without any signal from
    datagen itself. Pin the top-level column set that silver reads."""
    from generate import create_generator

    cfg = _cfg(seed=42)
    g = create_generator("customer360", cfg)
    g.ensure_loyalty()
    t = g.generate_file_data(0)
    cols = set(t.column_names)
    # Not exhaustive -- pin the columns silver_build reads.
    # If silver_build changes what it reads, update this list.
    expected = {
        "customer_id", "event_timestamp", "event_id",
        "interaction_type", "transaction_amount", "currency",
    }
    missing = expected - cols
    assert not missing, f"customer360 schema missing expected columns: {missing}"


def test_customer360_event_timestamp_is_utc():
    """`event_timestamp` column must carry a UTC timezone so downstream
    tools see one consistent wall-clock regardless of pod local TZ."""
    from generate import create_generator

    cfg = _cfg(seed=42)
    g = create_generator("customer360", cfg)
    g.ensure_loyalty()
    t = g.generate_file_data(0)
    if "event_timestamp" not in t.column_names:
        pytest.skip("no event_timestamp column")
    # PyArrow timestamp type may or may not preserve tz metadata after
    # conversion from python datetimes -- check the actual first value
    # rendered back is UTC.
    first = t.column("event_timestamp")[0].as_py()
    if isinstance(first, datetime):
        assert first.tzinfo is not None, "event_timestamp is naive"
        assert first.utcoffset().total_seconds() == 0


# ---------------------------------------------------------------------------
# Config sanity -- defaults haven't drifted silently
# ---------------------------------------------------------------------------


def test_config_defaults_stable():
    """Pinning defaults so a silent change here (e.g. someone flips a
    default in the wrong direction) fails a test."""
    from generate import Config

    c = Config(target_tb=0.001, bucket="b", prefix="p", checkpoint_file="/tmp/x")
    # Documented defaults from the argparse help text.
    assert c.seed == 42
    assert c.payload_kb == 2
    assert c.file_size_mb == 512
    assert c.workers == 4
    assert c.duplicate_email_pct == 0.10
    assert c.dirty_ratio == 0.08
    assert c.schema_name == "customer360"
