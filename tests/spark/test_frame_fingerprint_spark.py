"""AM-15a (AML-9, AML-10, V16-2, EVD-10): ``common.frame_fingerprint`` is
order-independent and sees every change a time-travel or reproduction
comparison must see, and gives the same value on the 4.0 and 4.1 lines.

No jars: the helper reads a DataFrame, not a table.
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.usefixtures("load_script")

SCHEMA = (
    "txn_id string, originator_id bigint, txn_amount decimal(18,2), "
    "txn_amount_usd decimal(18,2), txn_timestamp timestamp, txn_type string, "
    "correspondent_chain array<string>, cross_border boolean, value_date date, "
    "risk double, hop int, evidence map<string,string>, "
    "_batch_id bigint, _stream_id string, ingest_ts timestamp"
)
COLS = [part.strip().split(" ")[0] for part in SCHEMA.split(", ")]
T0 = datetime(2024, 3, 4, 10, tzinfo=timezone.utc)

# The committed fingerprint of _pinned_rows() over COLS. Asserted on both CI
# Spark legs (pyspark 4.0.1 and 4.1.1): the same frame must give the same
# string on both, or a comparison across lines is meaningless. Recompute only
# when _FINGERPRINT_VERSION is bumped.
PINNED = (20, "50127608237909602974", "dac18745da552321")


def _pinned_rows():
    rows = []
    for i in range(20):
        rows.append(
            (
                f"T{i:04d}",
                1_000_000_000_000 + i * 7919,
                Decimal(f"{(i * 1234.56) % 99999:.2f}"),
                None if i % 5 == 0 else Decimal(f"{i * 10.25:.2f}"),
                T0 + timedelta(minutes=17 * i),
                ("wire", "ach", "rtp", "internal")[i % 4],
                None if i % 7 == 0 else [f"BIC{j}" for j in range(i % 3 + 1)],
                None if i % 6 == 0 else i % 2 == 0,
                date(2024, 3, 1) + timedelta(days=i),
                i * 0.25,
                None if i % 4 == 0 else i,
                None if i % 3 == 0 else {"rule": f"W{i % 5}", "hop": str(i)},
                i // 5,
                "batch" if i < 10 else "s-1",
                T0 + timedelta(seconds=i),
            )
        )
    return rows


@pytest.fixture(scope="module")
def frame(spark_session):
    return spark_session.createDataFrame(_pinned_rows(), SCHEMA)


def _fp(df, cols=COLS):
    from common import frame_fingerprint

    return frame_fingerprint(df, cols)


def _with_row(spark, rows, i, **changes):
    row = list(rows[i])
    for name, value in changes.items():
        row[COLS.index(name)] = value
    out = list(rows)
    out[i] = tuple(row)
    return spark.createDataFrame(out, SCHEMA)


def test_pinned_cross_line_value(frame):
    """The committed value on both lines (the "xxhash64 equal on 4.0/4.1"
    check)."""
    assert _fp(frame) == PINNED


def test_shuffled_rows_equal(spark_session, frame):
    rows = _pinned_rows()
    random.Random(43).shuffle(rows)
    shuffled = spark_session.createDataFrame(rows, SCHEMA).repartition(3)
    assert _fp(shuffled) == _fp(frame)


def test_one_changed_cell_differs(spark_session, frame):
    rows = _pinned_rows()
    changed = _with_row(spark_session, rows, 3, txn_amount=rows[3][2] + Decimal("0.01"))
    assert _fp(changed)[1] != _fp(frame)[1]
    assert _fp(changed)[0] == _fp(frame)[0]


def test_duplicated_row_differs(spark_session, frame):
    rows = _pinned_rows()
    dup = spark_session.createDataFrame(rows + [rows[7]], SCHEMA)
    a, b = _fp(dup), _fp(frame)
    assert a[0] == b[0] + 1
    assert a[1] != b[1]


def test_swapped_null_position_differs(spark_session):
    """(x, NULL) and (NULL, x) over two columns of one type: xxhash64 alone
    gives the same hash for both; the null mask separates them."""
    from pyspark.sql import functions as F

    left = spark_session.createDataFrame([("x", None)], "a string, b string")
    right = spark_session.createDataFrame([(None, "x")], "a string, b string")
    assert (
        left.select(F.xxhash64("a", "b")).first()[0]
        == (right.select(F.xxhash64("a", "b")).first()[0])
    ), "premise: Spark's xxhash64 skips NULLs"
    assert _fp(left, ["a", "b"]) != _fp(right, ["a", "b"])


def test_one_batch_id_change_differs(spark_session, frame):
    """The batch stamp decides sealed visibility, so it is part of the
    fingerprint (silent-corruption L1)."""
    rows = _pinned_rows()
    restamped = _with_row(spark_session, rows, 12, _batch_id=rows[12][12] + 1)
    assert _fp(restamped)[1] != _fp(frame)[1]


def test_map_entry_order_does_not_matter(spark_session):
    from pyspark.sql import functions as F

    a = spark_session.createDataFrame([(1,)], "k int").select(
        F.create_map(F.lit("x"), F.lit("1"), F.lit("y"), F.lit("2")).alias("m")
    )
    b = spark_session.createDataFrame([(1,)], "k int").select(
        F.create_map(F.lit("y"), F.lit("2"), F.lit("x"), F.lit("1")).alias("m")
    )
    c = spark_session.createDataFrame([(1,)], "k int").select(
        F.create_map(F.lit("x"), F.lit("1"), F.lit("y"), F.lit("3")).alias("m")
    )
    assert _fp(a, ["m"]) == _fp(b, ["m"])
    assert _fp(a, ["m"]) != _fp(c, ["m"])


def test_column_type_and_order_in_cols_sha(spark_session):
    one = spark_session.createDataFrame([(1, 2)], "a bigint, b bigint")
    other_type = spark_session.createDataFrame([(1, 2)], "a int, b bigint")
    assert _fp(one, ["a", "b"])[2] != _fp(other_type, ["a", "b"])[2]
    assert _fp(one, ["a", "b"])[2] != _fp(one, ["b", "a"])[2]


def test_empty_frame(spark_session):
    empty = spark_session.createDataFrame([], "a bigint")
    assert _fp(empty, ["a"])[:2] == (0, "0")


@pytest.mark.parametrize(
    "cols",
    [[], ["txn_id", "txn_id"], ["nope"], [f"c{i}" for i in range(64)]],
    ids=["none", "repeated", "missing", "too-many"],
)
def test_bad_columns_raise(spark_session, frame, cols):
    df = frame
    if len(cols) == 64:
        df = spark_session.createDataFrame([tuple(range(64))], ", ".join(f"{c} int" for c in cols))
    with pytest.raises(ValueError):
        _fp(df, cols)


def test_sixty_three_columns(spark_session):
    """The largest mask: bit 62 set (column 63 NULL) stays a positive long."""
    cols = [f"c{i}" for i in range(63)]
    schema = ", ".join(f"{c} int" for c in cols)
    full = spark_session.createDataFrame([tuple(range(63))], schema)
    last_null = spark_session.createDataFrame([(*range(62), None)], schema)
    assert _fp(full, cols)[1] != _fp(last_null, cols)[1]
