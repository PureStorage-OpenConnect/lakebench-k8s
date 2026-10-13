"""create_namespace asks for the layer's location and falls back to none.

Polaris refuses a namespace location ("has a custom location, which is not
enabled"). The Customer 360 streaming scripts used to log that and go on, so
the first table create failed with NoSuchNamespace and ended the stream.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.usefixtures("load_script")

POLARIS_REFUSAL = (
    "Malformed request: Namespace silver has a custom location, which is not enabled. "
    "Expected location: [s3://dep-bronze/silver/]. Got location: [s3a://dep-silver/warehouse/]"
)


class _Spark:
    def __init__(self, refuse_location: str | None = None, refuse_all: bool = False):
        self.refuse_location = refuse_location
        self.refuse_all = refuse_all
        self.statements: list[str] = []

    def sql(self, statement: str):
        self.statements.append(statement)
        if self.refuse_all or (self.refuse_location and "LOCATION" in statement):
            raise RuntimeError(self.refuse_location or "catalog down")


def test_a_catalog_that_takes_the_location_gets_it():
    from common import create_namespace

    spark = _Spark()
    used = create_namespace(spark, "lakehouse", "silver", "s3a://dep-silver/warehouse/silver.db/")
    assert used == "s3a://dep-silver/warehouse/silver.db/"
    assert spark.statements == [
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver "
        "LOCATION 's3a://dep-silver/warehouse/silver.db/'"
    ]


def test_a_catalog_that_refuses_the_location_gets_the_namespace_without_one():
    from common import create_namespace

    spark = _Spark(refuse_location=POLARIS_REFUSAL)
    used = create_namespace(spark, "lakehouse", "silver", "s3a://dep-silver/warehouse/silver.db/")
    assert used is None
    assert spark.statements[-1] == "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver"
    assert len(spark.statements) == 2


def test_a_namespace_that_cannot_be_created_at_all_raises():
    from common import create_namespace

    with pytest.raises(RuntimeError, match="catalog down"):
        create_namespace(_Spark(refuse_all=True), "lakehouse", "gold", "s3a://dep-gold/w/gold.db/")
