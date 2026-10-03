"""The refused-key table: config values 1.6 accepted that 1.7 refuses at load.

A removed key is refused whatever its value (each model's ``_removed_keys``).
The keys here still exist; only some values are refused, by a validator in
``config/schema.py``, with the row's ``fix`` in the refusal text. Commands
that do not change data (``destroy``, ``status``, the read-only commands)
still load the value, ignoring it with a note where the validator says so.

Two readers use the table:

* ``lakebench init --from`` drops a row whose ``init_from`` is ``"drop"``,
  and one whose ``init_from`` is ``"drop-default"`` when it holds the
  schema default (1.6 ``save_config`` wrote every field, so the key changed
  nothing), printing the fix; it keeps the others and lists each one
  ``run`` or ``deploy`` still refuses in the new file;
* ``scripts/upgrading.py`` requires an entry in
  ``docs/upgrading/breaking-1.7.yaml`` for every row, by ``subject``.

``tests/test_init_from.py`` loads ``example`` at ``key`` for each row and
checks that the commands in ``refused_by`` refuse it with ``fix`` in the
message, so a row cannot drift from its validator; and it maps every
``config/schema.py`` function that reads ``CHANGES_DATA`` or
``LoadPurpose.RUN`` to its rows or to a stated exemption, so a new
validator of that kind cannot go without a row.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from .schema import (
    MAX_DRIVER_CORES,
    MAX_EXECUTOR_OVERRIDE,
    OPERATOR_INSTALL_KEYS,
    operator_install_fix,
)


@dataclass(frozen=True)
class RefusedKey:
    """One config key some of whose values are refused at load."""

    #: Dotted path as a config writes it (top-level ``workload``,
    #: ``architecture.pipeline.continuous``). A ``spark.conf`` row names the
    #: block; ``example`` then holds the key and value.
    key: str
    #: The refused values, in words: ``true``, ``above 28``.
    when: str
    #: A refused value at ``key``, for the drift test.
    example: Any
    #: ``"deploy and run"`` (LoadPurpose MUTATE and RUN) or ``"run"`` (RUN).
    refused_by: Literal["deploy and run", "run"]
    #: What to do instead; the refusal text contains it.
    fix: str
    #: What ``init --from`` does with the key: ``drop`` it with the fix,
    #: ``drop-default`` it only at the schema default (where it changes
    #: nothing), or ``keep`` it for the user to change (``run`` still
    #: refuses it).
    init_from: Literal["drop", "drop-default", "keep"]

    @property
    def subject(self) -> str:
        """The breaking-changes list subject: ``<key> <when>``."""
        return f"{self.key} {self.when}"


def _executor_row(field: str) -> RefusedKey:
    limit = MAX_DRIVER_CORES if field == "driver_cores" else MAX_EXECUTOR_OVERRIDE
    return RefusedKey(
        key=f"platform.compute.spark.{field}",
        when=f"above {limit}",
        example=limit + 1,
        refused_by="deploy and run",
        fix=f"set {limit} or less, or leave it unset for the scale-derived count",
        init_from="keep",
    )


REFUSED_KEYS: tuple[RefusedKey, ...] = (
    *(
        RefusedKey(
            key=key,
            when="true",
            example=True,
            refused_by="deploy and run",
            fix=operator_install_fix(key),
            # false is what deploy does now, so the key says nothing either way.
            init_from="drop",
        )
        for key in OPERATOR_INSTALL_KEYS
    ),
    RefusedKey(
        key="architecture.benchmark.mode",
        when="throughput or composite",
        example="throughput",
        refused_by="run",
        fix="use 'lakebench benchmark --mode' for throughput and composite",
        init_from="keep",
    ),
    RefusedKey(
        key="architecture.benchmark.cache",
        when="cold",
        example="cold",
        refused_by="run",
        fix="use 'lakebench benchmark --cold'",
        init_from="keep",
    ),
    RefusedKey(
        key="architecture.benchmark.streams",
        when="above 1",
        example=4,
        refused_by="run",
        fix="use 'lakebench benchmark --streams'",
        # 4 is the default: written explicitly (1.6 save_config) it is refused
        # by run and means the same as absent to benchmark.
        init_from="drop-default",
    ),
    *(
        _executor_row(field)
        for field in (
            "bronze_executors",
            "silver_executors",
            "gold_executors",
            "bronze_ingest_executors",
            "silver_stream_executors",
            "gold_refresh_executors",
            "driver_cores",
        )
    ),
    RefusedKey(
        key="platform.compute.spark.driver_memory",
        when="not a Spark size",
        example="16Gi",
        refused_by="deploy and run",
        fix="write a whole number above 0 with a unit, k, m, g or t",
        init_from="keep",
    ),
    RefusedKey(
        key="spark.conf",
        when="spark.lb.gold.strategy other than auto, simple_agg or two_phase_agg",
        example={"spark.lb.gold.strategy": "incremental"},
        refused_by="deploy and run",
        fix="Delete it from spark.conf, or set auto, simple_agg or two_phase_agg",
        init_from="keep",
    ),
    RefusedKey(
        key="spark.conf",
        when="a key Lakebench owns",
        example={"spark.sql.shuffle.partitions": "64"},
        refused_by="deploy and run",
        fix="Delete them from spark.conf",
        init_from="keep",
    ),
)
