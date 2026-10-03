"""Storage multiple: physical bytes over logical bytes, per table, layer and
in total, measured once at run end after the maintenance policy ran.

Physical is the object bytes under a table's location in object storage;
logical is the data files of the table's current snapshot. Physical is split
into current data, retained-snapshot data, metadata and other (unreferenced).
Measured from one paginated listing per bucket and the table metadata the
maintenance engine (Trino or Spark Thrift) reads:

1. The pipeline's tables per layer (``tables.workload_tables``).
2. Each table's location from the catalog: Trino ``SHOW CREATE TABLE``,
   Spark Thrift ``DESCRIBE TABLE EXTENDED``. A table the catalog does not
   know has no location and contributes nothing.
3. One listing per bucket, each key put in the first matching group:
   an excluded prefix (stream checkpoints, the datagen markers and manifest,
   scoring outputs, the ML loop's ``<gold>/_ml_loop/``), then the longest
   table location, then the corpus's raw datagen files (physical only,
   outside the total), then "unattributed".
4. Per table: metadata (``metadata/`` or ``_delta_log/`` under the
   location), current data (Iceberg ``$files``; Delta ``DESCRIBE DETAIL``),
   retained-snapshot data (Iceberg ``all_files`` not in ``files``) where the
   engine can separate it, and other: physical minus the three.

Excluded, and named without bytes because they are not in object storage:
the executor scratch PVCs and the dependency server's ``lb-deps`` PVC.
Incomplete multipart uploads do not appear in a listing and are not counted.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable, Iterable, Iterator
from typing import Any

logger = logging.getLogger(__name__)

OBJECTS_NOTE = "objects as listed; incomplete multipart uploads not counted"
ORPHAN_FLOOR_NOTE = "unreferenced, bounded by the 24 h 10 min orphan-removal floor"
NOT_PVC = None  # an excluded item named without bytes (not in object storage)

_CHECKPOINT_SEGMENT = re.compile(r"(^|/)_?checkpoints/")
_LOCATION_TRINO = re.compile(r"location\s*=\s*'([^']+)'", re.IGNORECASE)
_NUMBER = re.compile(r"^-?\d+(\.\d+)?([eE][+-]?\d+)?$")

#: Raised by a SQL runner when the engine refuses or fails a statement.
SqlRunner = Callable[[str], str]


def _split_uri(uri: str) -> tuple[str, str] | None:
    """(bucket, key prefix ending in "/") of an s3, s3a or s3n URI."""
    m = re.match(r"^s3[an]?://([^/]+)/?(.*)$", uri.strip())
    if not m:
        return None
    key = m.group(2)
    if key and not key.endswith("/"):
        key += "/"
    return m.group(1), key


def parse_number(stdout: str) -> float | None:
    """The first number in an engine's output (Trino prints a bare value,
    beeline a table with pipes and a header); None for NULL or nothing."""
    for line in (stdout or "").splitlines():
        cell = line.strip().strip("|").strip().strip('"').strip()
        if _NUMBER.match(cell):
            return float(cell)
    return None


def parse_location(engine: str, stdout: str) -> str | None:
    """The table location from ``SHOW CREATE TABLE`` (Trino) or ``DESCRIBE
    TABLE EXTENDED`` (Spark Thrift)."""
    if engine == "trino":
        m = _LOCATION_TRINO.search(stdout or "")
        return m.group(1) if m else None
    for line in (stdout or "").splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if cells and cells[0].lower() == "location" and len(cells) > 1 and cells[1]:
            return cells[1]
    return None


def _iceberg_ref(engine: str, fq: str, suffix: str) -> str:
    """``cat.schema."t$files"`` (Trino) or ``cat.schema.t.files`` (Thrift)."""
    if engine == "trino":
        prefix, _, tbl = fq.rpartition(".")
        return f'{prefix}."{tbl}${suffix}"' if prefix else f'"{fq}${suffix}"'
    return f"{fq}.{suffix}"


def current_sql(engine: str, table_format: str, fq: str) -> str | None:
    if table_format == "delta":
        return f"DESCRIBE DETAIL {fq}" if engine == "spark-thrift" else None
    return f"SELECT sum(file_size_in_bytes) FROM {_iceberg_ref(engine, fq, 'files')}"


def retained_sql(engine: str, table_format: str, fq: str) -> str | None:
    """Data files referenced by a retained snapshot but not the current one."""
    if table_format == "delta":
        return None
    files = _iceberg_ref(engine, fq, "files")
    if engine == "spark-thrift":
        return (
            "SELECT sum(a.file_size_in_bytes) FROM (SELECT DISTINCT file_path, "
            f"file_size_in_bytes FROM {_iceberg_ref(engine, fq, 'all_files')}) a "
            f"LEFT ANTI JOIN {files} b ON a.file_path = b.file_path"
        )
    entries = _iceberg_ref(engine, fq, "all_entries")
    return (
        "SELECT sum(a.file_size_in_bytes) FROM (SELECT DISTINCT data_file.file_path AS "
        "file_path, data_file.file_size_in_bytes AS file_size_in_bytes FROM "
        f"{entries} WHERE status <> 2) a WHERE a.file_path NOT IN "
        f"(SELECT file_path FROM {files})"
    )


def _describe_detail_size(stdout: str) -> float | None:
    """``sizeInBytes`` from Spark's ``DESCRIBE DETAIL`` table."""
    lines = [ln for ln in (stdout or "").splitlines() if "|" in ln]
    header: list[str] | None = None
    for ln in lines:
        cells = [c.strip() for c in ln.strip().strip("|").split("|")]
        if header is None and "sizeInBytes" in cells:
            header = cells
            continue
        if header is not None and len(cells) == len(header):
            value = cells[header.index("sizeInBytes")]
            return float(value) if _NUMBER.match(value) else None
    return None


def _excluded_label(bucket_layer: str, key: str, datagen_prefix: str) -> str | None:
    """The exclusion group of *key*, or None."""
    if _CHECKPOINT_SEGMENT.search(key):
        return "stream checkpoints"
    if bucket_layer == "bronze" and key.startswith(f"{datagen_prefix}/_corpus/"):
        return "datagen markers (_corpus/)"
    if bucket_layer == "bronze" and key.startswith(f"{datagen_prefix}/manifest/"):
        return "datagen manifest (manifest/)"
    if bucket_layer == "gold" and key.startswith("scoring/"):
        return "scoring outputs (scoring/)"
    if bucket_layer == "gold" and key.startswith("_ml_loop/"):
        return "ML loop (<gold>/_ml_loop/)"
    return None


def _ratio(physical: float, current: float | None) -> float | None:
    if not current:
        return None
    return round(physical / current, 4)


#: Retained-snapshot bytes are not queried on Trino past this many
#: snapshots: its ``$all_entries`` read grows quadratically with snapshots
#: on the coordinator, and a run end must not take the engine down.
TRINO_ALL_ENTRIES_MAX_SNAPSHOTS = 500

#: The whole measurement's time budget; statements after it are not run.
TIME_BUDGET_SECONDS = 600.0


def measure(
    *,
    buckets: dict[str, str],
    tables_by_layer: dict[str, list[str]],
    catalog: str | None,
    engine: str | None,
    table_format: str,
    datagen_prefix: str,
    list_objects: Callable[[str], Iterable[dict[str, Any]]],
    run_sql: SqlRunner | None,
    maintenance_id: str | None = None,
    orphan_removal_ran: bool = False,
    not_measured: str | None = None,
    raw_layers: tuple[str, ...] = (),
    clock: Callable[[], float] = time.monotonic,
    budget_seconds: float = TIME_BUDGET_SECONDS,
) -> dict[str, Any]:
    """The ``storage_multiple`` record block.

    *list_objects(bucket)* yields the bucket's objects (``Key``, ``Size``);
    *run_sql(sql)* runs one statement on the maintenance *engine* and
    returns its stdout, raising on failure. With no engine (DuckDB, ``none``)
    the block records physical bytes per bucket and *not_measured*. A layer
    in *raw_layers* is raw files the pipeline did not write as a table (C360
    batch bronze): its tables are not measured.
    """
    start = clock()

    def over_budget() -> bool:
        return clock() - start > budget_seconds

    excluded: dict[str, Any] = {
        "executor scratch PVCs": NOT_PVC,
        "dependency server PVC (lb-deps)": NOT_PVC,
    }
    out: dict[str, Any] = {
        "policy": maintenance_id,
        "engine": engine,
        "note": OBJECTS_NOTE,
        "tables": [],
        "layers": {},
        "total": {},
        "excluded": excluded,
        "raw_files": {},
        "unattributed": {},
        "not_measured": not_measured,
    }

    # Locations first: a listing is attributed to them.
    locations: dict[str, tuple[str, str, str]] = {}  # table -> (bucket, prefix, uri)
    skipped: dict[str, str] = {}
    for layer in raw_layers:
        for t in tables_by_layer.get(layer, []):
            skipped[t] = "raw files: this pipeline does not write it as a table"
    if run_sql is not None and engine and catalog:
        for tables in tables_by_layer.values():
            for t in tables:
                if t in skipped:
                    continue
                if over_budget():
                    skipped[t] = "time budget spent"
                    continue
                fq = f"{catalog}.{t}"
                sql = (
                    f"SHOW CREATE TABLE {fq}"
                    if engine == "trino"
                    else f"DESCRIBE TABLE EXTENDED {fq}"
                )
                try:
                    loc = parse_location(engine, run_sql(sql))
                except Exception as e:  # noqa: BLE001 -- a table the catalog lacks
                    skipped[t] = f"not in the catalog ({type(e).__name__})"
                    continue
                split = _split_uri(loc) if loc else None
                if split is None:
                    skipped[t] = "no location in the catalog's answer"
                    continue
                if split[0] not in buckets.values():
                    skipped[t] = "location outside this deployment's buckets"
                    continue
                locations[t] = (split[0], split[1], str(loc))

    # One listing per bucket (a bucket shared between layers is listed and
    # counted once; its exclusions are those of every layer it serves).
    layers_of: dict[str, list[str]] = {}
    for layer, bucket in buckets.items():
        layers_of.setdefault(bucket, []).append(layer)
    physical_by_table: dict[str, float] = dict.fromkeys(locations, 0.0)
    metadata_by_table: dict[str, float] = dict.fromkeys(locations, 0.0)
    per_bucket: dict[str, float] = {}
    listing_failed: dict[str, str] = {}
    for bucket, layers in layers_of.items():
        own = sorted(
            ((t, prefix) for t, (b, prefix, _u) in locations.items() if b == bucket),
            key=lambda tp: len(tp[1]),
            reverse=True,
        )
        # Accumulated per bucket and merged only when the whole listing
        # succeeded: a partial listing must not yield a multiple.
        b_total = 0.0
        b_excluded: dict[str, float] = {}
        b_physical: dict[str, float] = {}
        b_metadata: dict[str, float] = {}
        b_raw = 0.0
        b_unattributed = 0.0
        try:
            for n, obj in enumerate(list_objects(bucket)):
                if n % 1000 == 0 and over_budget():
                    raise TimeoutError("time budget spent")
                key = str(obj.get("Key", ""))
                size = float(obj.get("Size", 0) or 0)
                b_total += size
                label = next(
                    (x for x in (_excluded_label(ly, key, datagen_prefix) for ly in layers) if x),
                    None,
                )
                if label is not None:
                    b_excluded[label] = b_excluded.get(label, 0.0) + size
                    continue
                owner = next((t for t, prefix in own if key.startswith(prefix)), None)
                if owner is not None:
                    b_physical[owner] = b_physical.get(owner, 0.0) + size
                    rest = key[len(locations[owner][1]) :]
                    if rest.startswith("metadata/") or rest.startswith("_delta_log/"):
                        b_metadata[owner] = b_metadata.get(owner, 0.0) + size
                    continue
                if "bronze" in layers and key.startswith(f"{datagen_prefix}/"):
                    b_raw += size
                    continue
                b_unattributed += size
        except Exception as e:  # noqa: BLE001 -- recorded, never raised into the run
            out["unattributed"][bucket] = None
            out.setdefault("listing_errors", {})[bucket] = type(e).__name__
            for t, _prefix in own:
                listing_failed[t] = f"the listing of {bucket} failed ({type(e).__name__})"
            continue
        for label, size in b_excluded.items():
            excluded[label] = (excluded.get(label) or 0) + size
        for t, size in b_physical.items():
            physical_by_table[t] += size
        for t, size in b_metadata.items():
            metadata_by_table[t] += size
        if b_raw:
            out["raw_files"]["bronze"] = out["raw_files"].get("bronze", 0) + b_raw
        if b_unattributed:
            out["unattributed"][bucket] = out["unattributed"].get(bucket, 0) + b_unattributed
        per_bucket[bucket] = b_total
    out["physical_bytes_by_bucket"] = per_bucket

    def sql_value(sql: str) -> float | None:
        if over_budget():
            raise TimeoutError("time budget spent")
        return parse_number(run_sql(sql)) if run_sql is not None else None

    layer_sums: dict[str, dict[str, float]] = {}
    for layer, tables in tables_by_layer.items():
        for t in tables:
            row: dict[str, Any] = {"table": t, "layer": layer}
            if t not in locations:
                if run_sql is not None and engine and catalog or t in skipped:
                    row.update(location=None, not_measured=skipped.get(t, "no location"))
                    out["tables"].append(row)
                continue
            if t in listing_failed:
                row.update(location=locations[t][2], not_measured=listing_failed[t])
                out["tables"].append(row)
                continue
            bucket, prefix, uri = locations[t]
            fq = f"{catalog}.{t}"
            physical = physical_by_table[t]
            metadata = metadata_by_table[t]
            row.update(location=uri, physical_bytes=physical, metadata_bytes=metadata)
            csql = current_sql(str(engine), table_format, fq)
            if csql is None:
                row["not_measured"] = (
                    f"current data size is not readable for {table_format} on {engine}"
                )
                out["tables"].append(row)
                continue
            try:
                if table_format == "delta":
                    if over_budget():
                        raise TimeoutError("time budget spent")
                    current = _describe_detail_size(run_sql(csql)) if run_sql else None
                else:
                    current = sql_value(csql)
                    current = 0.0 if current is None else current  # sum over no files
                    # Data files registered in place outside the location
                    # (Iceberg add_files): their bytes are not under the
                    # location, so a multiple would be meaningless.
                    outside = sql_value(
                        f"SELECT sum(file_size_in_bytes) FROM {_iceberg_ref(str(engine), fq, 'files')} "
                        f"WHERE file_path NOT LIKE '%://{bucket}/{prefix}%'"
                    )
                    if outside:
                        row["not_measured"] = (
                            "data files registered in place outside the table location "
                            f"({outside / (1024**3):.2f} GiB)"
                        )
                        row["current_bytes"] = current
                        out["tables"].append(row)
                        continue
            except Exception as e:  # noqa: BLE001
                row["not_measured"] = f"current data size not read ({type(e).__name__})"
                out["tables"].append(row)
                continue
            if current is None:
                row["not_measured"] = "current data size not in the engine's answer"
                out["tables"].append(row)
                continue
            retained: float | None = None
            separable = False
            rsql = retained_sql(str(engine), table_format, fq)
            if rsql is not None:
                try:
                    if engine == "trino":
                        snaps = sql_value(
                            f"SELECT count(*) FROM {_iceberg_ref('trino', fq, 'snapshots')}"
                        )
                        if snaps is not None and snaps > TRINO_ALL_ENTRIES_MAX_SNAPSHOTS:
                            raise RuntimeError("too many retained snapshots to query safely")
                    value = sql_value(rsql)
                    retained = value if value is not None else 0.0
                    separable = True
                except Exception as e:  # noqa: BLE001 -- e.g. no $all_entries on this Trino
                    row["retained_error"] = f"{type(e).__name__}: {e}"[:120]
            other = physical - current - metadata - (retained or 0.0)
            if other < 0:
                row.update(
                    current_bytes=current,
                    retained_bytes=retained,
                    not_measured=(
                        "physical bytes under the location are below the bytes the table references"
                    ),
                )
                out["tables"].append(row)
                continue
            row.update(
                current_bytes=current,
                retained_bytes=retained,
                other_bytes=other,
                separable=separable,
                multiple=_ratio(physical, current),
            )
            if not separable:
                row["other_label"] = "retained and unreferenced, not separable on this engine"
            elif orphan_removal_ran:
                row["other_label"] = ORPHAN_FLOOR_NOTE
            out["tables"].append(row)
            acc = layer_sums.setdefault(layer, {"physical_bytes": 0.0, "current_bytes": 0.0})
            acc["physical_bytes"] += physical
            acc["current_bytes"] += current
    for layer, acc in layer_sums.items():
        out["layers"][layer] = {
            **acc,
            "multiple": _ratio(acc["physical_bytes"], acc["current_bytes"]),
        }
    if layer_sums:
        tp = sum(a["physical_bytes"] for a in layer_sums.values())
        tc = sum(a["current_bytes"] for a in layer_sums.values())
        out["total"] = {
            "physical_bytes": tp,
            "current_bytes": tc,
            "multiple": _ratio(tp, tc),
            # The total is over the measured tables only; it says how many.
            "tables_measured": sum(1 for t in out["tables"] if "multiple" in t),
            "tables": len(out["tables"]),
        }
    if over_budget():
        out["budget_spent"] = (
            f"the {budget_seconds:.0f} s budget ran out; later tables not measured"
        )
    return out


def iter_objects(boto_client: Any, bucket: str) -> Iterator[dict[str, Any]]:
    """Every object in *bucket* except Lakebench's own keys, one page at a
    time (s3/client.py does every listing)."""
    from lakebench.s3.client import iter_user_objects

    return iter_user_objects(boto_client, bucket, "")


def orphan_removal_ran(outcomes: list[dict[str, Any]] | None) -> bool:
    """Whether a ``remove_orphan_files`` statement succeeded in the run's
    recorded maintenance outcomes."""
    for outcome in outcomes or []:
        for op in (outcome or {}).get("operations") or []:
            if op.get("operation") == "remove_orphan_files" and (op.get("succeeded") or 0) > 0:
                return True
    return False


def _s3_client(cfg: Any) -> Any:
    from lakebench.s3 import S3Client

    s3_cfg = cfg.platform.storage.s3
    return S3Client(
        endpoint=s3_cfg.endpoint,
        access_key=s3_cfg.access_key,
        secret_key=s3_cfg.secret_key,
        region=s3_cfg.region,
        path_style=s3_cfg.path_style,
        ca_cert=s3_cfg.ca_cert,
        verify_ssl=s3_cfg.verify_ssl,
    )


def measure_run(cfg: Any, k8s: Any, s3: Any, metrics: Any) -> dict:
    """``measure`` for a deployed run: the config's buckets and tables, the
    maintenance engine's pod, the S3 client's listing. Never raises."""
    try:
        if s3 is None:
            s3 = _s3_client(cfg)
        from lakebench.deploy.datagen import bronze_datagen_prefix
        from lakebench.modules.table_formats.iceberg.maintenance import (
            find_maintenance_engine,
            query_sql,
        )

        buckets = {
            layer: getattr(cfg.platform.storage.s3.buckets, layer)
            for layer in ("bronze", "silver", "gold")
        }
        schema = cfg.architecture.workload.schema_type.value
        tables = cfg.architecture.tables
        tables_by_layer = {
            layer: tables.workload_tables(schema, layers=(layer,))
            for layer in ("bronze", "silver", "gold")
        }
        namespace = cfg.get_namespace()
        # C360 batch bronze is the raw corpus files, never a table; a
        # bronze_raw table a continuous run left in the namespace is not
        # this run's.
        from lakebench.config.schema import is_continuous_mode

        mode = cfg.architecture.pipeline.mode
        raw_layers = ("bronze",) if schema != "financial" and not is_continuous_mode(mode) else ()
        engine = pod = catalog = None
        reason = None
        if cfg.architecture.query_engine.type.value in ("duckdb", "none"):
            reason = "no SQL engine in this recipe can read table metadata"
        else:
            engine, pod, catalog = find_maintenance_engine(cfg, namespace)
            if engine is None:
                reason = "no maintenance engine pod was found"

        def run_sql(sql: str) -> str:
            return query_sql(str(engine), k8s, str(pod), namespace, sql, timeout=120)

        return measure(
            buckets=buckets,
            tables_by_layer=tables_by_layer,
            catalog=catalog,
            engine=engine,
            table_format=cfg.architecture.table_format.type.value,
            datagen_prefix=bronze_datagen_prefix(cfg).rstrip("/"),
            list_objects=lambda b: iter_objects(s3.raw_client, b),
            run_sql=run_sql if engine else None,
            maintenance_id=getattr(metrics, "maintenance_policy_id", None),
            orphan_removal_ran=orphan_removal_ran(getattr(metrics, "maintenance_outcomes", None)),
            not_measured=reason,
            raw_layers=raw_layers,
        )
    except Exception as e:  # noqa: BLE001 -- a measurement never fails the run
        logger.warning("storage multiple not measured: %s", e)
        return {"not_measured": f"measurement failed: {type(e).__name__}", "note": OBJECTS_NOTE}
