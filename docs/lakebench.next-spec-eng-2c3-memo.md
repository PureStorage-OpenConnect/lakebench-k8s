# ENG-2C.3 Design Memo -- Financial Pipeline Scripts

**Purpose.** Scope-and-sub-PR plan for the Financial (FinServ-Crime, AML) pipeline. The spec's ENG-2C.3 collapses "everything that emits or transforms pacs008" into a single work item; at ~2,000-3,000 LOC of real domain code it needs a decomposition before code lands. This memo picks the sub-PR boundaries and flags decisions that need your input.

---

## What ENG-2C.3 actually asks for

Nine Python scripts, one CLI, one Iceberg-table schema surface, and one FinancialGenerator that registers into the datagen dispatch already shipped in `612f199`.

**Pipeline scripts** (all live under `src/lakebench/spark/scripts/`, all authored once and dispatched through `runtime.protocol.Runtime` per addendum A.3):

| Script | Mode | Purpose | Depends on |
|---|---|---|---|
| `bronze_verify_financial.py` | batch | Validate pacs008 schema; register Iceberg table | data model |
| `silver_build_financial.py` | batch | Bronze → silver.{transactions, entities, accounts, counterparty_edges}. Reuses `SilverStrategy`. | data model |
| `gold_finalize_financial.py` | batch | Daily/monthly aggregates for dashboards | silver |
| `bronze_ingest_financial.py` | sustained | Structured Streaming from bronze land zone | data model |
| `silver_stream_financial.py` | sustained | Incremental bronze→silver; counterparty_edges merge | silver, bronze_ingest |
| `gold_refresh_financial.py` | sustained | Periodic gold recompute | silver_stream |
| `replay_financial.py` | on-demand (W8) | Historical replay against past snapshot | silver + ENG-R-05 already merged |
| `reproduce_financial.py` | on-demand (W10) | Time-travel alert reproduction | silver + ENG-R-05 |
| `score_financial.py` | post-detection | Manifest + workload output → `recall.parquet` | manifest sidecar (in generator) |

**FinancialGenerator** (`datagen/generate.py`): emits pacs008 rows for a time window, applies scheduled typology injections, writes manifest sidecar. Registers under `"financial"` in `_GENERATOR_REGISTRY`. Eight AMLworld typology primitives ship as reference implementations.

**Iceberg-table schema surface**: `TablesConfig` in `config/schema.py` grows `silver_entities`, `silver_accounts`, `silver_counterparty_edges`, `gold_alerts`, `gold_risk_scores` fields. Existing `silver`/`gold` fields stay (Customer 360 continues using them).

**JobType enum + script map**: 9 new values in `modules/pipeline_engines/spark/job.py` at `:751` and `:1175`. New scripts added to `script_files` list at `:1883` so they land in the Spark image ConfigMap.

**CLI additions**: `lakebench financial-replay` (invokes replay_financial.py) and `lakebench financial-reproduce` (invokes reproduce_financial.py). Both live in a new `src/lakebench/cli/_financial.py`.

**Not this ENG (deferred to workload authoring, ENG-2C.4.1-11)**: the 11 workload scripts themselves (W1 connected components, W2/W3 motifs, W4 Pregel, W5 Splink, W6/W7 investigator queries, W8 replay logic, W9 writeback, W10 reproduce logic, W11 ingest). W8/W10 orchestrators live in ENG-2C.3; their detection modules are ENG-2C.4.

---

## Sub-PR decomposition (proposed)

Nine PRs sized to be independently mergeable. Each keeps Customer 360 unchanged. Order chosen so each PR unblocks the next without waiting on live UAT.

| # | Sub-PR | Size est. | Depends on | Verification without cluster |
|---|---|---|---|---|
| a | `TablesConfig` extension + Iceberg table DDL constants | ~150 LOC | R-05 (landed) | pytest schema tests |
| b | `FinancialGenerator` + typology plugin interface + manifest sidecar | ~600 LOC | (a), datagen dispatch (landed) | pytest generator determinism tests; small in-process generation to `/tmp` |
| c | `bronze_verify_financial.py` script + JobType.BRONZE_VERIFY_FINFRAUD + script map entry + ComponentSpec | ~200 LOC | (a), (b) | pytest for the ComponentSpec builder + script argparse |
| d | `silver_build_financial.py` (batch) + SilverStrategy reuse + JobType | ~400 LOC | (c) | pytest for query-string builders; strategy dispatch |
| e | `gold_finalize_financial.py` + gold DDL + JobType | ~250 LOC | (d) | pytest for aggregation SQL builders |
| f | Sustained-mode scripts (`bronze_ingest`, `silver_stream`, `gold_refresh`) + JobTypes | ~400 LOC | (e) | pytest streaming-config builders |
| g | `replay_financial.py` + `financial-replay` CLI + JobType | ~250 LOC | (d), R-05 | pytest snapshot resolution; CLI parse tests |
| h | `reproduce_financial.py` + `financial-reproduce` CLI + JobType | ~200 LOC | (d), R-05 | pytest CLI + timestamp math |
| i | `score_financial.py` + recall.parquet emitter + JobType | ~150 LOC | (b) manifest, (d) | pytest for score computation on canned manifest |

**Total**: ~2,600 LOC across 9 sub-PRs. Each keeps `pytest tests/` green; no cluster access needed to merge any of them. The Spark 4.2 UAT gate does NOT block this chain -- Financial scripts run on the current 4.0.2 default.

**What's NOT in this decomposition**: workload scripts (W1-W11). They land under ENG-2C.4.1-11 which is its own chain, sequenced after (i).

---

## Decisions I need from you before writing code

1. **Gold DDL is undefined in the spec** (§2C.1 says "Gold DDL and manifest DDL: to be populated in this section"). Workloads reference `gold.alerts`, `gold.risk_scores`, `gold.synthetic_id_clusters`. I can synthesise plausible DDL from the workload writes I see in §2C.4 (W2 writes `rule_id` alerts, W4 writes risk_scores, W9 writes writeback merges), but this is a design fork you might want to shape. Options: (a) I synthesise from workload references and note assumptions, (b) you draft the gold DDL in Claude Desktop and I implement to it, (c) defer gold DDL to per-workload PRs in ENG-2C.4.

2. **Typology primitives count.** Spec says "eight AMLworld primitives ship as reference implementations." AMLworld has 8 named typologies (fan-in/fan-out/gather-scatter/scatter-gather/cycle/stack/random/bipartite). Ship all 8 in sub-PR (b), or ship 2-3 in (b) and defer the rest to ENG-2C.4? Ship-all keeps FinancialGenerator honest; defer-most makes (b) smaller.

3. **Manifest sidecar format.** Spec references "manifest.py" writing alongside pacs008 Parquet but doesn't specify the shape. Standard AMLworld manifest is JSONL with `{typology_id, typology_type, participant_uetrs, injection_ts_window}`. Confirm or override.

4. **Do I create `docs/financial-benchmark-baselines.md`?** ENG-2C.4.8 verification references it as the timeout budget source. Either I stub it in sub-PR (g) or you write it in Claude Desktop.

5. **`financial-replay` and `financial-reproduce` CLI subcommand naming.** Alternative: `lakebench replay --schema financial ...` (dispatches on schema). More Unix-y; but harder to discover. Current spec uses the explicit-verb form. Stick with `financial-replay`?

---

## Recommended next move

Answer the five decisions above (or pick "you decide, ship it and I'll review the PRs"), then I open sub-PR (a) -- the `TablesConfig` extension + Iceberg DDL. That one is pure schema plumbing, ~1 hour of work, sets up everything downstream.

If you want to shape the gold DDL yourself in Claude Desktop first, we can pause at (a) after schema extension and pick up (e) after your DDL comes back.
