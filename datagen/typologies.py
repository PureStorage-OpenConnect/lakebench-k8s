"""Eight AMLworld-derived typology primitives for the Financial datagen.

Each typology is a deterministic function of ``(rng, instance, config)``
that emits a set of flat pacs.008-flavoured transaction rows implementing
the pattern. Row emission is decoupled from row scheduling so the caller
(FinancialGenerator) can interleave typology transactions with baseline
noise transactions in time order.

Emitted rows are ``dict[str, Any]`` matching the flat columns of
``BRONZE_PACS008_DDL`` in ``src/lakebench/deploy/financial_ddl.py``.
STRUCT-typed columns are populated as nested dicts; ARRAY-typed columns
as Python lists. Columns not relevant to the typology are left None and
the caller substitutes the noise-baseline value.

Typologies (matched to workload detectors):

- ``fan_in``           -- many originators -> one beneficiary. W2 structuring.
- ``fan_out``          -- one originator -> many beneficiaries. Money mules.
- ``gather_scatter``   -- N -> 1 -> M. Layering.
- ``scatter_gather``   -- 1 -> N -> 1. Round-trip via intermediaries.
- ``cycle``            -- A -> B -> C -> D -> A. W3 round-tripping.
- ``stack``            -- linear chain A -> B -> C -> D -> E. Layering.
- ``random``           -- decoy pattern; single random transaction.
- ``bipartite``        -- two entity clusters transacting across boundary.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import numpy as np
from manifest import TypologyInstance
from realism import (
    build_bic_pool,
    currency_for,
    home_country_for,
    is_cross_border,
    log_normal_amount,
    sample_corridor,
    sample_correspondent_chain,
    sample_regulatory_reporting,
    structuring_amount,
    structuring_burst_window,
)

CURRENCIES = ("USD", "EUR", "GBP", "CHF", "JPY")
COUNTRIES = ("US", "GB", "DE", "FR", "CH", "SG", "AE", "PA", "KY")
BICS = build_bic_pool(40)


@dataclass(frozen=True)
class TypologySpec:
    """Static declaration of a typology primitive.

    ``emit_fn`` returns a list of transaction-row dicts; it is called
    once per scheduled instance. ``participant_count`` is used by the
    scheduler to pick entity IDs.
    """

    typology_type: str
    expected_workload: str
    default_severity: str
    participant_count: int
    emit_fn: Callable[[np.random.Generator, TypologyInstance], list[dict]]


# ---------------------------------------------------------------------------
# Row helpers
# ---------------------------------------------------------------------------


def _new_uetr(rng: np.random.Generator) -> str:
    return str(uuid.UUID(bytes=rng.bytes(16), version=4))


def _amount(rng: np.random.Generator, low: float, high: float) -> Decimal:
    """Sample an amount as ``Decimal(x.xx)`` so it lands in pyarrow decimal128 cleanly."""
    return Decimal(str(round(float(rng.uniform(low, high)), 2)))


def _timestamp_within(rng: np.random.Generator, start: datetime, end: datetime) -> datetime:
    delta_us = int((end - start).total_seconds() * 1_000_000)
    if delta_us <= 0:
        return start
    return start + timedelta(microseconds=int(rng.integers(0, delta_us)))


def _agent(rng: np.random.Generator) -> dict:
    idx = int(rng.integers(0, len(BICS)))
    return {
        "bicfi": BICS[idx],
        "lei": f"LEI{idx:018d}",
        "nm": f"Bank {idx:02d}",
    }


def _party(entity_id: int, name_prefix: str = "E", country: str | None = None) -> dict:
    ctry = country if country is not None else COUNTRIES[entity_id % len(COUNTRIES)]
    return {
        "nm": f"{name_prefix}-{entity_id}",
        "pstl_adr": {
            "strt_nm": f"{(entity_id * 17) % 999 + 1} Main St",
            "twn_nm": ctry,
            "ctry": ctry,
        },
        "id": {"any_bic": None, "lei": f"LEI{entity_id:018d}"},
        "ctry_of_res": ctry,
    }


def _base_row(
    rng: np.random.Generator,
    ts: datetime,
    originator_id: int,
    beneficiary_id: int,
    amount: Decimal,
    currency: str,
    purpose_code: str,
    dbtr_country: str | None = None,
    cdtr_country: str | None = None,
) -> dict:
    """Minimum-viable pacs.008 flat-row dict.

    Country model (fixed 2026-09-16 per cycle-2 review):
      - Party ``ctry_of_res`` is the *home country* of the entity, stable
        across every transaction the entity appears in. Derived from
        ``realism.home_country_for(entity_id)`` when not passed.
      - Corridor args (``dbtr_country``, ``cdtr_country``) exist only to
        drive routing decisions -- correspondent-chain population,
        regulatory-reporting attribution. They may differ from the party
        home country (e.g. a US corporate on a routing-corridor "US->PA"
        still has ctry_of_res=US).

    This means silver.entities.country stays stable per entity_id and
    silver's hash(name, country) key does not fragment corporate reuse.
    """
    dbtr_agent = _agent(rng)
    cdtr_agent = _agent(rng)
    settlement_date = ts.date()
    uetr = _new_uetr(rng)
    txn_id = f"TXN-{originator_id}-{beneficiary_id}-{uetr[:8]}"

    # Home country -- stable per entity_id, used for the party structs.
    dbtr_home = home_country_for(originator_id)
    cdtr_home = home_country_for(beneficiary_id)

    # Routing corridor -- used for chain / reporting decisions only.
    if dbtr_country is None:
        dbtr_country = dbtr_home
    if cdtr_country is None:
        cdtr_country = cdtr_home
    cross_border = is_cross_border(dbtr_country, cdtr_country)

    intrmy1, intrmy2, intrmy3 = sample_correspondent_chain(rng, cross_border, BICS)
    rgltry = sample_regulatory_reporting(dbtr_country, cdtr_country, amount, currency)

    return {
        # message envelope
        "msg_id": f"MSG-{uetr[:12]}",
        "cre_dt_tm": ts,
        "nb_of_txs": 1,
        "ctrl_sum": amount,
        "ttl_intr_bk_sttlm_amt": amount,
        "intr_bk_sttlm_dt": settlement_date,
        "sttlm_inf": {"sttlm_mtd": "INDA"},
        "pmt_tp_inf": {
            "instr_prty": "NORM",
            "clr_chanl": "RTGS",
            "svc_lvl": "SEPA",
            "lcl_instrm": None,
            "ctgy_purp": purpose_code,
        },
        "instg_agt": dbtr_agent,
        "instd_agt": cdtr_agent,
        # per-transaction
        "txn_id": txn_id,
        "instr_id": txn_id,
        "end_to_end_id": txn_id,
        "uetr": uetr,
        "clr_sys_ref": None,
        "intr_bk_sttlm_amt": amount,
        "intr_bk_sttlm_ccy": currency,
        "instd_amt": amount,
        "instd_ccy": currency,
        "xchg_rate": Decimal("1.0"),
        "chrg_br": "SHAR",
        # correspondent chain (intermediaries) -- realism.sample_correspondent_chain
        "intrmy_agt_1": intrmy1,
        "intrmy_agt_2": intrmy2,
        "intrmy_agt_3": intrmy3,
        "prvs_instg_agt_1": None,
        "prvs_instg_agt_2": None,
        "prvs_instg_agt_3": None,
        # party chain
        "ultmt_dbtr": None,
        "initg_pty": None,
        # Parties carry the stable home country, not the routing corridor.
        "dbtr": _party(originator_id, "D", dbtr_home),
        "dbtr_acct": {"iban": f"IBAN{originator_id:016d}", "othr": None, "ccy": currency},
        "dbtr_agt": dbtr_agent,
        "cdtr_agt": cdtr_agent,
        "cdtr": _party(beneficiary_id, "C", cdtr_home),
        "cdtr_acct": {"iban": f"IBAN{beneficiary_id:016d}", "othr": None, "ccy": currency},
        "ultmt_cdtr": None,
        "purp_cd": purpose_code,
        "purp_prtry": None,
        "rgltry_rptg": rgltry,
        "rmt_inf_ustrd": None,
        "rmt_inf_strd": None,
    }


# ---------------------------------------------------------------------------
# Typology emitters
# ---------------------------------------------------------------------------


def _emit_fan_in(rng: np.random.Generator, inst: TypologyInstance) -> list[dict]:
    """Many originators -> one beneficiary (W2 structuring, tight 24-72h burst).

    Structuring targets the local CTR/STR threshold. Beneficiary's home
    country determines the currency and threshold band so we emit e.g.
    £14,800 to a GB beneficiary, ¥998,000 to a JP beneficiary --
    currency-correct rather than $9,500-across-the-board.
    """
    *originators, beneficiary = inst.participant_entity_ids
    # Structuring uses the beneficiary's home country / currency -- that's
    # the jurisdiction whose CTR rule the launderer is dodging.
    bene_home = home_country_for(beneficiary)
    ccy = currency_for(bene_home)
    rows = []
    for orig in originators:
        ts = _timestamp_within(rng, inst.injection_ts_start, inst.injection_ts_end)
        orig_home = home_country_for(orig)
        row = _base_row(
            rng,
            ts,
            orig,
            beneficiary,
            structuring_amount(rng, ccy),
            ccy,
            "CASH",
            orig_home,
            bene_home,
        )
        rows.append(row)
    return rows


def _emit_fan_out(rng: np.random.Generator, inst: TypologyInstance) -> list[dict]:
    """One originator -> many beneficiaries (money-mule payout structuring)."""
    originator, *beneficiaries = inst.participant_entity_ids
    orig_home = home_country_for(originator)
    # For fan_out the originator is doing the structuring so their local
    # threshold rules.
    ccy = currency_for(orig_home)
    rows = []
    for bene in beneficiaries:
        ts = _timestamp_within(rng, inst.injection_ts_start, inst.injection_ts_end)
        bene_home = home_country_for(bene)
        row = _base_row(
            rng,
            ts,
            originator,
            bene,
            structuring_amount(rng, ccy),
            ccy,
            "SALA",
            orig_home,
            bene_home,
        )
        rows.append(row)
    return rows


def _emit_gather_scatter(rng: np.random.Generator, inst: TypologyInstance) -> list[dict]:
    """N originators -> 1 hub -> M beneficiaries (layering)."""
    hub = inst.participant_entity_ids[0]
    third = max(1, (len(inst.participant_entity_ids) - 1) // 2)
    originators = inst.participant_entity_ids[1 : 1 + third]
    beneficiaries = inst.participant_entity_ids[1 + third :]
    window_third = (inst.injection_ts_end - inst.injection_ts_start) / 3
    dbtr_c, hub_c = sample_corridor(rng)
    _, cdtr_c = sample_corridor(rng)
    ccy = currency_for(hub_c)
    rows = []
    for orig in originators:
        ts = _timestamp_within(rng, inst.injection_ts_start, inst.injection_ts_start + window_third)
        rows.append(
            _base_row(
                rng, ts, orig, hub, log_normal_amount(rng), ccy, "CASH", dbtr_c, hub_c
            )
        )
    for bene in beneficiaries:
        ts = _timestamp_within(
            rng, inst.injection_ts_start + 2 * window_third, inst.injection_ts_end
        )
        rows.append(
            _base_row(rng, ts, hub, bene, log_normal_amount(rng), ccy, "TRAD", hub_c, cdtr_c)
        )
    return rows


def _emit_scatter_gather(rng: np.random.Generator, inst: TypologyInstance) -> list[dict]:
    """1 originator -> N intermediaries -> 1 beneficiary (round-trip flavour)."""
    originator = inst.participant_entity_ids[0]
    beneficiary = inst.participant_entity_ids[-1]
    intermediaries = inst.participant_entity_ids[1:-1]
    window_half = (inst.injection_ts_end - inst.injection_ts_start) / 2
    dbtr_c, interm_c = sample_corridor(rng)
    _, cdtr_c = sample_corridor(rng)
    ccy_out = currency_for(interm_c)
    ccy_in = currency_for(cdtr_c)
    rows = []
    for interm in intermediaries:
        ts_out = _timestamp_within(
            rng, inst.injection_ts_start, inst.injection_ts_start + window_half
        )
        rows.append(
            _base_row(
                rng, ts_out, originator, interm, log_normal_amount(rng), ccy_out, "TRAD",
                dbtr_c, interm_c,
            )
        )
        ts_in = _timestamp_within(rng, inst.injection_ts_start + window_half, inst.injection_ts_end)
        rows.append(
            _base_row(
                rng, ts_in, interm, beneficiary, log_normal_amount(rng), ccy_in, "TRAD",
                interm_c, cdtr_c,
            )
        )
    return rows


def _emit_cycle(rng: np.random.Generator, inst: TypologyInstance) -> list[dict]:
    """A -> B -> C -> D -> A. Round-tripping across jurisdictions (W3 target)."""
    n = len(inst.participant_entity_ids)
    span = inst.injection_ts_end - inst.injection_ts_start
    step = span / n
    amount = log_normal_amount(rng)
    # Cycle rotates through 3 corridors to look more like layering across jurisdictions.
    corridors = [sample_corridor(rng) for _ in range(n)]
    rows = []
    ids = inst.participant_entity_ids
    for i in range(n):
        src = ids[i]
        dst = ids[(i + 1) % n]
        ts = inst.injection_ts_start + step * i + timedelta(seconds=int(rng.integers(0, 3600)))
        # Slight amount decay to simulate fees; final leg still >90% of original
        leg_amount = (amount * (Decimal("0.97") ** i)).quantize(Decimal("0.01"))
        c_out, c_in = corridors[i]
        ccy = currency_for(c_in)
        rows.append(_base_row(rng, ts, src, dst, leg_amount, ccy, "TRAD", c_out, c_in))
    return rows


def _emit_stack(rng: np.random.Generator, inst: TypologyInstance) -> list[dict]:
    """Linear chain: funds pass through N accounts in sequence."""
    ids = inst.participant_entity_ids
    span = inst.injection_ts_end - inst.injection_ts_start
    step = span / max(1, len(ids) - 1)
    amount = log_normal_amount(rng)
    corridors = [sample_corridor(rng) for _ in range(len(ids) - 1)]
    rows = []
    for i in range(len(ids) - 1):
        ts = inst.injection_ts_start + step * i + timedelta(seconds=int(rng.integers(0, 1800)))
        leg_amount = (amount * (Decimal("0.95") ** i)).quantize(Decimal("0.01"))
        c_out, c_in = corridors[i]
        ccy = currency_for(c_in)
        rows.append(_base_row(rng, ts, ids[i], ids[i + 1], leg_amount, ccy, "TRAD", c_out, c_in))
    return rows


def _emit_random(rng: np.random.Generator, inst: TypologyInstance) -> list[dict]:
    """Control/decoy: one plausible transaction between two of the participants."""
    a, b = inst.participant_entity_ids[0], inst.participant_entity_ids[1]
    ts = _timestamp_within(rng, inst.injection_ts_start, inst.injection_ts_end)
    dbtr_c, cdtr_c = sample_corridor(rng)
    ccy = currency_for(cdtr_c)
    return [_base_row(rng, ts, a, b, log_normal_amount(rng), ccy, "TRAD", dbtr_c, cdtr_c)]


def _emit_bipartite(rng: np.random.Generator, inst: TypologyInstance) -> list[dict]:
    """Two clusters transacting across a boundary (W1 synthetic-ID target).

    The two clusters use different countries so the inter-cluster edges
    look like cross-border activity; entities inside a cluster share the
    same country, giving Connected Components something country-shaped
    to find.
    """
    ids = inst.participant_entity_ids
    half = max(1, len(ids) // 2)
    left = ids[:half]
    right = ids[half:]
    dbtr_c, cdtr_c = sample_corridor(rng)
    ccy = currency_for(cdtr_c)
    rows = []
    for src in left:
        for dst in right:
            ts = _timestamp_within(rng, inst.injection_ts_start, inst.injection_ts_end)
            rows.append(
                _base_row(
                    rng, ts, src, dst, log_normal_amount(rng), ccy, "TRAD", dbtr_c, cdtr_c
                )
            )
    return rows


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


TYPOLOGIES: dict[str, TypologySpec] = {
    "fan_in": TypologySpec("fan_in", "W2_structuring", "operational", 6, _emit_fan_in),
    "fan_out": TypologySpec("fan_out", "W2_structuring", "operational", 6, _emit_fan_out),
    "gather_scatter": TypologySpec(
        "gather_scatter", "W2_structuring", "strategic", 9, _emit_gather_scatter
    ),
    "scatter_gather": TypologySpec(
        "scatter_gather", "W3_round_tripping", "strategic", 7, _emit_scatter_gather
    ),
    "cycle": TypologySpec("cycle", "W3_round_tripping", "operational", 4, _emit_cycle),
    "stack": TypologySpec("stack", "W2_structuring", "strategic", 5, _emit_stack),
    "random": TypologySpec("random", "W2_structuring", "smoke", 2, _emit_random),
    "bipartite": TypologySpec("bipartite", "W1_synthetic_id", "strategic", 8, _emit_bipartite),
}


def schedule_typologies(
    seed: int,
    scale: float,
    window_start: datetime,
    window_end: datetime,
    customer_id_max: int,
) -> list[TypologyInstance]:
    """Deterministically schedule typology instances across a generation window.

    Instance count scales linearly with ``scale`` per REQ-S-02. Each
    instance draws its participants from a scale-appropriate entity ID
    range so downstream Splink (W5) and Connected Components (W1) see
    non-trivial cluster shapes.
    """
    instances: list[TypologyInstance] = []
    total_span = window_end - window_start
    instances_per_typology = max(1, int(scale * 10))
    # Guard against zero-width windows (edge case tests, degenerate configs).
    # If the caller passes window_start == window_end, we can't sample offsets;
    # schedule everything to start exactly at window_start with zero-width
    # injection window. Emitters cope with zero-span injection ranges.
    max_offset_s = max(1, int(total_span.total_seconds()))

    for i, (_name, spec) in enumerate(sorted(TYPOLOGIES.items())):
        for j in range(instances_per_typology):
            instance_seed = seed + 0xF100 + i * 1000 + j
            inst_rng = np.random.default_rng(seed=instance_seed)
            participants = [
                int(inst_rng.integers(1, customer_id_max + 1))
                for _ in range(spec.participant_count)
            ]
            offset = timedelta(seconds=int(inst_rng.integers(0, max_offset_s)))
            # Structuring bursts (fan_in / fan_out) fire in a tight 24-72h window
            # per real-world CTR-evasion patterns. Layering typologies (cycle,
            # stack, gather_scatter) span days-to-a-week; bipartite and random
            # spread across the widest window.
            if spec.expected_workload == "W2_structuring" and spec.typology_type in (
                "fan_in",
                "fan_out",
            ):
                duration = structuring_burst_window(inst_rng)
            elif spec.typology_type in ("cycle", "stack", "gather_scatter", "scatter_gather"):
                duration = timedelta(hours=int(inst_rng.integers(48, 168)))  # 2-7 days
            else:
                duration = timedelta(hours=int(inst_rng.integers(1, 168)))
            start = window_start + offset
            end = min(window_end, start + duration)
            instances.append(
                TypologyInstance(
                    typology_id=f"{spec.typology_type.upper()}_{j:06d}",
                    typology_type=spec.typology_type,
                    participant_entity_ids=participants,
                    injection_ts_start=start,
                    injection_ts_end=end,
                    expected_workload=spec.expected_workload,
                    severity=spec.default_severity,
                    seed=instance_seed,
                )
            )
    return instances


def emit_instance_rows(
    instance: TypologyInstance,
) -> list[dict]:
    """Emit all rows for one scheduled instance and stamp its UETRs onto it."""
    spec = TYPOLOGIES[instance.typology_type]
    rng = np.random.default_rng(seed=instance.seed)
    rows = spec.emit_fn(rng, instance)
    instance.participant_uetrs = [r["uetr"] for r in rows]
    return rows
