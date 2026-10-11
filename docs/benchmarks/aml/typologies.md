# AML benchmark: typologies and planting

## 3.6 Typologies

Fifteen typologies are planted (`datagen_rs/src/typology.rs`): `bipartite`,
`cycle`, `fan_in`, `fan_out`, `gather_scatter`, `random`, `scatter_gather`,
`stack`, `synthetic_identity`, `corridor_high_risk`, `cross_border_cycle`,
`dormant_reactivation`, `micro_structuring`, `rapid_layering`,
`tbml_repeated_invoice`. The screening track (`datagen_rs/src/screening.rs`)
adds `sanctions_match` and `pep_match`: 17 manifest types.

- Each typology gets an equal share of a row budget of 0.1% of base payments,
  so instance counts vary with instance size.
- Every instance's subject is a customer.
- Nine typologies have a designated rule ([4.2](rules.md#42-detection-rules)).
  Eight have none: `bipartite`, `cross_border_cycle`, `fan_in`, `fan_out`,
  `random`, `scatter_gather`, `synthetic_identity`, `tbml_repeated_invoice`.
  `benchmark/aml_queries.py` lists each with a one-line reason
  (`UNMAPPED_TYPOLOGIES`), separating "planted, no detector scores it" from
  "forgot to hook it up". Recall for an unmapped typology is untestable, not
  zero.
- `random` is a control: its hit rate is the chance floor
  ([8.4](scoring.md#84-aml-scoring-reported-a-batch-run-without-a-result-fails)).
- The pre-registered behavioural subset: `gather_scatter`, `rapid_layering`,
  `stack`, `dormant_reactivation`, `micro_structuring`, `corridor_high_risk`.

## 3.8 Sanctions and PEP screening track

Every corpus carries a synthetic, dated watchlist (`bronze/watchlist.parquet`):

- a sanctions list in two versions: version 1 in force at corpus start;
  version 2 adds about a quarter more entries three quarters of the way
  through;
- a PEP list;
- entries with name, aliases, country and town; nothing marks which were
  paid;
- listed parties are external counterparties, never the bank's customers and
  never in the party master.

Planted payments:

- About 70% of listed parties are paid by one or two customers, one to three
  payments each.
- Each payer relationship pays one external account with a fixed creditor
  name: the list spelling, an alias, a token-order swap, a one-letter typo, a
  different romanisation, or (companies) the legal suffix dropped. An exact
  join on the list name misses most.
- About a fifth of those accounts are in another country than the list entry,
  so a screen requiring a country match loses recall.
- Version-2 parties are paid only before their listing; only the rescreen
  finds them.
- Namesake decoys are paid too and are not in the manifest: another middle
  name or line of business, a one-letter-different name in the same country,
  or the same name in another country. A loose screen loses precision.
- Twenty ordinary external payees per list entry, with the same name shapes,
  one to three payments each and no party or account master record, keep
  planted payments from standing out as "rare external payees". On a seed-43
  calibration corpus that heuristic reaches 3% precision.

**Ground truth** is the manifest: one `sanctions_match` or `pep_match`
instance per (customer, listed party), its UETRs the customer's payments to
that party. `injection_parameters` holds `list_id`, `list_version`,
`detectable_by` (`transaction_screen` or `rescreen`), `name_variant` and
`account_country` (`listed` or `other`).

Recall and precision are computed as for the behavioural rules.
Transaction-level precision is weighted by payments: one heavily paid world
entity whose name sits near a list entry (world persons carry a middle
initial, list entries a full middle name) can dominate it, so read it beside
the alert count.

**W7 corridors.** The high-risk corridor list W7 uses beside the FATF list
(`synthetic_corridors.json`) is synthetic: the country pool the generator
draws `corridor_high_risk` participants from, because no generator home
country is on the June 2026 FATF lists. W7's recall on that typology is
partly by construction: the rule uses the generator's corridor list, as a
bank uses its own. Neither its
recall nor its alert volume is comparable with 1.5.

## 3.9 Planting shapes and baseline timing

Every planting parameter is self-chosen unless a source is named. None is set
from a detection rule's threshold; rule recall moves as a consequence and is
reported, not targeted.

| Typology | Shape | Why |
|---|---|---|
| `micro_structuring` | A crew of 3 to 8 depositors (some deposit more than once) pays the collector 8 times over 3 to 21 days. 3 to 8 payments are structured; the rest are the depositors' ordinary amounts | Structurers reuse a few people ("smurfing") and run campaigns over weeks so no single day shows the pattern. The FFIEC BSA/AML manual describes both "just under" amounts and varied amounts meant to avoid an obvious pattern |
| `micro_structuring`, amounts | Structured amounts sit `threshold x 0.4 x (1 - sqrt(u))` below the threshold: a triangular density highest at the threshold and finite there, always under it, no step at W2's 90% band floor, about 44% inside W2's band | as above |
| `dormant_reactivation` | Dormancy log-uniform 45 to 365 days. Two thirds of reactivations are sudden (2 days); one third return to use over 4 to 10 days (short, so few straddle a month boundary, where the monthly unit would see the burst without its gap). Amounts stay the account's own draws | An account sending about monthly goes 45 days without a send about one time in five, so the short end overlaps normal gaps. Not every reactivation is one burst |
| `corridor_high_risk` | 2 to 4 payments from the subject to one counterparty in the higher-risk pool over 2 to 5 weeks, amounts from the sender's own distribution | W7's "corridors to high-risk jurisdictions" is about where an account's money goes; one payment cannot show that. About 38% of runs span two months; the monthly unit labels the last month and excludes the earlier, so a split run shows fewer corridor payments in its labelled month |

These three draw amounts from an instance-keyed stream
(`amounts::own_amount_stream`), so other typologies keep their amounts, times
and participants.

**Scheduled and bursty senders.** Real accounts mix steady cadences
(standing orders, bills, payroll, supplier runs) with bursts. The
pre-registered target: at least 15% of accounts with a gap coefficient of
variation below 0.5, and at least 15% above 1.0.

- `datagen_rs/src/regular.rs` makes 30% of accounts "scheduled": 75 to 95%
  of their expected sends follow a steady cadence (one payment every 1/K of
  calendar mass, rolled to the next business day); the rest stay random.
- The cadence is spaced in calendar mass, not wall-clock time, so scheduled
  rows keep the corpus's day-of-week, salary-day and quarter-end shape, which
  typology rows share. Even wall-clock spacing would put fewer rows on salary
  days and more on Mondays, and planted rows would stand out by date.
- Only timing changes. Expected total sends, amounts (persona draws),
  counterparty draws (the same ring and extended-band draw as a random row),
  every typology's rows and the corpus row count are unchanged.
- A dormant account's cadence stops during its dormancy.
- Scheduled events are computed per file from (seed, uid): memory stays
  O(accounts) at any scale and files depend only on (seed, file).
- Parameters are self-chosen. The primary source for the mixture split still
  awaits a citation.
