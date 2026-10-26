# AML benchmark: detection rules

## 4.2 Detection rules

Gold-finalize runs the rules in the order W5, W6, W2, W3, W17, W4, W7, W8,
then W1. Each rule is marked
`pending` in `gold.detection_status`, alerts from other runs are deleted, then
each rule's rows are replaced. A rule that skips is recorded `skipped` with a
reason. Any other exception is recorded `error`, the rule's
alerts are removed, and the other rules still run.

Continuous gold-refresh runs W4, W2, W5, W6, W17 and W3 every tick. W1, W7 and
W8 are recorded `skipped` there. Continuous W5 is the transaction screen only;
continuous W4 raises one alert per entity per week, batch W4 one per entity.

| Rule id | Detects | Target typology | Base reason code | Conditional reason codes | HIGH priority cut | Modes |
|---|---|---|---|---|---|---|
| `W1_connected_components` | multi-entity graph clusters | `gather_scatter` | `W1_COMPONENT` | `W1_LARGE_COMPONENT` | component size 8 | batch |
| `W2_structuring` | 3 or more structuring-band payments in 24 h: per originator (`structuring`, tumbling day) and per beneficiary from 3 or more senders (`structuring_beneficiary`, sliding 24 h) | `micro_structuring` | `W2_SUB_THRESHOLD_BURST` | `W2_BENEFICIARY_FAN_IN`, `W2_HIGH_COUNT` | 6 in-band payments | batch, continuous |
| `W3_round_tripping` | funds returning to the originator through 2 to 5 transfers within 30 days | `cycle` | `W3_CYCLE` | `W3_LONG_CYCLE` | 4 hops | batch, continuous |
| `W4_risk_propagation` | pass-through of 80% or more within 6 h | `rapid_layering` | `W4_FAST_PASS_THROUGH` | `W4_MULTI_CHAIN` | 3 chains | batch, continuous |
| `W5_sanctions_match` | fuzzy screen of payment beneficiaries against the corpus's dated sanctions list, at payment time and as a rescreen when a list version is published | `sanctions_match` | `W5_SANCTIONS_HIT` | `W5_EXACT`, `W5_FUZZY`, `W5_RESCREEN` | -- | batch, continuous (transaction screen only) |
| `W6_pep_counterparty` | the same screen against the PEP list; MED priority at $10,000 or more, LOW below | `pep_match` | `W6_PEP_HIT` | `W6_EXACT`, `W6_FUZZY` | -- | batch, continuous |
| `W7_cross_border_high_risk` | cross-border payments into a FATF black or grey list country or a synthetic high-risk corridor country ([3.9](typologies.md#39-planting-shapes-and-baseline-timing)) | `corridor_high_risk` | `W7_HIGH_RISK_CORRIDOR` | `W7_FATF_BLACK`, `W7_FATF_GREY`, `W7_SYNTHETIC_CORRIDOR` | -- | batch |
| `W8_dormant_reactivation` | a payment of $5,000 or more from a customer account idle for 90 days or more | `dormant_reactivation` | `W8_DORMANCY_GAP` | none | -- | batch |
| `W17_layering_chain` | open chains of 3 to 6 transfers, each forwarding 80 to 100% of the previous within 7 days | `stack` | `W17_CHAIN` | `W17_LONG_CHAIN` | 5 hops | batch, continuous |

- Scoring and the verdict read the target typology column, never the
  manifest's coarser `workload` category.

**Screen bounds** (W5, W6).

- Soundex codes of token pairs only block candidate pairs.
- A match is Levenshtein similarity of at least 0.85 over normalised,
  token-sorted names.
- The entry's country is the only secondary identifier. There is no phonetic
  scoring, date of birth or identifier matching.
- W6 screens every payment; its amount cut sets only priority, so its recall
  measures the screen, not the amount distribution.

**Path caps.** W3 and W17 are recorded `skipped` (`path-cap`) when their path
search would not fit the job's scratch (budget in
[7.4](execution-rules.md#74-lakebench-imposed-caps)). A `path-cap` skip, like
W1's `vertex-cap`, is a Lakebench cap: the run can still pass and the skip is
labelled in `limits.bound` and the verdict's `rule_caps` qualifier (see "What
a PASSED verdict asserts" in [benchmarking.md](../../benchmarking.md)).

**Hubs in W3 and W17.** An account sending more than 200 transfers in a week
is not an intermediary for that week.

**Reason codes.** Every alert carries
`reason_codes`: its rule's base code, then each conditional code whose
condition holds, so a rule's codes cover all its alerts.

- Conditional codes read only columns the rule's projection holds and cut
  points the rule already has: its HIGH cut, the screen's exact or fuzzy split
  at similarity 1.0, the rescreen pass, the corridor list's risk tier. No code
  adds a threshold or changes which alerts a rule raises.
- `W2_BENEFICIARY_FAN_IN` marks the beneficiary kind; `*_EXACT` and
  `*_FUZZY` the name match; `W5_RESCREEN` an alert raised when a list version
  listed a prior counterparty.
- A code reading the HIGH cut (`W1_LARGE_COMPONENT`, `W2_HIGH_COUNT`,
  `W3_LONG_CYCLE`, `W4_MULTI_CHAIN`, `W17_LONG_CHAIN`) always marks a
  HIGH-priority alert.
- The generator's home countries include no FATF-listed jurisdiction, so on
  generated corpora W7 alerts carry `W7_SYNTHETIC_CORRIDOR` and the two FATF
  codes have no alerts.
