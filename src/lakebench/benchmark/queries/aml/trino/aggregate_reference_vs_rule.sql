-- Q_reference_vs_rule: rule recall vs reference-model recall per typology.
--
-- Answers: "does our rule outscore what a canonical detector could
-- learn from the same data?" A rule that scores far above the
-- reference detector is either (a) a genuinely smart heuristic the
-- model cannot find with a simple feature set, or (b) benefiting
-- from a label leak the leakage gate should have flagged. Cross-
-- reference with `leakage_report.parquet`: if that shipped any
-- `verdict = "leaking"` row for a currency the typology touches,
-- read case (b) first.
--
-- Recall from the rules comes from the `score_financial.py` output
-- registered as {catalog}.metrics.recall; reference recall comes
-- from `score_financial_reference.py` writing to
-- {catalog}.metrics.reference_metrics (row_kind = "typology").
-- Both tables land alongside `gold.alerts`.
--
-- Model-didn't-run vs model-got-zero (P3 finding from PR-A
-- adversarial review): keep reference numbers as NULL when the
-- typology has no row in reference_metrics, and label the verdict
-- 'not_run'. A hard COALESCE to 0.0 would render both cases as
-- "reference_recall = 0.000" and a reader focused on the numeric
-- gap would misread "not run" as "model failed to detect anything".
SELECT
  r.typology_type,
  ROUND(r.recall, 3)                              AS rule_recall,
  CASE WHEN m.recall    IS NULL THEN NULL ELSE ROUND(m.recall, 3)    END AS reference_recall,
  CASE WHEN m.precision IS NULL THEN NULL ELSE ROUND(m.precision, 3) END AS reference_precision,
  CASE WHEN m.f1        IS NULL THEN NULL ELSE ROUND(m.f1, 3)        END AS reference_f1,
  CASE WHEN m.recall    IS NULL THEN NULL ELSE ROUND(r.recall - m.recall, 3) END AS recall_gap,
  COALESCE(m.verdict, 'not_run')                  AS reference_verdict
FROM {catalog}.metrics.recall AS r
LEFT JOIN {catalog}.metrics.reference_metrics AS m
       ON m.typology_type = r.typology_type
      AND m.row_kind      = 'typology'
-- NULLS LAST so scored typologies with the biggest gap surface first,
-- and unscored ones sit at the bottom rather than pretending to top
-- the list with a fabricated zero.
ORDER BY recall_gap DESC NULLS LAST, r.typology_type;
