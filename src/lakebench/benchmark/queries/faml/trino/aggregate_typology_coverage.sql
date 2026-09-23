-- Q_typology_coverage_matrix: which planted typologies are detected by
-- which rules. A cell says: "N distinct typologies of this type
-- generated at least one alert of this rule." Empty cells reveal
-- typology/rule gaps the operator should close.
WITH typology_alerts AS (
  SELECT
    m.typology_type,
    m.typology_id,
    a.rule_id,
    COUNT(*) AS alert_count
  FROM {catalog}.bronze.manifest m
  JOIN {catalog}.gold.alerts a
    ON ARRAYS_OVERLAP(a.related_txn_ids, m.participant_uetrs)
  GROUP BY m.typology_type, m.typology_id, a.rule_id
)
SELECT
  typology_type,
  rule_id,
  COUNT(DISTINCT typology_id) AS typologies_detected
FROM typology_alerts
GROUP BY typology_type, rule_id
ORDER BY typology_type, rule_id;
