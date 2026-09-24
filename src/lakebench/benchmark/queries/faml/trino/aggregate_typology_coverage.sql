-- Q_typology_coverage_matrix: which planted typologies are detected by
-- which rules. A cell says: "N distinct typologies of this type
-- generated at least one alert of this rule." Empty cells reveal
-- typology/rule gaps the operator should close.
-- Equi-join on unnested uetrs; alerts scoped to the run in
-- gold.detection_status.
WITH run AS (
  SELECT MAX(run_id) AS run_id FROM {catalog}.gold.detection_status
),
typ_uetrs AS (
  SELECT m.typology_type, m.typology_id, u.uetr
  FROM {catalog}.bronze.manifest m
  CROSS JOIN UNNEST(m.participant_uetrs) AS u(uetr)
),
alert_uetrs AS (
  SELECT DISTINCT a.rule_id, t.uetr
  FROM {catalog}.gold.alerts a
  CROSS JOIN UNNEST(a.related_txn_ids) AS t(uetr)
  JOIN run ON a.run_id = run.run_id
)
SELECT
  tu.typology_type,
  au.rule_id,
  COUNT(DISTINCT tu.typology_id) AS typologies_detected
FROM typ_uetrs tu
JOIN alert_uetrs au ON au.uetr = tu.uetr
GROUP BY tu.typology_type, au.rule_id
ORDER BY tu.typology_type, au.rule_id;
