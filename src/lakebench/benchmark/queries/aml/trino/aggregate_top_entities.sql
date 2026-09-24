-- Q_top_entities: top 100 entities by aggregate alert score (30d
-- window). Also reports rule diversity so an entity that trips many
-- different rules surfaces above one that trips one rule many times.
-- The 30-day window is anchored on the corpus's own data clock (the latest
-- alert), not the wall clock: the synthetic corpus ends before today, so a
-- CURRENT_TIMESTAMP window scanned nothing and the query timed as trivially
-- fast, inflating QpH.
WITH entity_scores AS (
  SELECT
    entity_id,
    SUM(alert_score) AS total_score,
    COUNT(*) AS alert_count,
    COUNT(DISTINCT rule_id) AS distinct_rules
  FROM {catalog}.gold.alerts
  WHERE alert_ts BETWEEN (SELECT MAX(alert_ts) FROM {catalog}.gold.alerts) - INTERVAL '30' DAY
                   AND (SELECT MAX(alert_ts) FROM {catalog}.gold.alerts)
  GROUP BY entity_id
)
SELECT
  es.entity_id,
  ent.name,
  ent.country,
  ROUND(es.total_score, 2) AS total_score,
  es.alert_count,
  es.distinct_rules
FROM entity_scores es
JOIN {catalog}.silver.entities ent ON es.entity_id = ent.entity_id
ORDER BY total_score DESC
LIMIT 100;
