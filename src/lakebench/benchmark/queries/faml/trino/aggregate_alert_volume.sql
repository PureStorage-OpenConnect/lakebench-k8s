-- Q_alert_volume: total alerts by day / rule / priority. Rollup query
-- an analyst runs to see the day's alert load and prioritize triage.
SELECT
  DATE_TRUNC('day', alert_ts) AS day,
  rule_id,
  priority,
  COUNT(*) AS alerts,
  ROUND(SUM(alert_score), 2) AS total_score
FROM {catalog}.gold.alerts
WHERE alert_ts BETWEEN CURRENT_TIMESTAMP - INTERVAL '30' DAY AND CURRENT_TIMESTAMP
GROUP BY 1, 2, 3
ORDER BY 1, 2;
