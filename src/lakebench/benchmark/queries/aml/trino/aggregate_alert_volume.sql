-- Q_alert_volume: total alerts by day / rule / priority. Rollup query
-- an analyst runs to see the day's alert load and prioritize triage.
-- The 30-day window is anchored on the corpus's own data clock (the latest
-- alert), not the wall clock: the synthetic corpus ends before today, so a
-- CURRENT_TIMESTAMP window scanned nothing and the query timed as trivially
-- fast, inflating QpH.
SELECT
  DATE_TRUNC('day', alert_ts) AS day,
  rule_id,
  priority,
  COUNT(*) AS alerts,
  ROUND(SUM(alert_score), 2) AS total_score
FROM {catalog}.gold.alerts
WHERE alert_ts BETWEEN (SELECT MAX(alert_ts) FROM {catalog}.gold.alerts) - INTERVAL '30' DAY
                   AND (SELECT MAX(alert_ts) FROM {catalog}.gold.alerts)
GROUP BY 1, 2, 3
ORDER BY 1, 2;
