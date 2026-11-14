-- Citi Bike availability dashboard (Databricks AI/BI dashboard, 2X-Small SQL warehouse)
-- Create one dataset per query below, then add the widget named in the header.
-- Replace workspace.citibike if you changed CATALOG/SCHEMA in config.py.

-- ============================================================
-- 1. KPI tiles: pipeline health right now          (Counter x4)
-- ============================================================
SELECT
  ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(MAX(s.snapshot_ts))) / 60, 1) AS snapshot_age_min,
  (SELECT ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(MAX(scored_at))) / 60, 1)
     FROM workspace.citibike.predictions)                                                 AS prediction_age_min,
  (SELECT COUNT(*) FROM workspace.citibike.collector_runs
    WHERE fetched_at >= current_timestamp() - INTERVAL 24 HOURS AND status = 'OK')        AS collector_runs_ok_24h,
  (SELECT COUNT(*) FROM workspace.citibike.collector_runs
    WHERE fetched_at >= current_timestamp() - INTERVAL 24 HOURS AND status <> 'OK')       AS collector_runs_failed_24h
FROM workspace.citibike.live_station_status s;

-- ============================================================
-- 2. Latest predictions on a map                  (Point map: lat/lon, color = risk)
-- ============================================================
WITH latest AS (SELECT MAX(prediction_time_utc) AS t FROM workspace.citibike.predictions)
SELECT p.station_id, i.name, i.lat, i.lon, i.capacity,
       p.num_bikes_available, p.num_docks_available,
       ROUND(p.p_empty, 3) AS p_empty, ROUND(p.p_full, 3) AS p_full,
       CASE WHEN p.pred_empty = 1 THEN 'Likely EMPTY'
            WHEN p.pred_full  = 1 THEN 'Likely FULL'
            ELSE 'OK' END                                         AS risk,
       from_utc_timestamp(p.prediction_time_utc, 'America/New_York') AS predicted_at_local,
       from_utc_timestamp(p.target_time_utc,    'America/New_York') AS for_time_local
FROM workspace.citibike.predictions p
JOIN latest ON p.prediction_time_utc = latest.t
LEFT JOIN workspace.citibike.live_station_info i USING (station_id);

-- ============================================================
-- 3. Highest-risk stations now                    (Table, sorted)
-- ============================================================
WITH latest AS (SELECT MAX(prediction_time_utc) AS t FROM workspace.citibike.predictions)
SELECT i.name, p.num_bikes_available AS bikes_now, p.num_docks_available AS docks_now,
       ROUND(p.p_empty, 3) AS p_empty_15m, ROUND(p.p_full, 3) AS p_full_15m,
       GREATEST(p.p_empty, p.p_full) AS max_risk
FROM workspace.citibike.predictions p
JOIN latest ON p.prediction_time_utc = latest.t
LEFT JOIN workspace.citibike.live_station_info i USING (station_id)
ORDER BY max_risk DESC
LIMIT 20;

-- ============================================================
-- 4. Offline test results: model vs persistence    (Bar: test_pr_auc by model, split by target)
-- ============================================================
SELECT target, model,
       ROUND(test_pr_auc, 3) AS test_pr_auc, ROUND(test_roc_auc, 3) AS test_roc_auc,
       ROUND(test_brier, 4) AS test_brier, ROUND(test_precision, 3) AS precision,
       ROUND(test_recall, 3) AS recall, ROUND(test_f1, 3) AS f1,
       ROUND(test_prevalence, 4) AS prevalence, test_n AS n
FROM workspace.citibike.model_comparison
ORDER BY target, test_pr_auc DESC;

-- ============================================================
-- 5. Reliability (calibration) on the test period  (Line: mean_pred vs obs_rate, series = model)
-- ============================================================
SELECT target, model, bin, mean_pred, obs_rate, n
FROM workspace.citibike.calibration_bins
WHERE model IN ('persistence_calibrated', 'logreg', 'hgb') AND n >= 30
ORDER BY target, model, bin;

-- ============================================================
-- 6. Live accuracy by hour (realized outcomes)     (Line: brier by hour, series = target)
-- ============================================================
SELECT date_trunc('HOUR', from_utc_timestamp(prediction_time_utc, 'America/New_York')) AS hour_local,
       COUNT(*)                                       AS n,
       AVG(POW(p_empty - empty_label, 2))             AS brier_empty,
       AVG(POW(p_full  - full_label, 2))              AS brier_full,
       AVG(empty_label) AS empty_rate, AVG(p_empty) AS mean_p_empty,
       AVG(full_label)  AS full_rate,  AVG(p_full)  AS mean_p_full
FROM workspace.citibike.live_outcomes
GROUP BY 1
ORDER BY 1;

-- ============================================================
-- 7. Live confusion counts, last 24h               (Table)
-- ============================================================
SELECT 'empty' AS target,
       SUM(CASE WHEN pred_empty = 1 AND empty_label = 1 THEN 1 ELSE 0 END) AS tp,
       SUM(CASE WHEN pred_empty = 1 AND empty_label = 0 THEN 1 ELSE 0 END) AS fp,
       SUM(CASE WHEN pred_empty = 0 AND empty_label = 1 THEN 1 ELSE 0 END) AS fn,
       SUM(CASE WHEN pred_empty = 0 AND empty_label = 0 THEN 1 ELSE 0 END) AS tn
FROM workspace.citibike.live_outcomes
WHERE prediction_time_utc >= current_timestamp() - INTERVAL 24 HOURS
UNION ALL
SELECT 'full',
       SUM(CASE WHEN pred_full = 1 AND full_label = 1 THEN 1 ELSE 0 END),
       SUM(CASE WHEN pred_full = 1 AND full_label = 0 THEN 1 ELSE 0 END),
       SUM(CASE WHEN pred_full = 0 AND full_label = 1 THEN 1 ELSE 0 END),
       SUM(CASE WHEN pred_full = 0 AND full_label = 0 THEN 1 ELSE 0 END)
FROM workspace.citibike.live_outcomes
WHERE prediction_time_utc >= current_timestamp() - INTERVAL 24 HOURS;

-- ============================================================
-- 8. Monitoring log                                (Table, newest first)
-- ============================================================
SELECT checked_at, check, ROUND(value, 3) AS value, ok, fatal, detail
FROM workspace.citibike.monitoring_log
WHERE checked_at >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY checked_at DESC, check;
