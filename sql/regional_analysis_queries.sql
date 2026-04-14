-- Regional business openings and closures analysis by region
-- Source table in SQLite DB created by analysis/build_regional_analysis.py:
--   outputs/business_dynamics_q1.db
--   tables: regional_monthly, regional_summary_q1, national_monthly

-- 1) Region-level Q1 2024 vs Q1 2025 comparison
SELECT
  region,
  openings_2024_q1,
  closures_2024_q1,
  net_2024_q1,
  openings_2025_q1,
  closures_2025_q1,
  net_2025_q1,
  ROUND(openings_yoy_pct, 2) AS openings_yoy_pct,
  ROUND(closures_yoy_pct, 2) AS closures_yoy_pct,
  net_change_abs,
  ROUND(closure_to_opening_2025_q1, 3) AS closure_to_opening_2025_q1
FROM regional_summary_q1
WHERE complete_data_q1_2024_2025 = 1
ORDER BY net_2025_q1 DESC;

-- 2) Regions where closure growth is faster than opening growth
SELECT
  region,
  ROUND(openings_yoy_pct, 2) AS openings_yoy_pct,
  ROUND(closures_yoy_pct, 2) AS closures_yoy_pct,
  net_change_abs
FROM regional_summary_q1
WHERE complete_data_q1_2024_2025 = 1
  AND closures_yoy_pct > openings_yoy_pct
ORDER BY (closures_yoy_pct - openings_yoy_pct) DESC;

-- 3) Monthly national view (sum across regions with available data)
SELECT
  month,
  openings,
  closures,
  net,
  regions_used,
  regions_missing
FROM national_monthly
ORDER BY month;

-- 4) Regions under the highest closure pressure in Q1 2025
SELECT
  region,
  openings_2025_q1,
  closures_2025_q1,
  ROUND(closure_to_opening_2025_q1, 3) AS closure_to_opening_2025_q1,
  net_2025_q1
FROM regional_summary_q1
WHERE complete_data_q1_2024_2025 = 1
ORDER BY closure_to_opening_2025_q1 DESC, openings_2025_q1 DESC;
