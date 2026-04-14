# Tableau Example Project (Auto-Generated)

Generated: 2026-04-13 23:14:45

This folder is a **Tableau starter project** built from a regional business dynamics analysis.

## What's included
- `data/`:
  - `fact_business_dynamics_wide_monthly.csv` (primary table)
  - `fact_business_dynamics_long_monthly.csv` (measure-flexible table)
  - `region_q1_2024_vs_2025_scorecard.csv` (Q1 comparison)
  - `dim_region.csv`, `dim_time.csv`, `dim_measure.csv` (optional dimensions)
- `sheet_specs.csv`: worksheet-by-worksheet design blueprint
- `calculated_fields_catalog.csv`: ready Tableau calculations
- `dashboard_storyboard.md`: presentation flow for analytics demonstration
- `talk_track.md`: concise script to explain analytical reasoning

## How to use in Tableau (Desktop Free Edition or Tableau Public)
1. Open Tableau and connect to `data/fact_business_dynamics_wide_monthly.csv`.
2. Add relationships to:
   - `data/region_q1_2024_vs_2025_scorecard.csv` via `region`
   - `data/dim_time.csv` via `month`
   - `data/dim_region.csv` via `region`
3. Build sheets using `sheet_specs.csv`.
4. Add calculated fields from `calculated_fields_catalog.csv`.
5. Assemble dashboard in the storyboard order.

## Primary focus for this assignment
Use this sequence to show analytics skill:
- trend detection
- decomposition analysis
- regional segmentation
- action recommendation tied directly to metrics

## Note
This package auto-generates all assets except a binary Tableau workbook file (`.twb/.twbx`), which must be saved from Tableau itself after opening the data.
