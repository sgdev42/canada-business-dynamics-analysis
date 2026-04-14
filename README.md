# Canada Business Openings & Closures Analysis

Portfolio analytics project using Statistics Canada business dynamics data to compare regional performance across **Q1 2024 vs Q1 2025**.

## Project Highlights
- Built an end-to-end analytics workflow (data acquisition, transformation, QA, insight synthesis).
- Produced decision-oriented regional analysis of openings, closures, and net business creation.
- Created a Tableau-ready project package (data model, sheet specs, calculated fields, storyboard).
- Delivered an **offline interactive HTML demo** for easy sharing without Tableau setup.

## Key Results (Q1 2025 vs Q1 2024, complete open/close regions)
- Openings: `134,484 -> 135,088` (+0.45%)
- Closures: `130,918 -> 134,632` (+2.84%)
- Net openings: `3,566 -> 456` (down `3,110`)
- Highest net openings (Q1 2025): British Columbia, Alberta, Quebec

## Repo Structure
- `analysis/` reproducible Python build scripts
- `data/` source metadata (large raw CSV excluded from git)
- `outputs/` analytical output tables
- `sql/` reusable SQL query pack
- `presentation/` concise business summary deliverables
- `tableau_example_project/` Tableau blueprint + interactive demo assets

## Run Locally
From repo root:

```bash
python3 analysis/build_regional_analysis.py
python3 analysis/build_tableau_example_project.py
python3 analysis/build_interactive_html_demo.py
```

Open:
- `tableau_example_project/interactive_demo_offline.html`

## Portfolio Artifacts
- Executive summary: `presentation/regional_business_dynamics_summary.md`
- Regional comparison table: `outputs/regional_summary_q1_2024_vs_2025.csv`
- Tableau design spec: `tableau_example_project/sheet_specs.csv`
- Offline demo: `tableau_example_project/interactive_demo_offline.html`

## Data Source
- Statistics Canada table 33-10-0270-01  
  https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3310027001

## Notes
- Large raw artifacts are excluded for GitHub compatibility (`data/33100270.csv`, zipped source extract).
- Suppressed/unavailable values (`STATUS = x`) are explicitly handled in scripts.
- `tableau_inputs/` is generated as needed and intentionally excluded from version control.
