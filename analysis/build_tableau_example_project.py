#!/usr/bin/env python3
import csv
import os
import shutil
import subprocess
import sys
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TABLEAU_INPUTS_DIR = os.path.join(BASE_DIR, "tableau_inputs")
EXAMPLE_DIR = os.path.join(BASE_DIR, "tableau_example_project")
DATA_DST_DIR = os.path.join(EXAMPLE_DIR, "data")


def ensure_tableau_inputs():
    req = [
        os.path.join(TABLEAU_INPUTS_DIR, "data", "fact_business_dynamics_wide_monthly.csv"),
        os.path.join(TABLEAU_INPUTS_DIR, "data", "fact_business_dynamics_long_monthly.csv"),
        os.path.join(TABLEAU_INPUTS_DIR, "data", "region_q1_2024_vs_2025_scorecard.csv"),
    ]
    missing = [p for p in req if not os.path.exists(p)]
    if missing:
        package_script = os.path.join(BASE_DIR, "analysis", "build_tableau_package.py")
        subprocess.run([sys.executable, package_script], check=True, cwd=BASE_DIR)
        missing_after = [p for p in req if not os.path.exists(p)]
        if missing_after:
            raise FileNotFoundError(
                "Missing Tableau inputs after rebuild attempt.\n" + "\n".join(missing_after)
            )


def copy_data_files():
    os.makedirs(DATA_DST_DIR, exist_ok=True)
    src_data = os.path.join(TABLEAU_INPUTS_DIR, "data")
    for name in sorted(os.listdir(src_data)):
        if name.endswith(".csv"):
            shutil.copy2(os.path.join(src_data, name), os.path.join(DATA_DST_DIR, name))


def write_text(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def write_sheet_specs():
    path = os.path.join(EXAMPLE_DIR, "sheet_specs.csv")
    rows = [
        {
            "dashboard_order": 1,
            "sheet_name": "Executive KPIs",
            "business_question": "Are openings keeping ahead of closures in the selected period?",
            "source_table": "fact_business_dynamics_wide_monthly.csv",
            "grain": "Month + selected geography",
            "chart_type": "KPI text table",
            "why_this_chart": "KPIs provide immediate status before detail views",
            "columns_shelf": "Measure Names",
            "rows_shelf": "Measure Values",
            "marks_card": "Text; include Openings, Closures, Net Openings, Close/Open Ratio",
            "required_filters": "geo_level in (National or Province/Territory via parameter); month range",
            "sort_logic": "N/A",
            "number_format": "Whole numbers for counts; percentage with 1 decimal for ratios",
            "labeling_and_tooltips": "Show latest month and selected-period totals in tooltip",
            "interactivity": "Parameter action to switch National vs Region view",
            "performance_guardrails": "Use extract; hide unused fields; keep to 4 KPIs",
            "accessibility_notes": "Use high contrast and avoid color-only encoding",
            "decision_output": "Headline health check for opening vs closure pressure",
        },
        {
            "dashboard_order": 2,
            "sheet_name": "National Trend (Core)",
            "business_question": "When did closures start to overtake openings nationally?",
            "source_table": "fact_business_dynamics_wide_monthly.csv",
            "grain": "Month",
            "chart_type": "Line chart (2 lines + optional net reference)",
            "why_this_chart": "Time-series is best for trend and crossover detection",
            "columns_shelf": "month",
            "rows_shelf": "SUM(opening_businesses) and SUM(closing_businesses)",
            "marks_card": "Color by measure; optional reference line at 0 net",
            "required_filters": "geo_level=National",
            "sort_logic": "Ascending month",
            "number_format": "Whole numbers with thousands separators",
            "labeling_and_tooltips": "Only label latest point; tooltip includes net_openings and YoY vs same month prior year",
            "interactivity": "Highlight action to update all downstream sheets by selected month",
            "performance_guardrails": "Limit marks to monthly grain; avoid high-cardinality tooltip fields",
            "accessibility_notes": "Colorblind-safe blue/orange palette; 12-14 px axis text",
            "decision_output": "Detect timing and persistence of national pressure",
        },
        {
            "dashboard_order": 3,
            "sheet_name": "Regional Pressure Map",
            "business_question": "Where is closure pressure highest in Q1 2025?",
            "source_table": "region_q1_2024_vs_2025_scorecard.csv",
            "grain": "Region",
            "chart_type": "Filled map + ranked bar companion",
            "why_this_chart": "Maps show geography quickly while ranked bars improve precision",
            "columns_shelf": "region",
            "rows_shelf": "closure_to_opening_ratio_q1_2025",
            "marks_card": "Color by ratio; size or label by net_q1_2025",
            "required_filters": "complete_data_open_close_q1_2024_2025=1",
            "sort_logic": "Descending by closure_to_opening_ratio_q1_2025 in bar companion",
            "number_format": "Ratio as 0.000; net as whole number",
            "labeling_and_tooltips": "Tooltip: region, openings, closures, ratio, net change",
            "interactivity": "Select region to filter decomposition sheets",
            "performance_guardrails": "Keep one map layer and minimal marks; avoid dense labels",
            "accessibility_notes": "Use sequential palette with clear midpoint at 1.0",
            "decision_output": "Prioritize high-pressure regions for retention support",
        },
        {
            "dashboard_order": 4,
            "sheet_name": "Regional Net Comparison",
            "business_question": "Which regions gained or lost net openings vs last year?",
            "source_table": "region_q1_2024_vs_2025_scorecard.csv",
            "grain": "Region",
            "chart_type": "Diverging bar chart",
            "why_this_chart": "Diverging bars communicate positive vs negative deltas clearly",
            "columns_shelf": "region",
            "rows_shelf": "net_change_abs",
            "marks_card": "Color by sign (positive/negative)",
            "required_filters": "complete_data_open_close_q1_2024_2025=1",
            "sort_logic": "Descending by net_change_abs",
            "number_format": "Whole numbers",
            "labeling_and_tooltips": "Show value labels for top/bottom performers only; tooltip includes YoY opening/closing %",
            "interactivity": "Click bar to filter action matrix and decomposition",
            "performance_guardrails": "Avoid dual-axis; one measure only for fast render",
            "accessibility_notes": "Red/blue pair with sufficient luminance contrast",
            "decision_output": "Target intervention where deterioration is largest",
        },
        {
            "dashboard_order": 5,
            "sheet_name": "Openings Mix (Entrants vs Reopenings)",
            "business_question": "Is regional growth coming from new firms or returning firms?",
            "source_table": "region_q1_2024_vs_2025_scorecard.csv",
            "grain": "Region",
            "chart_type": "100% stacked bar",
            "why_this_chart": "Composition view is ideal for part-to-whole comparison",
            "columns_shelf": "region",
            "rows_shelf": "SUM(entrants_q1_2025) and SUM(reopening_q1_2025)",
            "marks_card": "Color by component; show entrant_share",
            "required_filters": "complete_data_open_close_q1_2024_2025=1",
            "sort_logic": "Descending by openings_q1_2025",
            "number_format": "Percent axis (0-100%)",
            "labeling_and_tooltips": "Tooltip includes absolute counts and share percentages",
            "interactivity": "Region filter action from map/bar sheets",
            "performance_guardrails": "Use aggregated quarterly table (already pre-aggregated)",
            "accessibility_notes": "Use distinct patterns/labels in addition to color",
            "decision_output": "Decide whether programs should emphasize new-founder acquisition or reactivation",
        },
        {
            "dashboard_order": 6,
            "sheet_name": "Closures Mix (Exits vs Temporary)",
            "business_question": "Are closures structural (exits) or short-term (temporary)?",
            "source_table": "region_q1_2024_vs_2025_scorecard.csv",
            "grain": "Region",
            "chart_type": "Stacked bar",
            "why_this_chart": "Separates permanent loss from recoverable shutdown pressure",
            "columns_shelf": "region",
            "rows_shelf": "SUM(exits_q1_2025) and SUM(temp_closures_q1_2025)",
            "marks_card": "Color by closure type; annotate top 3 exit-heavy regions",
            "required_filters": "complete_data_open_close_q1_2024_2025=1",
            "sort_logic": "Descending by closures_q1_2025",
            "number_format": "Whole numbers and percentage in tooltip",
            "labeling_and_tooltips": "Tooltip includes exit_share_of_closings proxy and data note for unavailable exit values",
            "interactivity": "Cross-filter from regional map and net comparison",
            "performance_guardrails": "Warn users that exits may lag publication in latest periods",
            "accessibility_notes": "Add tooltip note for data completeness fields",
            "decision_output": "Choose between turnaround support vs exit-prevention initiatives",
        },
        {
            "dashboard_order": 7,
            "sheet_name": "Action Matrix",
            "business_question": "Which regions combine worsening risk and large business volume?",
            "source_table": "region_q1_2024_vs_2025_scorecard.csv",
            "grain": "Region",
            "chart_type": "Scatter plot (quadrant)",
            "why_this_chart": "Scatter with quadrants supports prioritization and segmentation",
            "columns_shelf": "openings_yoy_pct",
            "rows_shelf": "closures_yoy_pct",
            "marks_card": "Size by openings_q1_2025; color by net_change_abs or Risk Quadrant calc",
            "required_filters": "complete_data_open_close_q1_2024_2025=1",
            "sort_logic": "N/A",
            "number_format": "Percent axes with 0 lines",
            "labeling_and_tooltips": "Show region labels only for outliers and largest bubbles",
            "interactivity": "Reference lines at 0% and median values; click to keep/exclude",
            "performance_guardrails": "Keep row count small (regional summary only); avoid extra dimensions on marks",
            "accessibility_notes": "Do not rely on red/green only; include shape or label for quadrant",
            "decision_output": "Allocate funding and advisory intensity by quadrant",
        },
    ]
    cols = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)


def write_calc_catalog():
    path = os.path.join(EXAMPLE_DIR, "calculated_fields_catalog.csv")
    rows = [
        {
            "calc_name": "Net Openings",
            "table": "fact_business_dynamics_wide_monthly.csv",
            "formula": "SUM([opening_businesses]) - SUM([closing_businesses])",
            "purpose": "Core growth/decline signal.",
        },
        {
            "calc_name": "Close/Open Ratio",
            "table": "fact_business_dynamics_wide_monthly.csv",
            "formula": "SUM([closing_businesses]) / SUM([opening_businesses])",
            "purpose": "Closure pressure index.",
        },
        {
            "calc_name": "Entrant Share",
            "table": "fact_business_dynamics_wide_monthly.csv",
            "formula": "SUM([entrants]) / SUM([opening_businesses])",
            "purpose": "How much opening activity is true new creation.",
        },
        {
            "calc_name": "Reopening Share",
            "table": "fact_business_dynamics_wide_monthly.csv",
            "formula": "SUM([reopening_businesses]) / SUM([opening_businesses])",
            "purpose": "How much opening activity is returning businesses.",
        },
        {
            "calc_name": "Opening Identity Gap",
            "table": "fact_business_dynamics_wide_monthly.csv",
            "formula": "SUM([opening_businesses]) - (SUM([entrants]) + SUM([reopening_businesses]))",
            "purpose": "Data quality validation.",
        },
        {
            "calc_name": "Closing Identity Gap",
            "table": "fact_business_dynamics_wide_monthly.csv",
            "formula": "SUM([closing_businesses]) - (SUM([exits]) + SUM([temporary_closures]))",
            "purpose": "Data quality validation.",
        },
        {
            "calc_name": "Risk Quadrant",
            "table": "region_q1_2024_vs_2025_scorecard.csv",
            "formula": "IF [closures_yoy_pct] > 0 AND [openings_yoy_pct] <= 0 THEN 'High Risk' ELSEIF [closures_yoy_pct] > [openings_yoy_pct] THEN 'Pressure Rising' ELSE 'Relatively Stable/Growing' END",
            "purpose": "Segment regions for action recommendations.",
        },
    ]
    cols = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)


def write_readme():
    content = f"""# Tableau Example Project (Auto-Generated)

Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

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
"""
    write_text(os.path.join(EXAMPLE_DIR, "README.md"), content)


def write_storyboard():
    content = """# Dashboard Storyboard

## Dashboard 1: National Pulse
- KPI tiles: Openings (latest month), Closures (latest month), Net Openings, Close/Open Ratio
- Line chart: Openings vs Closures over time
- Message: "Closures have risen faster than openings, compressing net creation."

## Dashboard 2: Regional Risk & Opportunity
- Filled map or bar chart: closure_to_opening_ratio_q1_2025 by region
- Table: net_q1_2025 and net_change_abs
- Message: "Risk is uneven by region; priorities must be targeted, not uniform."

## Dashboard 3: Business Dynamics Decomposition
- Stacked bars (openings): entrants vs reopening
- Stacked bars (closings): exits vs temporary closures
- Message: "Not all openings/closures are alike; support should match the composition."

## Dashboard 4: Action Matrix
- Scatter:
  - X = openings_yoy_pct
  - Y = closures_yoy_pct
  - Color = risk quadrant
  - Size = openings_q1_2025
- Message: "Direct funding and advisory support by quadrant."
"""
    write_text(os.path.join(EXAMPLE_DIR, "dashboard_storyboard.md"), content)


def write_talk_track():
    content = """# Presentation Talk Track (Analytics-Focused)

1. Start with trend evidence.
   - "Nationally, openings edged up, but closures increased faster, reducing net creation."

2. Move to geography.
   - "This is not a uniform national pattern. A few regions drive positive net creation; others face closure pressure."

3. Explain composition.
   - "Openings include entrants and reopenings; closures include exits and temporary closures. This changes intervention design."

4. Prioritize action.
   - "High closure pressure + large base size = immediate retention intervention."
   - "Positive momentum regions = scale acceleration."

5. Close with measurable plan.
   - "Track monthly close/open ratio, net openings, and entrant share as leading indicators."
"""
    write_text(os.path.join(EXAMPLE_DIR, "talk_track.md"), content)


def main():
    ensure_tableau_inputs()
    os.makedirs(EXAMPLE_DIR, exist_ok=True)
    copy_data_files()
    write_sheet_specs()
    write_calc_catalog()
    write_readme()
    write_storyboard()
    write_talk_track()
    print("Tableau example project generated:")
    print(EXAMPLE_DIR)


if __name__ == "__main__":
    main()
