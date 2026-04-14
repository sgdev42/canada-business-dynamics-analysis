#!/usr/bin/env python3
import csv
import os
import re
import zipfile
from collections import defaultdict
from datetime import datetime
from urllib.request import urlopen

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_PATH = os.path.join(BASE_DIR, "data", "33100270.csv")
META_PATH = os.path.join(BASE_DIR, "data", "33100270_MetaData.csv")
ZIP_PATH = os.path.join(BASE_DIR, "data_33100270_eng.zip")
ZIP_URL = "https://www150.statcan.gc.ca/n1/tbl/csv/33100270-eng.zip"
TABLEAU_DIR = os.path.join(BASE_DIR, "tableau_inputs")
TABLEAU_DATA_DIR = os.path.join(TABLEAU_DIR, "data")

TARGET_INDUSTRY = "Business sector industries [T004]"
TARGET_MEASURES = [
    "Active businesses",
    "Opening businesses",
    "Continuing businesses",
    "Closing businesses",
    "Reopening businesses",
    "Entrants",
    "Temporary closures",
    "Exits",
]


REGION_GROUPS = {
    "Newfoundland and Labrador": "Atlantic",
    "Prince Edward Island": "Atlantic",
    "Nova Scotia": "Atlantic",
    "New Brunswick": "Atlantic",
    "Quebec": "Central",
    "Ontario": "Central",
    "Manitoba": "Prairies",
    "Saskatchewan": "Prairies",
    "Alberta": "Prairies",
    "British Columbia": "West",
    "Yukon": "North",
    "Northwest Territories": "North",
    "Nunavut": "North",
    "Canada": "National",
}

REGION_ABBR = {
    "Newfoundland and Labrador": "NL",
    "Prince Edward Island": "PE",
    "Nova Scotia": "NS",
    "New Brunswick": "NB",
    "Quebec": "QC",
    "Ontario": "ON",
    "Manitoba": "MB",
    "Saskatchewan": "SK",
    "Alberta": "AB",
    "British Columbia": "BC",
    "Yukon": "YT",
    "Northwest Territories": "NT",
    "Nunavut": "NU",
    "Canada": "CA",
}


def ensure_data_files():
    os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)
    if os.path.exists(DATA_PATH) and os.path.exists(META_PATH):
        return
    if not os.path.exists(ZIP_PATH):
        with urlopen(ZIP_URL, timeout=60) as r:
            payload = r.read()
        with open(ZIP_PATH, "wb") as f:
            f.write(payload)
    with zipfile.ZipFile(ZIP_PATH, "r") as zf:
        zf.extractall(os.path.join(BASE_DIR, "data"))


def write_csv(path, rows, columns):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in columns})


def get_pt_regions_from_meta():
    pt_regions = []
    with open(META_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        in_members = False
        for row in reader:
            if row[:2] == ["Dimension ID", "Member Name"]:
                in_members = True
                continue
            if not in_members or len(row) < 5:
                continue
            if row[0] != "1":
                continue
            name = row[1].replace("\xa0", "").strip()
            parent = (row[4] or "").strip()
            class_code = (row[2] or "").strip()
            if (
                parent == "1"
                and name != "Canada"
                and re.match(r"^\[\d{2}\]$", class_code)
            ):
                pt_regions.append(name)
    return sorted(set(pt_regions))


def load_long_fact():
    pt_regions = set(get_pt_regions_from_meta())
    geos = pt_regions | {"Canada"}
    rows = []
    month_set = set()

    with open(DATA_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            geo = row["GEO"].replace("\xa0", "").strip()
            if geo not in geos:
                continue
            if row["Industry"] != TARGET_INDUSTRY:
                continue
            measure = row["Business dynamics measure"]
            if measure not in TARGET_MEASURES:
                continue

            month = row["REF_DATE"]
            month_set.add(month)
            status = (row["STATUS"] or "").strip()
            value_text = (row["VALUE"] or "").strip()
            value = float(value_text) if value_text else None
            dt = datetime.strptime(month + "-01", "%Y-%m-%d")
            q = (dt.month - 1) // 3 + 1
            rows.append(
                {
                    "month": month,
                    "year": dt.year,
                    "month_num": dt.month,
                    "quarter_label": f"Q{q}",
                    "year_quarter": f"{dt.year}-Q{q}",
                    "region": geo,
                    "region_abbr": REGION_ABBR.get(geo, ""),
                    "region_group": REGION_GROUPS.get(geo, "Other"),
                    "geo_level": "National" if geo == "Canada" else "Province/Territory",
                    "industry": row["Industry"],
                    "measure": measure,
                    "value": value,
                    "status": status,
                    "data_available": int(value is not None),
                }
            )
    return rows, sorted(month_set)


def build_wide_fact(long_rows):
    base = defaultdict(
        lambda: {
            "active_businesses": None,
            "opening_businesses": None,
            "continuing_businesses": None,
            "closing_businesses": None,
            "reopening_businesses": None,
            "entrants": None,
            "temporary_closures": None,
            "exits": None,
            "suppressed_count": 0,
            "available_measures": 0,
        }
    )
    measure_to_col = {
        "Active businesses": "active_businesses",
        "Opening businesses": "opening_businesses",
        "Continuing businesses": "continuing_businesses",
        "Closing businesses": "closing_businesses",
        "Reopening businesses": "reopening_businesses",
        "Entrants": "entrants",
        "Temporary closures": "temporary_closures",
        "Exits": "exits",
    }

    for row in long_rows:
        key = (
            row["month"],
            row["year"],
            row["month_num"],
            row["quarter_label"],
            row["year_quarter"],
            row["region"],
            row["region_abbr"],
            row["region_group"],
            row["geo_level"],
            row["industry"],
        )
        b = base[key]
        col = measure_to_col[row["measure"]]
        b[col] = row["value"]
        if row["status"] == "x":
            b["suppressed_count"] += 1
        if row["data_available"] == 1:
            b["available_measures"] += 1

    out = []
    for key, b in base.items():
        (
            month,
            year,
            month_num,
            quarter_label,
            year_quarter,
            region,
            region_abbr,
            region_group,
            geo_level,
            industry,
        ) = key
        o = b["opening_businesses"]
        c = b["closing_businesses"]
        e = b["entrants"]
        r = b["reopening_businesses"]
        x = b["exits"]
        t = b["temporary_closures"]

        net = (o - c) if (o is not None and c is not None) else None
        gross_churn = (o + c) if (o is not None and c is not None) else None
        close_open_ratio = (c / o) if (o not in (None, 0) and c is not None) else None
        entrant_share = (e / o) if (o not in (None, 0) and e is not None) else None
        reopening_share = (r / o) if (o not in (None, 0) and r is not None) else None
        exit_share_close = (x / c) if (c not in (None, 0) and x is not None) else None
        temp_share_close = (t / c) if (c not in (None, 0) and t is not None) else None

        formula_check_open = (
            int(abs(o - (e + r)) < 1e-9)
            if None not in (o, e, r)
            else None
        )
        formula_check_close = (
            int(abs(c - (x + t)) < 1e-9)
            if None not in (c, x, t)
            else None
        )

        out.append(
            {
                "month": month,
                "year": year,
                "month_num": month_num,
                "quarter_label": quarter_label,
                "year_quarter": year_quarter,
                "region": region,
                "region_abbr": region_abbr,
                "region_group": region_group,
                "geo_level": geo_level,
                "industry": industry,
                "active_businesses": b["active_businesses"],
                "opening_businesses": o,
                "continuing_businesses": b["continuing_businesses"],
                "closing_businesses": c,
                "reopening_businesses": r,
                "entrants": e,
                "temporary_closures": t,
                "exits": x,
                "net_openings": net,
                "gross_churn": gross_churn,
                "closure_to_opening_ratio": close_open_ratio,
                "entrant_share_of_openings": entrant_share,
                "reopening_share_of_openings": reopening_share,
                "exit_share_of_closings": exit_share_close,
                "temporary_closure_share_of_closings": temp_share_close,
                "formula_check_opening_eq_entrants_plus_reopening": formula_check_open,
                "formula_check_closing_eq_exits_plus_tempclosures": formula_check_close,
                "suppressed_measure_count": b["suppressed_count"],
                "complete_row_all_8_measures": int(b["available_measures"] == 8),
            }
        )
    return sorted(out, key=lambda x: (x["region"], x["month"]))


def build_q1_compare(wide_rows):
    months_2024 = {"2024-01", "2024-02", "2024-03"}
    months_2025 = {"2025-01", "2025-02", "2025-03"}
    grouped = defaultdict(list)
    for r in wide_rows:
        if r["geo_level"] != "Province/Territory":
            continue
        if r["month"] in months_2024 or r["month"] in months_2025:
            grouped[r["region"]].append(r)

    out = []
    for region, rows in grouped.items():
        a24 = [r for r in rows if r["month"] in months_2024]
        a25 = [r for r in rows if r["month"] in months_2025]
        full24 = len(a24) == 3 and all(r["complete_row_all_8_measures"] == 1 for r in a24)
        full25 = len(a25) == 3 and all(r["complete_row_all_8_measures"] == 1 for r in a25)

        oc24 = (
            len(a24) == 3
            and all(r["opening_businesses"] is not None and r["closing_businesses"] is not None for r in a24)
        )
        oc25 = (
            len(a25) == 3
            and all(r["opening_businesses"] is not None and r["closing_businesses"] is not None for r in a25)
        )

        def s(arr, col):
            vals = [r[col] for r in arr if r[col] is not None]
            return sum(vals) if len(vals) == len(arr) else None

        o24, c24 = s(a24, "opening_businesses"), s(a24, "closing_businesses")
        o25, c25 = s(a25, "opening_businesses"), s(a25, "closing_businesses")
        e24, r24 = s(a24, "entrants"), s(a24, "reopening_businesses")
        e25, r25 = s(a25, "entrants"), s(a25, "reopening_businesses")
        x24, t24 = s(a24, "exits"), s(a24, "temporary_closures")
        x25, t25 = s(a25, "exits"), s(a25, "temporary_closures")

        n24 = (o24 - c24) if (o24 is not None and c24 is not None) else None
        n25 = (o25 - c25) if (o25 is not None and c25 is not None) else None
        out.append(
            {
                "region": region,
                "region_abbr": REGION_ABBR.get(region, ""),
                "region_group": REGION_GROUPS.get(region, "Other"),
                "openings_q1_2024": o24,
                "closures_q1_2024": c24,
                "net_q1_2024": n24,
                "entrants_q1_2024": e24,
                "reopening_q1_2024": r24,
                "exits_q1_2024": x24,
                "temp_closures_q1_2024": t24,
                "openings_q1_2025": o25,
                "closures_q1_2025": c25,
                "net_q1_2025": n25,
                "entrants_q1_2025": e25,
                "reopening_q1_2025": r25,
                "exits_q1_2025": x25,
                "temp_closures_q1_2025": t25,
                "openings_yoy_pct": ((o25 - o24) / o24 * 100) if (o24 not in (None, 0) and o25 is not None) else None,
                "closures_yoy_pct": ((c25 - c24) / c24 * 100) if (c24 not in (None, 0) and c25 is not None) else None,
                "net_change_abs": (n25 - n24) if (n24 is not None and n25 is not None) else None,
                "closure_to_opening_ratio_q1_2025": (c25 / o25) if (o25 not in (None, 0) and c25 is not None) else None,
                "complete_data_open_close_q1_2024_2025": int(oc24 and oc25),
                "complete_data_all8_q1_2024_2025": int(full24 and full25),
            }
        )
    return sorted(out, key=lambda x: x["region"])


def build_dimensions(months, long_rows):
    # dim_time
    dim_time = []
    for month in months:
        dt = datetime.strptime(month + "-01", "%Y-%m-%d")
        q = (dt.month - 1) // 3 + 1
        dim_time.append(
            {
                "month": month,
                "year": dt.year,
                "month_num": dt.month,
                "month_name": dt.strftime("%B"),
                "quarter_label": f"Q{q}",
                "year_quarter": f"{dt.year}-Q{q}",
            }
        )

    # dim_region
    regions = sorted(set(r["region"] for r in long_rows))
    dim_region = []
    for region in regions:
        dim_region.append(
            {
                "region": region,
                "region_abbr": REGION_ABBR.get(region, ""),
                "region_group": REGION_GROUPS.get(region, "Other"),
                "geo_level": "National" if region == "Canada" else "Province/Territory",
            }
        )

    # dim_measure
    dim_measure = []
    for m in TARGET_MEASURES:
        dim_measure.append(
            {
                "measure": m,
                "measure_group": (
                    "Stock"
                    if m in ("Active businesses", "Continuing businesses")
                    else "Flow"
                ),
                "direction": (
                    "Inflow"
                    if m in ("Opening businesses", "Entrants", "Reopening businesses")
                    else ("Outflow" if m in ("Closing businesses", "Exits", "Temporary closures") else "Neutral")
                ),
            }
        )

    return dim_time, dim_region, dim_measure


def build_readme():
    return """# Tableau Input Package: Business Openings and Closures (Canada)

This folder contains Tableau-ready data assets focused on **analytics demonstration**:
finding patterns, testing hypotheses, and communicating insights clearly.

## Recommended Data Source in Tableau
- Primary table: `data/fact_business_dynamics_wide_monthly.csv`
- Optional long table for flexible measure switching:
  `data/fact_business_dynamics_long_monthly.csv`

## Why Two Fact Tables?
- `wide` is best for KPI tiles, ratios, decomposition, and easy calculations.
- `long` is best for parameter-driven charts where `measure` is on Color/Rows.

## Files
- `data/fact_business_dynamics_wide_monthly.csv`
- `data/fact_business_dynamics_long_monthly.csv`
- `data/region_q1_2024_vs_2025_scorecard.csv`
- `data/dim_time.csv`
- `data/dim_region.csv`
- `data/dim_measure.csv`
- `tableau_calculated_fields.md`
- `analysis_questions.md`

## Suggested Tableau Demonstration Flow
1. **National pulse**
   - Line chart: `SUM(opening_businesses)` vs `SUM(closing_businesses)` by `month`
   - KPI cards: latest month net openings, close/open ratio
2. **Regional pressure map**
   - Filled map by province on `closure_to_opening_ratio` (Q1 2025 filter)
   - Tooltip includes net change and opening/closure YoY %
3. **Decomposition of openings**
   - Stacked bars by region: `entrants` vs `reopening_businesses`
   - Insight: where growth is new creation vs returning businesses
4. **Decomposition of closures**
   - Stacked bars by region: `exits` vs `temporary_closures`
   - Insight: structural exits vs temporary stress
5. **Action matrix**
   - Scatter plot:
     - X = `openings_yoy_pct`
     - Y = `closures_yoy_pct`
     - Size = `openings_q1_2025`
     - Color = `net_change_abs`
   - Quadrants inform targeted interventions.

## Data Note
- `status='x'` means suppressed/unavailable.
- In those rows, value is null and `data_available=0`.
- For Q1 comparison, use `complete_data_open_close_q1_2024_2025` as the primary filter for this case.
- `Exits` can be unavailable in recent months (StatCan note), so `complete_data_all8_q1_2024_2025` is stricter.
"""


def build_calculated_fields_doc():
    return """# Tableau Calculated Fields (Suggested)

Use these in Tableau on `fact_business_dynamics_wide_monthly.csv`.

## Core Metrics
```tableau
Net Openings = SUM([opening_businesses]) - SUM([closing_businesses])
```

```tableau
Close/Open Ratio = SUM([closing_businesses]) / SUM([opening_businesses])
```

```tableau
Gross Churn = SUM([opening_businesses]) + SUM([closing_businesses])
```

## Composition
```tableau
Entrant Share of Openings = SUM([entrants]) / SUM([opening_businesses])
```

```tableau
Reopening Share of Openings = SUM([reopening_businesses]) / SUM([opening_businesses])
```

```tableau
Exit Share of Closings = SUM([exits]) / SUM([closing_businesses])
```

## Quality Control (good demo of analytics rigor)
```tableau
Opening Identity Gap = SUM([opening_businesses]) - (SUM([entrants]) + SUM([reopening_businesses]))
```

```tableau
Closing Identity Gap = SUM([closing_businesses]) - (SUM([exits]) + SUM([temporary_closures]))
```

These should be zero when data is complete.

## Q1 2025 vs Q1 2024 (on scorecard table)
```tableau
Net Change = SUM([net_q1_2025]) - SUM([net_q1_2024])
```

```tableau
High Pressure Flag =
IF [closure_to_opening_ratio_q1_2025] >= 1 THEN "High closure pressure" ELSE "Lower pressure" END
```
"""


def build_analysis_questions_doc():
    return """# Analytics Questions to Demonstrate Skill

Use these as a storyboard in Tableau.

1. Where did business dynamism weaken even though openings grew?
2. Which regions are driven by true new entrants vs reopenings?
3. Are closures driven more by temporary closures or permanent exits by region?
4. Which regions combine high closure pressure and high market size (priority risk)?
5. Which regions show improving net openings momentum and could absorb scale support?

## Demonstration Techniques
- YoY decomposition (`openings`, `closures`, `net`)
- Ratio analysis (`close/open`, entrant share, exit share)
- Outlier detection (rank, z-score optional)
- Segment comparisons (Atlantic/Central/Prairies/West/North)
- Data quality transparency (`data_available`, suppressed rows)
"""


def main():
    ensure_data_files()
    os.makedirs(TABLEAU_DATA_DIR, exist_ok=True)
    long_rows, months = load_long_fact()
    wide_rows = build_wide_fact(long_rows)
    scorecard_rows = build_q1_compare(wide_rows)
    dim_time, dim_region, dim_measure = build_dimensions(months, long_rows)

    write_csv(
        os.path.join(TABLEAU_DATA_DIR, "fact_business_dynamics_long_monthly.csv"),
        long_rows,
        [
            "month",
            "year",
            "month_num",
            "quarter_label",
            "year_quarter",
            "region",
            "region_abbr",
            "region_group",
            "geo_level",
            "industry",
            "measure",
            "value",
            "status",
            "data_available",
        ],
    )
    write_csv(
        os.path.join(TABLEAU_DATA_DIR, "fact_business_dynamics_wide_monthly.csv"),
        wide_rows,
        [
            "month",
            "year",
            "month_num",
            "quarter_label",
            "year_quarter",
            "region",
            "region_abbr",
            "region_group",
            "geo_level",
            "industry",
            "active_businesses",
            "opening_businesses",
            "continuing_businesses",
            "closing_businesses",
            "reopening_businesses",
            "entrants",
            "temporary_closures",
            "exits",
            "net_openings",
            "gross_churn",
            "closure_to_opening_ratio",
            "entrant_share_of_openings",
            "reopening_share_of_openings",
            "exit_share_of_closings",
            "temporary_closure_share_of_closings",
            "formula_check_opening_eq_entrants_plus_reopening",
            "formula_check_closing_eq_exits_plus_tempclosures",
            "suppressed_measure_count",
            "complete_row_all_8_measures",
        ],
    )
    write_csv(
        os.path.join(TABLEAU_DATA_DIR, "region_q1_2024_vs_2025_scorecard.csv"),
        scorecard_rows,
        [
            "region",
            "region_abbr",
            "region_group",
            "openings_q1_2024",
            "closures_q1_2024",
            "net_q1_2024",
            "entrants_q1_2024",
            "reopening_q1_2024",
            "exits_q1_2024",
            "temp_closures_q1_2024",
            "openings_q1_2025",
            "closures_q1_2025",
            "net_q1_2025",
            "entrants_q1_2025",
            "reopening_q1_2025",
            "exits_q1_2025",
            "temp_closures_q1_2025",
            "openings_yoy_pct",
            "closures_yoy_pct",
            "net_change_abs",
            "closure_to_opening_ratio_q1_2025",
            "complete_data_open_close_q1_2024_2025",
            "complete_data_all8_q1_2024_2025",
        ],
    )
    write_csv(
        os.path.join(TABLEAU_DATA_DIR, "dim_time.csv"),
        dim_time,
        ["month", "year", "month_num", "month_name", "quarter_label", "year_quarter"],
    )
    write_csv(
        os.path.join(TABLEAU_DATA_DIR, "dim_region.csv"),
        dim_region,
        ["region", "region_abbr", "region_group", "geo_level"],
    )
    write_csv(
        os.path.join(TABLEAU_DATA_DIR, "dim_measure.csv"),
        dim_measure,
        ["measure", "measure_group", "direction"],
    )

    with open(os.path.join(TABLEAU_DIR, "README.md"), "w", encoding="utf-8") as f:
        f.write(build_readme())
    with open(os.path.join(TABLEAU_DIR, "tableau_calculated_fields.md"), "w", encoding="utf-8") as f:
        f.write(build_calculated_fields_doc())
    with open(os.path.join(TABLEAU_DIR, "analysis_questions.md"), "w", encoding="utf-8") as f:
        f.write(build_analysis_questions_doc())

    print("Built Tableau package:")
    print(TABLEAU_DIR)
    print("Rows:")
    print(f"  long fact: {len(long_rows)}")
    print(f"  wide fact: {len(wide_rows)}")
    print(f"  scorecard: {len(scorecard_rows)}")


if __name__ == "__main__":
    main()
