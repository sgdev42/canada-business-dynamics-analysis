#!/usr/bin/env python3
import csv
import io
import os
import re
import sqlite3
import zipfile
from collections import defaultdict
from datetime import date
from urllib.request import urlopen

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")
PRESENTATION_DIR = os.path.join(BASE_DIR, "presentation")

ZIP_URL = "https://www150.statcan.gc.ca/n1/tbl/csv/33100270-eng.zip"
ZIP_PATH = os.path.join(BASE_DIR, "data_33100270_eng.zip")
CSV_PATH = os.path.join(DATA_DIR, "33100270.csv")
META_PATH = os.path.join(DATA_DIR, "33100270_MetaData.csv")

MONTHS_2024 = ["2024-01", "2024-02", "2024-03"]
MONTHS_2025 = ["2025-01", "2025-02", "2025-03"]
MONTHS_ALL = MONTHS_2024 + MONTHS_2025
TARGET_MEASURES = {"Opening businesses", "Closing businesses"}
TARGET_INDUSTRY = "Business sector industries [T004]"


def ensure_data_files():
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(CSV_PATH) and os.path.exists(META_PATH):
        return
    if not os.path.exists(ZIP_PATH):
        with urlopen(ZIP_URL, timeout=60) as response:
            payload = response.read()
        with open(ZIP_PATH, "wb") as f:
            f.write(payload)
    with zipfile.ZipFile(ZIP_PATH, "r") as zf:
        zf.extractall(DATA_DIR)


def load_regions_from_metadata():
    regions = []
    with open(META_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        in_member_section = False
        for row in reader:
            if row[:2] == ["Dimension ID", "Member Name"]:
                in_member_section = True
                continue
            if not in_member_section or len(row) < 5:
                continue
            if row[0] != "1":
                continue
            member_name = row[1].replace("\xa0", "").strip()
            parent_member_id = (row[4] or "").strip()
            class_code = (row[2] or "").strip()
            if (
                parent_member_id == "1"
                and member_name != "Canada"
                and re.match(r"^\[\d{2}\]$", class_code)
            ):
                regions.append(member_name)
    return sorted(set(regions))


def parse_main_dataset(regions):
    records = {}
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            month = row["REF_DATE"]
            if month not in MONTHS_ALL:
                continue
            region = row["GEO"].replace("\xa0", "").strip()
            if region not in regions:
                continue
            if row["Industry"] != TARGET_INDUSTRY:
                continue
            measure = row["Business dynamics measure"]
            if measure not in TARGET_MEASURES:
                continue

            key = (region, month)
            if key not in records:
                records[key] = {
                    "region": region,
                    "month": month,
                    "opening": None,
                    "closing": None,
                    "status_opening": "",
                    "status_closing": "",
                }

            value_text = (row["VALUE"] or "").strip()
            value = float(value_text) if value_text else None
            status = (row["STATUS"] or "").strip()
            if measure == "Opening businesses":
                records[key]["opening"] = value
                records[key]["status_opening"] = status
            elif measure == "Closing businesses":
                records[key]["closing"] = value
                records[key]["status_closing"] = status

    monthly_rows = []
    for region in regions:
        for month in MONTHS_ALL:
            rec = records.get(
                (region, month),
                {
                    "region": region,
                    "month": month,
                    "opening": None,
                    "closing": None,
                    "status_opening": "",
                    "status_closing": "",
                },
            )
            opening = rec["opening"]
            closing = rec["closing"]
            net = (opening - closing) if (opening is not None and closing is not None) else None
            ratio = (closing / opening) if (opening not in (None, 0) and closing is not None) else None
            monthly_rows.append(
                {
                    "region": region,
                    "month": month,
                    "year": int(month[:4]),
                    "opening": opening,
                    "closing": closing,
                    "net": net,
                    "closure_to_opening_ratio": ratio,
                    "status_opening": rec["status_opening"],
                    "status_closing": rec["status_closing"],
                    "data_available": int(opening is not None and closing is not None),
                }
            )
    return monthly_rows


def build_summary(monthly_rows):
    by_region = defaultdict(list)
    for row in monthly_rows:
        by_region[row["region"]].append(row)

    summary_rows = []
    for region, rows in sorted(by_region.items()):
        rows_2024 = [r for r in rows if r["month"] in MONTHS_2024]
        rows_2025 = [r for r in rows if r["month"] in MONTHS_2025]

        available_2024 = all(r["data_available"] for r in rows_2024)
        available_2025 = all(r["data_available"] for r in rows_2025)

        openings_2024 = sum(r["opening"] for r in rows_2024 if r["opening"] is not None)
        closures_2024 = sum(r["closing"] for r in rows_2024 if r["closing"] is not None)
        openings_2025 = sum(r["opening"] for r in rows_2025 if r["opening"] is not None)
        closures_2025 = sum(r["closing"] for r in rows_2025 if r["closing"] is not None)

        net_2024 = openings_2024 - closures_2024 if available_2024 else None
        net_2025 = openings_2025 - closures_2025 if available_2025 else None

        open_yoy = (
            ((openings_2025 - openings_2024) / openings_2024) * 100
            if openings_2024 not in (0, None)
            else None
        )
        close_yoy = (
            ((closures_2025 - closures_2024) / closures_2024) * 100
            if closures_2024 not in (0, None)
            else None
        )
        net_change = (net_2025 - net_2024) if (net_2024 is not None and net_2025 is not None) else None

        ratio_2024 = (closures_2024 / openings_2024) if openings_2024 else None
        ratio_2025 = (closures_2025 / openings_2025) if openings_2025 else None

        data_note = ""
        if not available_2024 or not available_2025:
            data_note = "Incomplete data in at least one month (e.g., STATUS='x')."

        summary_rows.append(
            {
                "region": region,
                "openings_2024_q1": openings_2024 if available_2024 else None,
                "closures_2024_q1": closures_2024 if available_2024 else None,
                "net_2024_q1": net_2024,
                "openings_2025_q1": openings_2025 if available_2025 else None,
                "closures_2025_q1": closures_2025 if available_2025 else None,
                "net_2025_q1": net_2025,
                "openings_yoy_pct": open_yoy if available_2024 and available_2025 else None,
                "closures_yoy_pct": close_yoy if available_2024 and available_2025 else None,
                "net_change_abs": net_change,
                "closure_to_opening_2024_q1": ratio_2024 if available_2024 else None,
                "closure_to_opening_2025_q1": ratio_2025 if available_2025 else None,
                "complete_data_q1_2024_2025": int(available_2024 and available_2025),
                "data_note": data_note,
            }
        )
    return summary_rows


def build_national_monthly(monthly_rows):
    monthly = defaultdict(lambda: {"opening": 0.0, "closing": 0.0, "regions_used": 0, "regions_missing": 0})
    regions = sorted(set(r["region"] for r in monthly_rows))
    for month in MONTHS_ALL:
        for region in regions:
            row = next(r for r in monthly_rows if r["month"] == month and r["region"] == region)
            if row["data_available"]:
                monthly[month]["opening"] += row["opening"]
                monthly[month]["closing"] += row["closing"]
                monthly[month]["regions_used"] += 1
            else:
                monthly[month]["regions_missing"] += 1

    result = []
    for month in MONTHS_ALL:
        o = monthly[month]["opening"]
        c = monthly[month]["closing"]
        result.append(
            {
                "month": month,
                "year": int(month[:4]),
                "openings": o,
                "closures": c,
                "net": o - c,
                "regions_used": monthly[month]["regions_used"],
                "regions_missing": monthly[month]["regions_missing"],
            }
        )
    return result


def write_csv(path, rows, columns):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in columns})


def create_sqlite(monthly_rows, summary_rows, national_rows):
    db_path = os.path.join(OUTPUTS_DIR, "business_dynamics_q1.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE regional_monthly (
            region TEXT,
            month TEXT,
            year INTEGER,
            opening REAL,
            closing REAL,
            net REAL,
            closure_to_opening_ratio REAL,
            status_opening TEXT,
            status_closing TEXT,
            data_available INTEGER
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE regional_summary_q1 (
            region TEXT,
            openings_2024_q1 REAL,
            closures_2024_q1 REAL,
            net_2024_q1 REAL,
            openings_2025_q1 REAL,
            closures_2025_q1 REAL,
            net_2025_q1 REAL,
            openings_yoy_pct REAL,
            closures_yoy_pct REAL,
            net_change_abs REAL,
            closure_to_opening_2024_q1 REAL,
            closure_to_opening_2025_q1 REAL,
            complete_data_q1_2024_2025 INTEGER,
            data_note TEXT
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE national_monthly (
            month TEXT,
            year INTEGER,
            openings REAL,
            closures REAL,
            net REAL,
            regions_used INTEGER,
            regions_missing INTEGER
        );
        """
    )
    cur.executemany(
        """
        INSERT INTO regional_monthly
        (region, month, year, opening, closing, net, closure_to_opening_ratio,
         status_opening, status_closing, data_available)
        VALUES (:region, :month, :year, :opening, :closing, :net, :closure_to_opening_ratio,
                :status_opening, :status_closing, :data_available);
        """,
        monthly_rows,
    )
    cur.executemany(
        """
        INSERT INTO regional_summary_q1
        (region, openings_2024_q1, closures_2024_q1, net_2024_q1, openings_2025_q1, closures_2025_q1,
         net_2025_q1, openings_yoy_pct, closures_yoy_pct, net_change_abs, closure_to_opening_2024_q1,
         closure_to_opening_2025_q1, complete_data_q1_2024_2025, data_note)
        VALUES (:region, :openings_2024_q1, :closures_2024_q1, :net_2024_q1, :openings_2025_q1, :closures_2025_q1,
                :net_2025_q1, :openings_yoy_pct, :closures_yoy_pct, :net_change_abs, :closure_to_opening_2024_q1,
                :closure_to_opening_2025_q1, :complete_data_q1_2024_2025, :data_note);
        """,
        summary_rows,
    )
    cur.executemany(
        """
        INSERT INTO national_monthly
        (month, year, openings, closures, net, regions_used, regions_missing)
        VALUES (:month, :year, :openings, :closures, :net, :regions_used, :regions_missing);
        """,
        national_rows,
    )
    conn.commit()
    conn.close()


def fmt_num(value, decimals=0):
    if value is None:
        return "n/a"
    return f"{value:,.{decimals}f}"


def svg_line_chart(national_rows):
    width = 980
    height = 320
    margin_left = 70
    margin_right = 30
    margin_top = 35
    margin_bottom = 55
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    values = [r["openings"] for r in national_rows] + [r["closures"] for r in national_rows]
    y_min = min(values)
    y_max = max(values)
    y_pad = (y_max - y_min) * 0.1
    y_min -= y_pad
    y_max += y_pad

    def x_pos(i):
        return margin_left + (plot_w * i / (len(national_rows) - 1))

    def y_pos(v):
        return margin_top + (plot_h * (1 - (v - y_min) / (y_max - y_min)))

    def points(series_key):
        return " ".join(f"{x_pos(i):.2f},{y_pos(r[series_key]):.2f}" for i, r in enumerate(national_rows))

    x_labels = "".join(
        f'<text x="{x_pos(i):.2f}" y="{height-20}" text-anchor="middle" class="axis">{r["month"]}</text>'
        for i, r in enumerate(national_rows)
    )

    y_ticks = []
    for t in range(5):
        v = y_min + (y_max - y_min) * t / 4
        y = y_pos(v)
        y_ticks.append(
            f'<line x1="{margin_left}" y1="{y:.2f}" x2="{width-margin_right}" y2="{y:.2f}" class="grid"/>'
            f'<text x="{margin_left-8}" y="{y+4:.2f}" text-anchor="end" class="axis">{int(v):,}</text>'
        )

    return f"""
<svg viewBox="0 0 {width} {height}" role="img" aria-label="Monthly openings and closures line chart">
  <style>
    .axis {{ font: 11px sans-serif; fill: #34495e; }}
    .grid {{ stroke: #e6edf3; stroke-width: 1; }}
    .line-open {{ fill: none; stroke: #1f77b4; stroke-width: 3; }}
    .line-close {{ fill: none; stroke: #d62728; stroke-width: 3; }}
    .legend {{ font: 12px sans-serif; fill: #2c3e50; }}
  </style>
  {''.join(y_ticks)}
  <polyline points="{points('openings')}" class="line-open"/>
  <polyline points="{points('closures')}" class="line-close"/>
  <text x="{margin_left}" y="18" class="legend">Openings (blue) vs Closures (red), provinces/territories with available data</text>
  {x_labels}
</svg>
"""


def svg_net_change_bars(summary_rows):
    rows = [r for r in summary_rows if r["complete_data_q1_2024_2025"] == 1]
    rows.sort(key=lambda r: r["net_change_abs"], reverse=True)

    width = 980
    bar_h = 24
    gap = 9
    margin_left = 250
    margin_right = 40
    margin_top = 25
    margin_bottom = 25
    height = margin_top + margin_bottom + len(rows) * (bar_h + gap)
    plot_w = width - margin_left - margin_right
    max_abs = max(abs(r["net_change_abs"]) for r in rows)

    def x_scale(v):
        return (v / max_abs) * (plot_w / 2)

    zero_x = margin_left + plot_w / 2
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="YoY net change by region">',
        """
  <style>
    .label { font: 12px sans-serif; fill: #2c3e50; }
    .axis { stroke: #aeb8c2; stroke-width: 1; }
    .pos { fill: #1f8a70; }
    .neg { fill: #c0392b; }
    .val { font: 11px sans-serif; fill: #2c3e50; }
  </style>
""",
        f'<line x1="{zero_x}" y1="{margin_top-10}" x2="{zero_x}" y2="{height-margin_bottom+5}" class="axis"/>',
    ]
    for i, r in enumerate(rows):
        y = margin_top + i * (bar_h + gap)
        v = r["net_change_abs"]
        w = abs(x_scale(v))
        if v >= 0:
            x = zero_x
            cls = "pos"
            tx = x + w + 6
            anchor = "start"
        else:
            x = zero_x - w
            cls = "neg"
            tx = x - 6
            anchor = "end"
        parts.append(f'<text x="{margin_left-10}" y="{y+bar_h*0.72:.2f}" text-anchor="end" class="label">{r["region"]}</text>')
        parts.append(f'<rect x="{x:.2f}" y="{y:.2f}" width="{w:.2f}" height="{bar_h}" class="{cls}"/>')
        parts.append(f'<text x="{tx:.2f}" y="{y+bar_h*0.72:.2f}" text-anchor="{anchor}" class="val">{int(v):,}</text>')
    parts.append("</svg>")
    return "".join(parts)


def top_recommendations(summary_rows):
    clean = [r for r in summary_rows if r["complete_data_q1_2024_2025"] == 1]
    best_net = sorted(clean, key=lambda r: r["net_2025_q1"], reverse=True)[:3]
    most_pressure = sorted(clean, key=lambda r: r["closure_to_opening_2025_q1"], reverse=True)[:4]
    improving = sorted(clean, key=lambda r: r["net_change_abs"], reverse=True)[:3]

    return best_net, most_pressure, improving


def build_html_presentation(summary_rows, national_rows):
    best_net, most_pressure, improving = top_recommendations(summary_rows)
    summary_sorted = sorted(
        [r for r in summary_rows if r["complete_data_q1_2024_2025"] == 1],
        key=lambda r: r["net_2025_q1"],
        reverse=True,
    )
    suppressed = [r for r in summary_rows if r["complete_data_q1_2024_2025"] == 0]

    total_open_2024 = sum(r["openings_2024_q1"] for r in summary_sorted)
    total_close_2024 = sum(r["closures_2024_q1"] for r in summary_sorted)
    total_open_2025 = sum(r["openings_2025_q1"] for r in summary_sorted)
    total_close_2025 = sum(r["closures_2025_q1"] for r in summary_sorted)
    total_net_2024 = total_open_2024 - total_close_2024
    total_net_2025 = total_open_2025 - total_close_2025

    html_table_rows = []
    for r in summary_sorted:
        html_table_rows.append(
            "<tr>"
            f"<td>{r['region']}</td>"
            f"<td>{fmt_num(r['openings_2025_q1'])}</td>"
            f"<td>{fmt_num(r['closures_2025_q1'])}</td>"
            f"<td>{fmt_num(r['net_2025_q1'])}</td>"
            f"<td>{fmt_num(r['openings_yoy_pct'], 2)}%</td>"
            f"<td>{fmt_num(r['closures_yoy_pct'], 2)}%</td>"
            f"<td>{fmt_num(r['net_change_abs'])}</td>"
            f"<td>{fmt_num(r['closure_to_opening_2025_q1'], 3)}</td>"
            "</tr>"
        )

    suppressed_note = ", ".join(r["region"] for r in suppressed) if suppressed else "None"

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Regional Business Openings & Closures (Q1 2024 vs Q1 2025)</title>
  <style>
    :root {{
      --bg: #f6f8fb;
      --panel: #ffffff;
      --ink: #1f2d3d;
      --muted: #5b6b7a;
      --accent: #005f73;
      --good: #1f8a70;
      --risk: #b23a48;
      --line: #dce3ea;
    }}
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: var(--bg); color: var(--ink); }}
    .wrap {{ max-width: 1100px; margin: 0 auto; padding: 28px 20px 50px; }}
    h1, h2 {{ margin: 0 0 10px; }}
    .sub {{ color: var(--muted); margin-bottom: 22px; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 18px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 12px; }}
    .card .k {{ font-size: 24px; font-weight: 700; color: var(--accent); }}
    .card .t {{ font-size: 13px; color: var(--muted); }}
    .panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; margin-top: 14px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 7px 6px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ color: var(--muted); position: sticky; top: 0; background: var(--panel); }}
    ul {{ margin: 8px 0 0; }}
    li {{ margin: 6px 0; }}
    .fine {{ font-size: 12px; color: var(--muted); }}
    @media (max-width: 900px) {{
      .grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Business Openings vs Closures Across Canada by Region</h1>
    <div class="sub">Source: Statistics Canada table 33-10-0270-01 | Timeframe: Jan-Mar 2024 vs Jan-Mar 2025 | Built: {date.today().isoformat()}</div>

    <div class="grid">
      <div class="card"><div class="k">{fmt_num(total_open_2025)}</div><div class="t">Openings (Q1 2025)</div></div>
      <div class="card"><div class="k">{fmt_num(total_close_2025)}</div><div class="t">Closures (Q1 2025)</div></div>
      <div class="card"><div class="k">{fmt_num(total_net_2025)}</div><div class="t">Net Openings (Q1 2025)</div></div>
      <div class="card"><div class="k">{fmt_num(total_net_2025-total_net_2024)}</div><div class="t">Net Change vs Q1 2024</div></div>
    </div>

    <div class="panel">
      <h2>National Trend (Monthly)</h2>
      {svg_line_chart(national_rows)}
      <div class="fine">March 2025 net openings turned negative across regions with available data (openings 44,885 vs closures 45,083).</div>
    </div>

    <div class="panel">
      <h2>Regional Shift in Net Openings (Q1 2025 vs Q1 2024)</h2>
      {svg_net_change_bars(summary_rows)}
      <div class="fine">Positive bars indicate improved net openings year-over-year; negative bars indicate deterioration.</div>
    </div>

    <div class="panel">
      <h2>Region Comparison Table (Q1)</h2>
      <table>
        <thead>
          <tr>
            <th>Region</th>
            <th>Open 2025</th>
            <th>Close 2025</th>
            <th>Net 2025</th>
            <th>Open YoY %</th>
            <th>Close YoY %</th>
            <th>Net Delta</th>
            <th>Close/Open 2025</th>
          </tr>
        </thead>
        <tbody>
          {''.join(html_table_rows)}
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h2>Key Insights</h2>
      <ul>
        <li>Net business creation dropped from {fmt_num(total_net_2024)} in Q1 2024 to {fmt_num(total_net_2025)} in Q1 2025 across regions with complete data, despite openings increasing slightly.</li>
        <li>Top net-opening regions in Q1 2025 were {best_net[0]['region']} ({fmt_num(best_net[0]['net_2025_q1'])}), {best_net[1]['region']} ({fmt_num(best_net[1]['net_2025_q1'])}), and {best_net[2]['region']} ({fmt_num(best_net[2]['net_2025_q1'])}).</li>
        <li>Highest closure pressure (close/open ratio) appeared in {most_pressure[0]['region']} ({fmt_num(most_pressure[0]['closure_to_opening_2025_q1'], 3)}), {most_pressure[1]['region']} ({fmt_num(most_pressure[1]['closure_to_opening_2025_q1'], 3)}), and {most_pressure[2]['region']} ({fmt_num(most_pressure[2]['closure_to_opening_2025_q1'], 3)}).</li>
        <li>Most improved net performance came from {improving[0]['region']} ({fmt_num(improving[0]['net_change_abs'])}), followed by {improving[1]['region']} ({fmt_num(improving[1]['net_change_abs'])}) and {improving[2]['region']} ({fmt_num(improving[2]['net_change_abs'])}).</li>
        <li>Largest decline in absolute net openings was concentrated in Ontario, which is material because of its overall business base size.</li>
      </ul>
    </div>

    <div class="panel">
      <h2>Recommendations</h2>
      <ul>
        <li>Prioritize retention-focused support in high-closure-pressure regions: cash-flow coaching in first 12 months, milestone-based mentoring, and early-warning triggers for liquidity stress.</li>
        <li>Scale growth programs in high-net regions (especially Western provinces) using sector-specific accelerator cohorts and faster micro-financing approvals.</li>
        <li>Create an Ontario stabilization initiative combining market validation sprints, procurement-readiness training, and post-launch advisor check-ins.</li>
        <li>Deploy a monthly regional risk dashboard in Power BI/Tableau using the output files in <code>outputs/</code> to identify where closure growth outpaces opening growth.</li>
      </ul>
      <div class="fine">Data quality note: suppressed/unavailable records detected for {suppressed_note}; excluded from aggregate comparisons.</div>
    </div>
  </div>
</body>
</html>
"""
    return html


def main():
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    os.makedirs(PRESENTATION_DIR, exist_ok=True)
    ensure_data_files()

    regions = load_regions_from_metadata()
    monthly_rows = parse_main_dataset(regions)
    summary_rows = build_summary(monthly_rows)
    national_rows = build_national_monthly(monthly_rows)

    write_csv(
        os.path.join(OUTPUTS_DIR, "regional_monthly_open_close_q1_2024_2025.csv"),
        monthly_rows,
        [
            "region",
            "month",
            "year",
            "opening",
            "closing",
            "net",
            "closure_to_opening_ratio",
            "status_opening",
            "status_closing",
            "data_available",
        ],
    )
    write_csv(
        os.path.join(OUTPUTS_DIR, "regional_summary_q1_2024_vs_2025.csv"),
        summary_rows,
        [
            "region",
            "openings_2024_q1",
            "closures_2024_q1",
            "net_2024_q1",
            "openings_2025_q1",
            "closures_2025_q1",
            "net_2025_q1",
            "openings_yoy_pct",
            "closures_yoy_pct",
            "net_change_abs",
            "closure_to_opening_2024_q1",
            "closure_to_opening_2025_q1",
            "complete_data_q1_2024_2025",
            "data_note",
        ],
    )
    write_csv(
        os.path.join(OUTPUTS_DIR, "national_monthly_q1_2024_2025.csv"),
        national_rows,
        ["month", "year", "openings", "closures", "net", "regions_used", "regions_missing"],
    )
    create_sqlite(monthly_rows, summary_rows, national_rows)

    html = build_html_presentation(summary_rows, national_rows)
    html_path = os.path.join(
        PRESENTATION_DIR, "regional_business_dynamics_q1_2024_vs_2025.html"
    )
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    print("Analysis complete.")
    print(f"Presentation: {html_path}")
    print(f"Outputs: {OUTPUTS_DIR}")


if __name__ == "__main__":
    main()
