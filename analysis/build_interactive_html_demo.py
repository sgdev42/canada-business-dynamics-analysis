#!/usr/bin/env python3
import csv
import json
import os
import subprocess
import sys
from datetime import datetime
from urllib.request import urlopen

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TABLEAU_DATA = os.path.join(BASE_DIR, "tableau_example_project", "data")
DEMO_DIR = os.path.join(BASE_DIR, "tableau_example_project")
ASSETS_DIR = os.path.join(DEMO_DIR, "assets")
OUT_HTML = os.path.join(DEMO_DIR, "interactive_demo_offline.html")
PLOTLY_LOCAL = os.path.join(ASSETS_DIR, "plotly-2.35.2.min.js")
PLOTLY_URL = "https://cdn.plot.ly/plotly-2.35.2.min.js"


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def to_num(v):
    if v in (None, "", "null"):
        return None
    try:
        return float(v)
    except Exception:
        return None


def ensure_demo_inputs():
    required = [
        os.path.join(TABLEAU_DATA, "fact_business_dynamics_wide_monthly.csv"),
        os.path.join(TABLEAU_DATA, "region_q1_2024_vs_2025_scorecard.csv"),
    ]
    missing = [p for p in required if not os.path.exists(p)]
    if not missing:
        return
    build_script = os.path.join(BASE_DIR, "analysis", "build_tableau_example_project.py")
    subprocess.run([sys.executable, build_script], check=True, cwd=BASE_DIR)


def prepare_data():
    wide = read_csv(os.path.join(TABLEAU_DATA, "fact_business_dynamics_wide_monthly.csv"))
    score = read_csv(os.path.join(TABLEAU_DATA, "region_q1_2024_vs_2025_scorecard.csv"))

    target_months = {"2024-01", "2024-02", "2024-03", "2025-01", "2025-02", "2025-03"}
    national = []
    regional_monthly = []
    for r in wide:
        if r["month"] not in target_months:
            continue
        row = {
            "month": r["month"],
            "year": int(r["year"]),
            "region": r["region"],
            "region_group": r["region_group"],
            "geo_level": r["geo_level"],
            "opening_businesses": to_num(r["opening_businesses"]),
            "closing_businesses": to_num(r["closing_businesses"]),
            "net_openings": to_num(r["net_openings"]),
            "entrants": to_num(r["entrants"]),
            "reopening_businesses": to_num(r["reopening_businesses"]),
            "exits": to_num(r["exits"]),
            "temporary_closures": to_num(r["temporary_closures"]),
            "complete_row_all_8_measures": int(r["complete_row_all_8_measures"]),
        }
        if row["geo_level"] == "National":
            national.append(row)
        else:
            regional_monthly.append(row)

    score_rows = []
    for r in score:
        score_rows.append(
            {
                "region": r["region"],
                "region_abbr": r["region_abbr"],
                "region_group": r["region_group"],
                "openings_q1_2024": to_num(r["openings_q1_2024"]),
                "closures_q1_2024": to_num(r["closures_q1_2024"]),
                "net_q1_2024": to_num(r["net_q1_2024"]),
                "openings_q1_2025": to_num(r["openings_q1_2025"]),
                "closures_q1_2025": to_num(r["closures_q1_2025"]),
                "net_q1_2025": to_num(r["net_q1_2025"]),
                "entrants_q1_2025": to_num(r["entrants_q1_2025"]),
                "reopening_q1_2025": to_num(r["reopening_q1_2025"]),
                "exits_q1_2025": to_num(r["exits_q1_2025"]),
                "temp_closures_q1_2025": to_num(r["temp_closures_q1_2025"]),
                "openings_yoy_pct": to_num(r["openings_yoy_pct"]),
                "closures_yoy_pct": to_num(r["closures_yoy_pct"]),
                "net_change_abs": to_num(r["net_change_abs"]),
                "closure_to_opening_ratio_q1_2025": to_num(r["closure_to_opening_ratio_q1_2025"]),
                "complete_data_open_close_q1_2024_2025": int(r["complete_data_open_close_q1_2024_2025"]),
                "complete_data_all8_q1_2024_2025": int(r["complete_data_all8_q1_2024_2025"]),
            }
        )

    national.sort(key=lambda x: x["month"])
    regional_monthly.sort(key=lambda x: (x["region"], x["month"]))

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "national_monthly": national,
        "regional_monthly": regional_monthly,
        "regional_q1_scorecard": score_rows,
    }


def build_html(data):
    payload = json.dumps(data, separators=(",", ":"))
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Interactive Business Dynamics Demo (Q1 Focus)</title>
  <script src="./assets/plotly-2.35.2.min.js"></script>
  <style>
    :root {{
      --bg: #f4f7fb;
      --card: #ffffff;
      --line: #d8e0ea;
      --text: #1d2a35;
      --muted: #5b6b7a;
      --blue: #1f77b4;
      --orange: #ff7f0e;
      --green: #1f8a70;
      --red: #c0392b;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; background: var(--bg); color: var(--text); }}
    .wrap {{ max-width: 1240px; margin: 0 auto; padding: 18px; }}
    h1 {{ margin: 0 0 6px; font-size: 24px; }}
    .sub {{ color: var(--muted); margin-bottom: 14px; font-size: 13px; }}
    .controls {{
      display: grid; grid-template-columns: repeat(4, minmax(150px, 1fr)); gap: 10px;
      background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 10px;
    }}
    .controls label {{ display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }}
    .controls select {{ width: 100%; padding: 7px; border: 1px solid var(--line); border-radius: 8px; background: #fff; }}
    .kpis {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-top: 10px; }}
    .kpi {{ background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 10px; }}
    .kpi .val {{ font-size: 24px; font-weight: 700; }}
    .kpi .lbl {{ font-size: 12px; color: var(--muted); margin-top: 3px; }}
    .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 12px; }}
    .panel {{ background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 8px 8px 0; }}
    .panel h3 {{ margin: 4px 8px 0; font-size: 14px; }}
    .plot {{ height: 360px; }}
    .insights {{ margin-top: 12px; background: var(--card); border: 1px solid var(--line); border-radius: 10px; padding: 10px; }}
    .insights h3 {{ margin: 2px 0 8px; font-size: 14px; }}
    .insights ul {{ margin: 0; padding-left: 18px; }}
    .insights li {{ margin: 6px 0; font-size: 13px; }}
    .foot {{ margin-top: 12px; font-size: 12px; color: var(--muted); }}
    @media (max-width: 980px) {{
      .controls {{ grid-template-columns: 1fr 1fr; }}
      .kpis {{ grid-template-columns: 1fr 1fr; }}
      .grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Interactive Analytics Demo (Offline, Q1 Focus)</h1>
    <div class="sub">Scope locked to Jan-Mar 2024 and Jan-Mar 2025 | Built from Statistics Canada table 33-10-0270-01 | Generated: {data["generated_at"]}</div>

    <div class="controls">
      <div>
        <label>Regional Group</label>
        <select id="groupFilter">
          <option value="All">All</option>
          <option>Atlantic</option>
          <option>Central</option>
          <option>Prairies</option>
          <option>West</option>
          <option>North</option>
        </select>
      </div>
      <div>
        <label>Sort Regions By</label>
        <select id="sortBy">
          <option value="net_q1_2025">Net Q1 2025</option>
          <option value="net_change_abs">Net Change vs Q1 2024</option>
          <option value="closure_to_opening_ratio_q1_2025">Closure/Open Ratio</option>
          <option value="openings_q1_2025">Openings Q1 2025</option>
        </select>
      </div>
      <div>
        <label>Focus Region</label>
        <select id="focusRegion">
          <option value="All">All Regions</option>
        </select>
      </div>
      <div>
        <label>View Scope</label>
        <select id="scope">
          <option value="openclose">Open/Close Complete Regions</option>
          <option value="all8">All 8 Measures Complete Regions</option>
        </select>
      </div>
    </div>

    <div class="kpis">
      <div class="kpi"><div class="val" id="kpiOpen">-</div><div class="lbl">Q1 2025 Openings</div></div>
      <div class="kpi"><div class="val" id="kpiClose">-</div><div class="lbl">Q1 2025 Closures</div></div>
      <div class="kpi"><div class="val" id="kpiNet">-</div><div class="lbl">Q1 2025 Net Openings</div></div>
      <div class="kpi"><div class="val" id="kpiDelta">-</div><div class="lbl">Net Change vs Q1 2024</div></div>
    </div>

    <div class="grid">
      <div class="panel">
        <h3>National Q1 Monthly Trend: Openings vs Closures vs Net</h3>
        <div id="trend" class="plot"></div>
      </div>
      <div class="panel">
        <h3>Regional Net Openings (Q1 2025)</h3>
        <div id="regionalBar" class="plot"></div>
      </div>
      <div class="panel">
        <h3>Action Matrix: Opening YoY% vs Closing YoY%</h3>
        <div id="matrix" class="plot"></div>
      </div>
      <div class="panel">
        <h3>Composition: Entrants/Reopenings and Exits/Temporary Closures</h3>
        <div id="mix" class="plot"></div>
      </div>
    </div>

    <div class="insights">
      <h3>Auto Insights</h3>
      <ul id="insightsList"></ul>
    </div>

    <div class="foot">
      Offline version: includes local Plotly asset for easy sharing. Click bars or bubbles to focus a region. Nunavut may be excluded due to suppressed values.
    </div>
  </div>

  <script>
    const data = {payload};
    const fmtInt = (v) => new Intl.NumberFormat('en-CA', {{maximumFractionDigits:0}}).format(v);
    const fmtPct = (v, d=1) => (v == null ? "n/a" : `${{v.toFixed(d)}}%`);
    const fmtRat = (v) => (v == null ? "n/a" : v.toFixed(3));

    const els = {{
      groupFilter: document.getElementById('groupFilter'),
      sortBy: document.getElementById('sortBy'),
      focusRegion: document.getElementById('focusRegion'),
      scope: document.getElementById('scope'),
      kpiOpen: document.getElementById('kpiOpen'),
      kpiClose: document.getElementById('kpiClose'),
      kpiNet: document.getElementById('kpiNet'),
      kpiDelta: document.getElementById('kpiDelta'),
      insightsList: document.getElementById('insightsList'),
    }};

    const regions = [...new Set(data.regional_q1_scorecard.map(d => d.region))].sort();
    regions.forEach(r => {{
      const opt = document.createElement('option');
      opt.value = r; opt.textContent = r;
      els.focusRegion.appendChild(opt);
    }});

    function getFilteredScorecard() {{
      const group = els.groupFilter.value;
      const scope = els.scope.value;
      return data.regional_q1_scorecard.filter(d => {{
        if (group !== 'All' && d.region_group !== group) return false;
        if (scope === 'openclose' && d.complete_data_open_close_q1_2024_2025 !== 1) return false;
        if (scope === 'all8' && d.complete_data_all8_q1_2024_2025 !== 1) return false;
        return true;
      }});
    }}

    function selectedRegionOrNull() {{
      const r = els.focusRegion.value;
      return r === 'All' ? null : r;
    }}

    function renderKPIs(rows) {{
      const o = rows.reduce((a,b) => a + (b.openings_q1_2025 || 0), 0);
      const c = rows.reduce((a,b) => a + (b.closures_q1_2025 || 0), 0);
      const n = rows.reduce((a,b) => a + (b.net_q1_2025 || 0), 0);
      const d = rows.reduce((a,b) => a + (b.net_change_abs || 0), 0);
      els.kpiOpen.textContent = fmtInt(o);
      els.kpiClose.textContent = fmtInt(c);
      els.kpiNet.textContent = fmtInt(n);
      els.kpiNet.style.color = n >= 0 ? 'var(--green)' : 'var(--red)';
      els.kpiDelta.textContent = `${{d >= 0 ? '+' : ''}}${{fmtInt(d)}}`;
      els.kpiDelta.style.color = d >= 0 ? 'var(--green)' : 'var(--red)';
    }}

    function renderNationalTrend() {{
      const n = data.national_monthly;
      Plotly.newPlot('trend', [
        {{
          x: n.map(d => d.month),
          y: n.map(d => d.opening_businesses),
          name: 'Openings',
          mode: 'lines+markers',
          line: {{color: '#1f77b4', width: 3}},
          marker: {{size: 6}}
        }},
        {{
          x: n.map(d => d.month),
          y: n.map(d => d.closing_businesses),
          name: 'Closures',
          mode: 'lines+markers',
          line: {{color: '#ff7f0e', width: 3}},
          marker: {{size: 6}}
        }},
        {{
          x: n.map(d => d.month),
          y: n.map(d => d.net_openings),
          name: 'Net',
          mode: 'lines+markers',
          line: {{color: '#1f8a70', width: 3, dash: 'dot'}},
          marker: {{size: 6}},
          yaxis: 'y2'
        }}
      ], {{
        margin: {{l: 55, r: 55, t: 20, b: 50}},
        hovermode: 'x unified',
        xaxis: {{tickangle: -45}},
        yaxis: {{title: 'Openings / Closures'}},
        yaxis2: {{title: 'Net', overlaying: 'y', side: 'right', showgrid: false}},
        legend: {{orientation: 'h', y: 1.13}}
      }}, {{displayModeBar: false, responsive: true}});
    }}

    function renderRegionalBar(rows) {{
      const sortBy = els.sortBy.value;
      const regionFocus = selectedRegionOrNull();
      const sorted = [...rows].sort((a,b) => (b[sortBy] || -Infinity) - (a[sortBy] || -Infinity));
      const filtered = regionFocus ? sorted.filter(r => r.region === regionFocus) : sorted;
      const colors = filtered.map(d => (d.net_q1_2025 || 0) >= 0 ? '#1f8a70' : '#c0392b');

      Plotly.newPlot('regionalBar', [{{
        type: 'bar',
        x: filtered.map(d => d.net_q1_2025),
        y: filtered.map(d => d.region),
        orientation: 'h',
        marker: {{color: colors}},
        customdata: filtered,
        hovertemplate:
          '<b>%{{y}}</b><br>' +
          'Net Q1 2025: %{{x:,}}<br>' +
          'Openings YoY: %{{customdata.openings_yoy_pct:.2f}}%<br>' +
          'Closures YoY: %{{customdata.closures_yoy_pct:.2f}}%<extra></extra>'
      }}], {{
        margin: {{l: 160, r: 20, t: 20, b: 35}},
        xaxis: {{title: 'Net Openings (Q1 2025)', zeroline: true, zerolinecolor: '#999'}},
        yaxis: {{autorange: 'reversed'}}
      }}, {{displayModeBar: false, responsive: true}});

      const bar = document.getElementById('regionalBar');
      bar.on('plotly_click', ev => {{
        const region = ev.points?.[0]?.y;
        if (region) {{
          els.focusRegion.value = region;
          refresh();
        }}
      }});
    }}

    function renderActionMatrix(rows) {{
      const regionFocus = selectedRegionOrNull();
      const filtered = regionFocus ? rows.filter(r => r.region === regionFocus) : rows;
      Plotly.newPlot('matrix', [{{
        type: 'scatter',
        mode: 'markers+text',
        x: filtered.map(d => d.openings_yoy_pct),
        y: filtered.map(d => d.closures_yoy_pct),
        text: filtered.map(d => d.region_abbr),
        textposition: 'top center',
        marker: {{
          size: filtered.map(d => Math.max(12, (d.openings_q1_2025 || 0) / 1800)),
          color: filtered.map(d => d.net_change_abs),
          colorscale: 'RdYlGn',
          reversescale: false,
          showscale: true,
          colorbar: {{title: 'Net Delta'}}
        }},
        customdata: filtered,
        hovertemplate:
          '<b>%{{customdata.region}}</b><br>' +
          'Openings YoY: %{{x:.2f}}%<br>' +
          'Closures YoY: %{{y:.2f}}%<br>' +
          'Net Change: %{{customdata.net_change_abs:,}}<extra></extra>'
      }}], {{
        margin: {{l: 50, r: 20, t: 20, b: 45}},
        xaxis: {{title: 'Openings YoY %', zeroline: true, zerolinewidth: 1, zerolinecolor: '#888'}},
        yaxis: {{title: 'Closures YoY %', zeroline: true, zerolinewidth: 1, zerolinecolor: '#888'}},
        shapes: [
          {{type: 'line', x0: 0, x1: 0, y0: -15, y1: 20, line: {{color:'#777', dash:'dot'}}}},
          {{type: 'line', x0: -15, x1: 20, y0: 0, y1: 0, line: {{color:'#777', dash:'dot'}}}}
        ]
      }}, {{displayModeBar: false, responsive: true}});

      const scatter = document.getElementById('matrix');
      scatter.on('plotly_click', ev => {{
        const region = ev.points?.[0]?.customdata?.region;
        if (region) {{
          els.focusRegion.value = region;
          refresh();
        }}
      }});
    }}

    function renderMix(rows) {{
      const regionFocus = selectedRegionOrNull();
      const filtered = regionFocus ? rows.filter(r => r.region === regionFocus) : rows.slice(0, 8);
      Plotly.newPlot('mix', [
        {{
          type: 'bar',
          x: filtered.map(d => d.region),
          y: filtered.map(d => d.entrants_q1_2025),
          name: 'Entrants',
          marker: {{color: '#1f77b4'}}
        }},
        {{
          type: 'bar',
          x: filtered.map(d => d.region),
          y: filtered.map(d => d.reopening_q1_2025),
          name: 'Reopenings',
          marker: {{color: '#6baed6'}}
        }},
        {{
          type: 'bar',
          x: filtered.map(d => d.region),
          y: filtered.map(d => d.exits_q1_2025),
          name: 'Exits',
          marker: {{color: '#e34a33'}}
        }},
        {{
          type: 'bar',
          x: filtered.map(d => d.region),
          y: filtered.map(d => d.temp_closures_q1_2025),
          name: 'Temp closures',
          marker: {{color: '#fdbb84'}}
        }}
      ], {{
        barmode: 'stack',
        margin: {{l: 55, r: 20, t: 20, b: 95}},
        xaxis: {{tickangle: -35}},
        yaxis: {{title: 'Q1 2025 count'}},
        legend: {{orientation: 'h', y: 1.16}}
      }}, {{displayModeBar: false, responsive: true}});
    }}

    function renderInsights(rows) {{
      const list = els.insightsList;
      list.innerHTML = '';
      if (!rows.length) return;
      const byNet = [...rows].sort((a,b) => (b.net_q1_2025||0)-(a.net_q1_2025||0));
      const byPressure = [...rows].sort((a,b) => (b.closure_to_opening_ratio_q1_2025||0)-(a.closure_to_opening_ratio_q1_2025||0));
      const byDelta = [...rows].sort((a,b) => (a.net_change_abs||0)-(b.net_change_abs||0));
      const i1 = `${{byNet[0].region}} leads net openings in Q1 2025 with ${{fmtInt(byNet[0].net_q1_2025)}}.`;
      const i2 = `${{byPressure[0].region}} shows highest closure pressure (close/open=${{fmtRat(byPressure[0].closure_to_opening_ratio_q1_2025)}}).`;
      const i3 = `Largest deterioration vs Q1 2024: ${{byDelta[0].region}} (${{fmtInt(byDelta[0].net_change_abs)}}).`;
      [i1, i2, i3].forEach(t => {{
        const li = document.createElement('li');
        li.textContent = t;
        list.appendChild(li);
      }});
    }}

    function refresh() {{
      const rows = getFilteredScorecard();
      renderKPIs(rows);
      renderNationalTrend();
      renderRegionalBar(rows);
      renderActionMatrix(rows);
      renderMix(rows);
      renderInsights(rows);
    }}

    [els.groupFilter, els.sortBy, els.focusRegion, els.scope].forEach(el => el.addEventListener('change', refresh));
    refresh();
  </script>
</body>
</html>
"""


def main():
    ensure_demo_inputs()
    os.makedirs(ASSETS_DIR, exist_ok=True)
    if not os.path.exists(PLOTLY_LOCAL):
        with urlopen(PLOTLY_URL, timeout=60) as r:
            content = r.read()
        with open(PLOTLY_LOCAL, "wb") as f:
            f.write(content)

    data = prepare_data()
    html = build_html(data)
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print("Generated interactive offline demo:")
    print(OUT_HTML)
    print("Local asset:")
    print(PLOTLY_LOCAL)


if __name__ == "__main__":
    main()
