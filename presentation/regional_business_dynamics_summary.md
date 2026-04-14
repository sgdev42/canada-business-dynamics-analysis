# Regional Brief: Business Openings vs Closures

## Scope
- Source: Statistics Canada table `33-10-0270-01`
- Window compared: **Jan-Mar 2024** vs **Jan-Mar 2025**
- Geography: Provinces and territories (Nunavut has suppressed values in this period)
- Industry: `Business sector industries [T004]`

## What Changed
- Openings rose slightly: **134,484 -> 135,088** (+0.45%)
- Closures rose faster: **130,918 -> 134,632** (+2.84%)
- Net openings fell sharply: **3,566 -> 456** (down **3,110**)

## Regional Highlights
- Strongest net openings in Q1 2025:
  - British Columbia: **+592**
  - Alberta: **+510**
  - Quebec: **+315**
- Weakest net outcomes in Q1 2025:
  - Ontario: **-935**
  - Nova Scotia: **-78**
  - Saskatchewan: **-61**
- Highest closure pressure (closures/openings ratio) in Q1 2025:
  - Prince Edward Island: **1.062**
  - Nova Scotia: **1.027**
  - Yukon: **1.027**

## Why It Matters
- Entrepreneurial churn is rising in several regions even where startup formation remains stable.
- A national increase in closures relative to openings suggests greater early-stage vulnerability, especially in high-cost or lower-scale markets.
- Ontario’s decline has outsized national impact due to base size.

## Recommended Actions
1. Launch **retention-first support** in high-closure-pressure regions: 90/180/365-day survival coaching and liquidity planning.
2. Expand **growth acceleration** in strong-net regions (BC, Alberta): faster financing pathways and mentor matching by sector.
3. Stand up an **Ontario stabilization stream**: market-validation sprints, customer acquisition diagnostics, and post-launch check-ins.
4. Track a monthly **regional risk score** in Power BI/Tableau using the output CSVs.

## Files Produced
- Presentation: `presentation/regional_business_dynamics_q1_2024_vs_2025.html`
- Summary table: `outputs/regional_summary_q1_2024_vs_2025.csv`
- Monthly table: `outputs/regional_monthly_open_close_q1_2024_2025.csv`
- National trend: `outputs/national_monthly_q1_2024_2025.csv`
- SQLite database: `outputs/business_dynamics_q1.db`
- SQL queries: `sql/regional_analysis_queries.sql`
