# CLAUDE.md

## What this project is

A data quality scanner (`dqs`) that runs parameterized checks against enterprise data warehouses, scores them, and produces HTML reports. Two modes: **live scan** (direct DB connection) and **synthetic clone** (profile only, synthesize locally in DuckDB — no raw data leaves production).

---

## 13 Quality Dimensions & Weights

| Dimension | Weight | Check IDs |
|---|---|---|
| Completeness | 0.20 | 1–4 |
| Uniqueness | 0.14 | 5–7 |
| Validity | 0.14 | 8–12 |
| Integrity | 0.12 | 17–19 |
| Accuracy | 0.10 | 25–27 |
| Freshness | 0.09 | 20–22 |
| Consistency | 0.08 | 13–16 |
| Volume | 0.05 | 23–24 |
| Anomaly | 0.03 | 28–29 |
| Schema | 0.02 | 30 |
| Pipeline | 0.01 | 32 |
| Time Series | 0.01 | 33 |
| Metadata | 0.01 | 31 |

Dimension weights sum to 1.00. Each dimension has internal check weights that also sum to 1.00. Effective weight per check = `dim_weight × check_within_dim_weight`.

---

## Scoring

`overall_score = Σ(effective_weight × pass_score) × 100`

- `pass_score = 1.0` if check passes
- `pass_score = max(0, 1 - (actual − threshold) / threshold)` if it fails (proximity scoring)

**AI Readiness threshold: 90 points.** Scores below 90 show a gap in points. Top 5 highest-impact failing checks are surfaced as priority actions.

---

## MVP vs Phase 2

- **MVP (20 checks):** completeness (1–4), uniqueness (5–7), validity (8–12), integrity (17–19), freshness (20–22), volume (23–24)
- **Phase 2 (13 checks):** consistency (13–16), accuracy (25–27), anomaly (28–29), schema (30), pipeline (32)
- **Later (2 checks):** metadata (31), time_series (33)

---

## Report generation

`generate_report.py` takes a scan result JSON and produces a self-contained HTML report. Key outputs: overall score ring, dimension breakdown chart, AI Readiness panel (segmented arc, top 5 actions modal), hierarchical drill-down of all checks.
