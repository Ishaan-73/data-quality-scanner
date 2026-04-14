"""
Generate a self-contained HTML report from a DQS JSON result file.

Usage:
    python3 generate_report.py tpcds_result.json
    python3 generate_report.py tpcds_result.json --out my_report.html
"""

from __future__ import annotations

import base64
import json
import math
import sys
from datetime import datetime
from pathlib import Path

# ── Pre-generated AI column semantics (TPC-DS demo) ──────────────────────
_sem_path = Path(__file__).parent / "column_semantics_result.json"
DEMO_SEMANTICS: dict = json.loads(_sem_path.read_text(encoding="utf-8")) if _sem_path.exists() else {}

# ── Layman descriptions for every check ID ────────────────────────────────
CHECK_PLAIN = {
    1:  {
        "what": "Are there blanks in important columns?",
        "why":  "Missing values in a column mean some records are incomplete. If a model tries to learn from them, it either breaks or silently ignores those rows — losing real signal.",
        "example": "The 'email' column is 8% empty. That means 1 in 12 customer records has no email — loyalty campaigns would miss them entirely.",
    },
    2:  {
        "what": "Are critical columns missing values together?",
        "why":  "Even if each column looks fine individually, a whole group of fields being null on the same row means that record is effectively useless for analysis.",
        "example": "3% of store_sales rows have no price, no quantity, and no store ID all at once — those rows cannot contribute to any revenue metric.",
    },
    3:  {
        "what": "Are there completely empty rows?",
        "why":  "A row where every key field is null carries zero information. It inflates row counts and misleads dashboards into thinking data arrived when it didn't.",
        "example": "45 rows in the orders table have no customer, no product, and no amount — they are ghost records that pad the count without adding value.",
    },
    4:  {
        "what": "When a condition is true, is the required field always filled?",
        "why":  "Some fields are only mandatory under certain circumstances. If they're missing exactly when they should be present, it signals a broken data pipeline or form-submission bug.",
        "example": "Preferred customers must have an email address, but 2% don't — those high-value customers can't receive targeted communications.",
    },
    5:  {
        "what": "Are primary keys truly unique?",
        "why":  "A primary key is supposed to identify each record uniquely. Duplicates mean the same transaction or entity was recorded twice — double-counting revenue, inflating counts.",
        "example": "The ticket_number column appears twice for 0.6% of store_sales rows — those transactions are counted twice in every sales report.",
    },
    6:  {
        "what": "Are business IDs unique across the dataset?",
        "why":  "Natural keys like customer_id or product_code should identify one real-world entity. Duplicates usually mean the same entity was created twice in the source system.",
        "example": "50% of item IDs appear more than once — in this case it's intentional (historical versions of the same product), but the check flags it so the analyst confirms.",
    },
    7:  {
        "what": "Are there near-duplicate records that should be one?",
        "why":  "Two rows with the same trimmed, uppercased key are almost certainly the same thing entered twice — perhaps once as 'John Smith' and once as 'JOHN SMITH'.",
        "example": "15 customer IDs differ only in whitespace — they represent the same person and will produce duplicate entries in any report grouped by customer.",
    },
    8:  {
        "what": "Do values actually match the expected data type?",
        "why":  "A price column storing the text 'N/A' instead of a number will silently become NULL when a model tries to use it, or crash the pipeline entirely.",
        "example": "3 rows in the revenue column contain the string 'pending' — any aggregation on that column will produce incorrect totals.",
    },
    9:  {
        "what": "Are values drawn only from the approved list?",
        "why":  "Categorical columns like 'status' or 'category' should only contain known values. Anything outside the list is usually a typo, a new undocumented code, or bad ETL.",
        "example": "The item category column contains 'Electroncis' (a typo) and 'NULL_STRING' — any model treating category as a feature will have two spurious classes.",
    },
    10: {
        "what": "Do values match the required format?",
        "why":  "Formats like email addresses or phone numbers have specific patterns. Rows that don't match can't be used for communication or join operations.",
        "example": "12% of email addresses are missing the '@' sign — those contacts can't receive any email, and joining on email to a CRM will miss them.",
    },
    11: {
        "what": "Are numeric values within the expected range?",
        "why":  "A quantity of -5 or an age of 300 is physically impossible and signals bad data entry, a unit conversion error, or a pipeline bug.",
        "example": "2 rows have a purchase quantity of 0 and 3 rows have quantity over 100 — both violate the contract that quantities must be between 1 and 100.",
    },
    12: {
        "what": "Are monetary or quantity columns free of negatives?",
        "why":  "Prices, costs, and quantities should never be negative. Negative values corrupt revenue totals and can flip the sign of an entire aggregated metric.",
        "example": "The wholesale_cost column has 6 rows with value -99.99 — these drag the average cost calculation down and distort margin reports.",
    },
    13: {
        "what": "Are related fields within the same row consistent?",
        "why":  "Some fields must obey a logical relationship. An end date before a start date, or a ship date before an order date, means the data was recorded incorrectly.",
        "example": "14 orders have a ship date earlier than the order date — impossible in reality, these rows will produce negative lead-time values in logistics models.",
    },
    14: {
        "what": "Do matching fields agree across two tables?",
        "why":  "When the same fact (e.g., a customer's name) appears in two tables, they should agree. Mismatches mean one table wasn't updated after the other changed.",
        "example": "The orders table shows customer 'Acme Corp' but the customers table now reads 'ACME Corporation' — the rename wasn't propagated.",
    },
    15: {
        "what": "Are all currency or unit codes from the approved set?",
        "why":  "Mixing USD and EUR in the same numeric column without conversion produces completely wrong totals. An unapproved currency code is a silent data corruption.",
        "example": "3% of revenue rows carry currency code 'GBP' but the system is US-only — those amounts are in pounds, not dollars, inflating revenue by ~25%.",
    },
    16: {
        "what": "Are text values in a consistent format (case, spacing)?",
        "why":  "\"USA\", \"U.S.A\", and \"united states\" all mean the same thing but a computer treats them as three different countries. This creates phantom segments in every report.",
        "example": "The country column has 'US', 'U.S.', 'USA', and 'United States' — a pivot table on country will show four rows instead of one.",
    },
    17: {
        "what": "Do all foreign key values exist in the parent table?",
        "why":  "A foreign key pointing nowhere means a record references something that doesn't exist — like a sales order for a store that was deleted. Joins on these fail silently.",
        "example": "999 sales rows reference store_sk = 999999 which doesn't exist in the store table — every join to get the store name returns NULL for those rows.",
    },
    18: {
        "what": "Are there orphan records with no parent?",
        "why":  "Orphan records are rows that should belong to a parent entity but don't. They won't appear in any joined report and inflate raw counts without contributing to analysis.",
        "example": "4.5% of sales rows reference a customer_sk with no matching customer record — those transactions are invisible in any customer-level analysis.",
    },
    19: {
        "what": "What fraction of rows successfully join to the reference table?",
        "why":  "A low join rate means most of your fact data can't be enriched with dimension attributes. Reports using those attributes will be based on only a fraction of the data.",
        "example": "Only 95.5% of store_sales rows join to date_dim — the remaining 4.5% (null date keys) are excluded from every date-based report.",
    },
    20: {
        "what": "How old is the most recent record in this table?",
        "why":  "Stale data means the pipeline hasn't run recently. A model trained on yesterday's data will make decisions based on outdated reality.",
        "example": "The latest record in the transactions table is 36 hours old against a 24-hour SLA — yesterday's afternoon data is missing entirely.",
    },
    21: {
        "what": "Has the table gone stale (no updates in too long)?",
        "why":  "A table that hasn't been updated in over a day (or your defined SLA) may indicate a failed pipeline job — affecting all downstream dashboards and models.",
        "example": "The inventory table hasn't received new rows in 3 days — stock levels shown in the app are 3 days out of date.",
    },
    22: {
        "what": "Is data arriving late relative to when the event happened?",
        "why":  "If an event happened at 2pm but doesn't appear in your warehouse until 8pm, models trained on 'recent' data are actually 6 hours behind reality.",
        "example": "10% of web clicks take more than 6 hours to arrive in the events table — real-time personalization using this data is running on stale signals.",
    },
    23: {
        "what": "Has the row count changed dramatically since yesterday?",
        "why":  "A sudden drop or spike in row count is the fastest signal that something broke — a loader ran twice, a partition was deleted, or a source feed stopped.",
        "example": "Today's orders table has 40% fewer rows than yesterday — either a batch job failed or a DELETE was accidentally run on production data.",
    },
    24: {
        "what": "Are any expected date partitions missing?",
        "why":  "If data is partitioned by date, every day should have a partition. A missing partition means an entire day of data is silently absent from all queries.",
        "example": "The events table has no partition for 2024-03-15 — every query that includes that date range is unknowingly missing one day of data.",
    },
    25: {
        "what": "Does the total in the source system match the total in the target?",
        "why":  "The sum of revenue (or any metric) should be identical between the source and its downstream copy. Any difference means data was lost or duplicated in transit.",
        "example": "Source system total revenue = $1.2M, warehouse total = $1.18M — $20K of transactions were dropped during the ETL load.",
    },
    26: {
        "what": "Do the numbers obey fundamental business rules?",
        "why":  "Some relationships between columns are always true by definition. If they're violated, at least one of the values is wrong.",
        "example": "Revenue = Price × Quantity must always hold. 8.5% of item rows show wholesale_cost > current_price — the store is apparently selling below cost.",
    },
    27: {
        "what": "Do values exist in the approved reference list?",
        "why":  "Some fields should only contain codes or values from a master reference table. Anything outside that table is an unknown code that can't be interpreted.",
        "example": "15 product codes in the sales table don't exist in the product catalogue — those sales can't be mapped to a category or brand.",
    },
    28: {
        "what": "Are there extreme outliers that look like errors?",
        "why":  "A single data entry error — like a price of $100,000 when all others are under $100 — can skew averages and ruin model training if not caught.",
        "example": "One row in the sales table has quantity = 9,999 while all others are under 100 — likely a data entry mistake that would dominate any average calculation.",
    },
    29: {
        "what": "Has the distribution of values shifted significantly?",
        "why":  "If the average age of your customers suddenly jumps from 35 to 55, something changed — either the data is wrong or a segment was accidentally excluded.",
        "example": "Average transaction value this week is $240, up from a historical mean of $85 — either premium products are selling unusually well or prices were entered incorrectly.",
    },
    30: {
        "what": "Have any columns been added, removed, or retyped?",
        "why":  "An unexpected schema change will silently break all downstream queries and models that depend on the old structure — usually discovered only when a dashboard goes blank.",
        "example": "The column 'customer_tier' was renamed to 'tier_code' in the source — every query referencing the old name now returns an error.",
    },
    31: {
        "what": "Are tables and columns properly documented in the catalogue?",
        "why":  "Columns without an owner or description can't be trusted — no one knows what they mean, so analysts make guesses and models use them incorrectly.",
        "example": "42% of columns in the data catalogue have no description and no assigned owner — anyone new to the team can't know what those fields represent.",
    },
    32: {
        "what": "Did the data pipeline run successfully?",
        "why":  "A failed pipeline means data didn't load. Everything that depends on it — dashboards, models, reports — is running on yesterday's (or older) snapshot.",
        "example": "The nightly ETL job for the orders table failed 3 out of the last 7 runs — 3 days of orders may be missing or loaded twice.",
    },
    33: {
        "what": "Are timestamps in the correct chronological order?",
        "why":  "Time-series models rely on events being in order. Out-of-order timestamps produce negative time deltas, broken lag features, and nonsensical sequences.",
        "example": "Some customer sessions have an 'end' event timestamped before the 'start' event — any session-duration calculation on these rows will be negative.",
    },
}


def score_color(score: float) -> str:
    if score >= 90: return "#47A848"
    if score >= 70: return "#F5A623"
    return "#E53935"

def score_label(score: float) -> str:
    if score >= 90: return "Good"
    if score >= 70: return "Fair"
    return "Poor"

def fmt_date(iso: str) -> str:
    try:
        return datetime.fromisoformat(iso.replace("Z", "")).strftime("%d %b %Y, %H:%M UTC")
    except Exception:
        return iso


def ai_readiness_svg(score: float, threshold: float = 90.0) -> str:
    """Segmented semicircle arc: left=0, top=50, right=100. Dark-bg optimised."""
    cx, cy, r, sw = 110, 90, 74, 14

    def pt(s: float):
        a = math.pi * (1.0 - s / 100.0)
        return cx + r * math.cos(a), cy - r * math.sin(a)

    x0,   y0   = pt(0)
    x70,  y70  = pt(70)
    xt,   yt   = pt(threshold)
    x100, y100 = pt(100)
    xs,   ys   = pt(min(max(score, 0), 100))

    dot_c = "#E53935" if score < 70 else ("#F5A623" if score < threshold else "#47A848")

    # Threshold tick extending outward
    ta     = math.pi * (1.0 - threshold / 100.0)
    tx_out = cx + (r + 18) * math.cos(ta)
    ty_out = cy - (r + 18) * math.sin(ta)

    # Label positions just outside arc
    def lpt(s: float, off: float = 16):
        a = math.pi * (1.0 - s / 100.0)
        return cx + (r + off) * math.cos(a), cy - (r + off) * math.sin(a)

    lx0,  ly0   = lpt(2)
    lxt,  lyt   = lpt(threshold, 20)
    lx100, ly100 = lpt(98)

    # All arcs: sweep=1 (clockwise in SVG) traces left→top→right (upper semicircle)
    return (
        f'<svg width="220" height="106" viewBox="0 0 220 106" style="overflow:visible">'
        # Background full arc
        f'<path d="M {x0:.1f},{y0:.1f} A {r},{r} 0 1,1 {x100:.1f},{y100:.1f}"'
        f' fill="none" stroke="rgba(255,255,255,0.10)" stroke-width="{sw}" stroke-linecap="butt"/>'
        # Red 0–70
        f'<path d="M {x0:.1f},{y0:.1f} A {r},{r} 0 0,1 {x70:.1f},{y70:.1f}"'
        f' fill="none" stroke="#E53935" stroke-width="{sw}" stroke-linecap="butt"/>'
        # Amber 70–threshold
        f'<path d="M {x70:.1f},{y70:.1f} A {r},{r} 0 0,1 {xt:.1f},{yt:.1f}"'
        f' fill="none" stroke="#F5A623" stroke-width="{sw}" stroke-linecap="butt"/>'
        # Green threshold–100
        f'<path d="M {xt:.1f},{yt:.1f} A {r},{r} 0 0,1 {x100:.1f},{y100:.1f}"'
        f' fill="none" stroke="#47A848" stroke-width="{sw}" stroke-linecap="butt"/>'
        # Gap lines between segments (thin white separators)
        f'<line x1="{x70:.1f}" y1="{y70:.1f}" x2="{cx+(r+sw/2)*math.cos(math.pi*0.3):.1f}"'
        f' y2="{cy-(r+sw/2)*math.sin(math.pi*0.3):.1f}"'
        f' stroke="#1B3D6F" stroke-width="2.5"/>'
        f'<line x1="{xt:.1f}" y1="{yt:.1f}" x2="{cx+(r+sw/2)*math.cos(ta):.1f}"'
        f' y2="{cy-(r+sw/2)*math.sin(ta):.1f}"'
        f' stroke="#1B3D6F" stroke-width="2.5"/>'
        # Threshold dashed tick
        f'<line x1="{xt:.1f}" y1="{yt:.1f}" x2="{tx_out:.1f}" y2="{ty_out:.1f}"'
        f' stroke="rgba(255,255,255,0.45)" stroke-width="1.5" stroke-dasharray="3,2"/>'
        # Score marker
        f'<circle cx="{xs:.1f}" cy="{ys:.1f}" r="9" fill="#fff" stroke="{dot_c}" stroke-width="3"/>'
        # Axis labels
        f'<text x="{lx0:.0f}" y="{ly0+4:.0f}" text-anchor="middle" font-size="10"'
        f' fill="rgba(255,255,255,0.4)">0</text>'
        f'<text x="{lxt:.0f}" y="{lyt:.0f}" text-anchor="middle" font-size="9" font-weight="700"'
        f' fill="rgba(255,255,255,0.65)">{threshold:.0f}</text>'
        f'<text x="{lx100:.0f}" y="{ly100+4:.0f}" text-anchor="middle" font-size="10"'
        f' fill="rgba(255,255,255,0.4)">100</text>'
        f'</svg>'
    )


def build_html(data: dict, logo_b64: str) -> str:
    scan_name  = data.get("scan_name", "DQS Scan")
    dialect    = data.get("connector_dialect", "").upper()
    overall    = float(data.get("overall_score", 0))
    passed     = data.get("checks_passed", 0)
    failed     = data.get("checks_failed", 0)
    skipped    = data.get("checks_skipped", 0)
    total      = data.get("total_checks", 0)
    duration   = data.get("duration_seconds", 0)
    scan_start = fmt_date(data.get("scan_start", ""))
    dimensions = data.get("dimensions", [])
    pii_mfs    = data.get("pii_manifests", [])
    is_synth   = data.get("is_synthetic", False)

    sc         = score_color(overall)
    mode_label = "Synthetic Clone" if is_synth else "Live Scan"
    logo_tag   = (f'<img src="data:image/png;base64,{logo_b64}" height="44" alt="GovernIQ">'
                  if logo_b64 else '<span style="color:#fff;font-size:22px;font-weight:700">GovernIQ</span>')

    # ── AI Semantics metadata ─────────────────────────────────────────────
    sem_col_count = sum(
        len(tdata.get("columns", {}))
        for tdata in DEMO_SEMANTICS.get("tables", {}).values()
    )
    sem_model     = DEMO_SEMANTICS.get("model", "gemini")
    sem_tables    = len(DEMO_SEMANTICS.get("tables", {}))
    sem_json      = json.dumps(DEMO_SEMANTICS)

    # ── AI Readiness ──────────────────────────────────────────────────────────
    AI_READY_THRESHOLD = 90.0
    gap_to_ready = max(0.0, AI_READY_THRESHOLD - overall)
    ready_pct    = min(100, int(overall / AI_READY_THRESHOLD * 100))

    # Priority tiers for check_ids (higher = more impactful for AI readiness)
    AI_PRIORITY = {
        # Tier 1 — directly corrupt model training
        9: 3, 11: 3, 15: 3, 16: 3, 26: 3, 13: 3,
        # Tier 2 — cause drift / bias
        4: 2, 22: 2, 23: 2, 25: 2,
        # Tier 1.5 — data integrity
        1: 2, 5: 2, 17: 2, 18: 2,
        # Everything else
    }
    DIM_IMPACT = {  # dimension weight proxy for impact ordering
        "completeness": 0.20, "uniqueness": 0.14, "validity": 0.14,
        "integrity": 0.12, "accuracy": 0.10, "freshness": 0.09,
        "consistency": 0.08, "volume": 0.05, "anomaly": 0.03,
    }

    # Collect all failing checks with an impact score
    fail_candidates: list[dict] = []
    for d in dimensions:
        dim_w = d.get("dimension_weight", 0.01)
        for c in d.get("checks", []):
            if not c.get("passed") and not c.get("skipped") and not c.get("error"):
                cid = c["check_id"]
                tier = AI_PRIORITY.get(cid, 1)
                impact = tier * dim_w * (1.0 - (c.get("pass_score") or 0))
                fail_candidates.append({
                    "check_id":   cid,
                    "check_name": c["check_name"],
                    "table":      c.get("table") or c.get("child_table") or "—",
                    "column":     c.get("column") or "—",
                    "metric":     c.get("metric_value"),
                    "threshold":  c.get("threshold"),
                    "dimension":  d["dimension"],
                    "impact":     impact,
                    "plain":      CHECK_PLAIN.get(cid, {}),
                })

    # Deduplicate by check_id — keep worst instance per check type
    seen: dict = {}
    for fc in sorted(fail_candidates, key=lambda x: -x["impact"]):
        cid = fc["check_id"]
        if cid not in seen:
            seen[cid] = fc
    top5 = list(seen.values())[:5]
    top5_json = json.dumps(top5, default=str)

    ring_color = score_color(overall)

    # ── Dimension chart ──────────────────────────────────────────────────────
    dim_labels  = json.dumps([d["dimension"].replace("_"," ").title() for d in dimensions])
    dim_scores  = json.dumps([round(d["dimension_score"], 1) for d in dimensions])
    dim_colors  = json.dumps([score_color(d["dimension_score"]) for d in dimensions])
    dim_passed  = json.dumps([d["checks_passed"] for d in dimensions])
    dim_failed  = json.dumps([d["checks_failed"] for d in dimensions])
    dim_height  = max(300, len(dimensions) * 42)

    # ── Dimension summary table (below chart) ────────────────────────────────
    dim_rows = ""
    for d in dimensions:
        ds, dc = d["dimension_score"], score_color(d["dimension_score"])
        df = d["checks_failed"]
        dim_rows += f"""<tr>
          <td style="font-weight:600;white-space:nowrap">{d['dimension'].replace('_',' ').title()}</td>
          <td style="color:#9ba8c0;font-size:12px;text-align:center">{d['dimension_weight']:.0%}</td>
          <td style="width:120px">
            <div style="background:#EEF2F7;border-radius:4px;height:7px">
              <div style="background:{dc};border-radius:4px;height:7px;width:{int(ds)}%"></div>
            </div>
          </td>
          <td style="font-weight:700;color:{dc};text-align:right;padding-right:6px">{ds:.1f}</td>
          <td style="text-align:center;color:#47A848;font-weight:600">{d['checks_passed']}</td>
          <td style="text-align:center;color:{'#E53935' if df else '#ccc'};font-weight:600">{df}</td>
          <td style="text-align:center;color:#9E9E9E">{d['checks_skipped']}</td>
        </tr>"""

    # ── All checks — embed as JS for modal + build hierarchical structure ─────
    all_checks = []
    for d in dimensions:
        for c in d.get("checks", []):
            c["_idx"] = len(all_checks)
            all_checks.append(c)

    # Build hierarchy: dimensions → check_types → instances
    hier = []
    for d in dimensions:
        check_types: dict = {}
        for c in d.get("checks", []):
            cid = c["check_id"]
            if cid not in check_types:
                check_types[cid] = {
                    "check_id":   cid,
                    "check_name": c["check_name"],
                    "instances":  [],
                    "n_passed":   0,
                    "n_failed":   0,
                    "n_skipped":  0,
                }
            ct = check_types[cid]
            ct["instances"].append(c)
            if c.get("skipped"):
                ct["n_skipped"] += 1
            elif c.get("passed"):
                ct["n_passed"] += 1
            else:
                ct["n_failed"] += 1

        for ct in check_types.values():
            scores = [i.get("pass_score", 0) for i in ct["instances"] if not i.get("skipped")]
            ct["avg_score"] = (sum(scores) / len(scores) * 100) if scores else 100.0
            fail_insts = [i for i in ct["instances"] if not i.get("passed") and not i.get("skipped")]
            ct["worst_table"] = fail_insts[0].get("table") or "—" if fail_insts else None

        hier.append({
            "dimension":        d["dimension"],
            "dimension_score":  d["dimension_score"],
            "dimension_weight": d["dimension_weight"],
            "checks_passed":    d["checks_passed"],
            "checks_failed":    d["checks_failed"],
            "checks_skipped":   d["checks_skipped"],
            "check_types":      list(check_types.values()),
        })

    hier_json        = json.dumps(hier, default=str)
    checks_json      = json.dumps(all_checks, default=str)
    check_plain_json = json.dumps(CHECK_PLAIN)

    # ── PII banner ───────────────────────────────────────────────────────────
    pii_html = ""
    if pii_mfs:
        prio  = {"BLOCK": 3, "SYNTHETIC_MODE": 2, "PROCEED_EXCLUDE": 1, "CLEAR": 0}
        worst = max(pii_mfs, key=lambda m: prio.get(m.get("gate_decision", "CLEAR"), 0))
        gate  = worst.get("gate_decision", "CLEAR")
        h_cnt = sum(m.get("high_count", 0)   for m in pii_mfs)
        m_cnt = sum(m.get("medium_count", 0) for m in pii_mfs)
        gate_styles = {
            "BLOCK":           ("#C62828", "#FFEBEE", "🚫"),
            "SYNTHETIC_MODE":  ("#1565C0", "#E3F2FD", "🔄"),
            "PROCEED_EXCLUDE": ("#E65100", "#FFF3E0", "⚠️"),
            "CLEAR":           ("#2E7D32", "#E8F5E9", "✅"),
        }
        gc, gb, gicon = gate_styles.get(gate, ("#666", "#eee", "ℹ️"))
        tags = "".join(
            f'<span style="font-size:11px;padding:3px 9px;border-radius:4px;border:1px solid '
            f'{"#C62828" if c["status"]=="HIGH" else "#E65100"};'
            f'color:{"#C62828" if c["status"]=="HIGH" else "#E65100"};background:#fff">'
            f'<b>{c["status"]}</b> {c["table"]}.{c["column"]}</span>'
            for m in pii_mfs for c in m.get("columns", [])
            if c.get("status") in ("HIGH", "MEDIUM")
        )[:10]
        pii_html = f"""
  <div style="background:{gb};border-left:4px solid {gc};border-radius:8px;
      padding:13px 20px;margin-bottom:20px;display:flex;align-items:center;gap:16px;flex-wrap:wrap">
    <span style="background:{gc};color:#fff;font-size:12px;font-weight:700;
        padding:4px 14px;border-radius:20px;flex-shrink:0">{gicon} {gate.replace('_',' ')}</span>
    <span style="font-size:13px">HIGH <b style="color:#C62828">{h_cnt}</b></span>
    <span style="font-size:13px">MEDIUM <b style="color:#E65100">{m_cnt}</b></span>
    <span style="font-size:13px;color:#888">Tables screened: <b>{len(pii_mfs)}</b></span>
    <div style="display:flex;flex-wrap:wrap;gap:6px">{tags}</div>
  </div>"""

    TH = ("text-align:left;padding:9px 12px;font-size:11px;text-transform:uppercase;"
          "letter-spacing:.05em;color:#6b7a99;border-bottom:2px solid #E8ECF0;font-weight:600")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>GovernIQ — {scan_name}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
        background:#F0F4F8;color:#1a2433;min-width:920px}}
  .card{{background:#fff;border-radius:12px;box-shadow:0 1px 4px rgba(0,0,0,.07);
         padding:24px 28px;margin-bottom:22px}}
  .card-title{{font-size:11px;font-weight:700;text-transform:uppercase;
               letter-spacing:.07em;color:#6b7a99;margin-bottom:18px}}
  .stat-box{{background:#F8FAFC;border-radius:8px;padding:14px 18px}}
  .stat-val{{font-size:26px;font-weight:700;line-height:1}}
  .stat-key{{font-size:11px;color:#6b7a99;font-weight:600;text-transform:uppercase;
             letter-spacing:.05em;margin-top:3px}}
  table{{width:100%;border-collapse:collapse;font-size:13px}}
  th{{text-align:left;padding:9px 12px;font-size:11px;text-transform:uppercase;
      letter-spacing:.05em;color:#6b7a99;border-bottom:2px solid #E8ECF0;font-weight:600}}
  td{{padding:10px 12px;border-bottom:1px solid #F0F4F8;vertical-align:middle}}
  tr:last-child td{{border-bottom:none}}
  .check-row:hover td{{background:#F5F8FF}}

  /* Modal */
  .modal-overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.45);
                  z-index:1000;align-items:center;justify-content:center}}
  .modal-overlay.open{{display:flex}}
  .modal{{background:#fff;border-radius:14px;width:680px;max-width:95vw;
          max-height:88vh;overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.25);
          animation:slideUp .2s ease}}
  @keyframes slideUp{{from{{transform:translateY(24px);opacity:0}}to{{transform:translateY(0);opacity:1}}}}
  .modal-header{{padding:20px 24px 16px;border-bottom:1px solid #F0F4F8;
                 display:flex;align-items:flex-start;justify-content:space-between}}
  .modal-body{{padding:20px 24px}}
  .modal-close{{background:none;border:none;font-size:22px;cursor:pointer;
                color:#9ba8c0;line-height:1;padding:0 4px}}
  .modal-close:hover{{color:#1a2433}}
  .detail-grid{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:18px}}
  .detail-box{{background:#F8FAFC;border-radius:8px;padding:12px 14px}}
  .detail-label{{font-size:10px;text-transform:uppercase;letter-spacing:.06em;
                 color:#9ba8c0;font-weight:600;margin-bottom:4px}}
  .detail-val{{font-size:15px;font-weight:600;color:#1a2433}}
  .sql-block{{background:#1a2433;border-radius:8px;padding:14px 16px;
              font-family:"SF Mono","Fira Code",monospace;font-size:12px;
              color:#a8d5a2;white-space:pre-wrap;word-break:break-all;line-height:1.6}}
  .error-block{{background:#FFEBEE;border-radius:8px;padding:12px 14px;
                color:#C62828;font-size:13px;margin-top:12px}}

  /* Hierarchical drill-down */
  .dim-card   {{border:1px solid #D1DCE8;border-radius:10px;margin-bottom:12px;overflow:hidden}}
  .dim-header {{display:flex;justify-content:space-between;align-items:center;
                padding:14px 18px;cursor:pointer;background:#F8FAFC;transition:background .15s}}
  .dim-header:hover {{background:#EEF2F7}}
  .dim-name   {{font-size:15px;font-weight:700;color:#1B3D6F}}
  .dim-weight {{font-size:11px;color:#9ba8c0;background:#EEF2F7;padding:2px 8px;border-radius:20px}}
  .dim-body   {{border-top:1px solid #D1DCE8}}
  .ct-row     {{display:flex;justify-content:space-between;align-items:center;
                padding:10px 18px 10px 32px;cursor:pointer;border-bottom:1px solid #F0F4F8;
                transition:background .12s}}
  .ct-row:hover {{background:#F5F8FF}}
  .ct-id      {{font-size:12px;font-weight:700;color:#1B3D6F;background:#E8F1FF;
                padding:2px 7px;border-radius:4px;flex-shrink:0}}
  .ct-name    {{font-size:13px;font-weight:600;color:#1a2433}}
  .inst-body  {{background:#FAFBFD;border-bottom:1px solid #EEF2F7}}
  .inst-table {{width:100%;border-collapse:collapse;font-size:12px}}
  .inst-table th {{padding:7px 12px 7px 18px;color:#9ba8c0;font-weight:600;font-size:10px;
                   text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid #EEF2F7;
                   text-align:left}}
  .inst-row td {{padding:7px 12px 7px 18px;border-bottom:1px solid #F5F6F8}}
  .inst-row:hover td {{background:#EEF2F7}}
  .score-pill {{font-size:13px;font-weight:700;padding:4px 12px;border-radius:20px}}
  .filter-btn {{border:1px solid #D1DCE8;background:#fff;border-radius:6px;
                padding:5px 14px;font-size:12px;cursor:pointer;color:#6b7a99}}
  .filter-btn.active {{background:#1B3D6F;color:#fff;border-color:#1B3D6F}}
  .chevron    {{font-size:11px;color:#9ba8c0;width:14px;display:inline-block;flex-shrink:0}}
  .chevron.small {{font-size:10px}}
</style>
</head>
<body>

<!-- Header -->
<div style="background:#1B3D6F;padding:14px 40px;display:flex;
            align-items:center;justify-content:space-between">
  {logo_tag}
  <div style="text-align:right">
    <div style="color:#fff;font-size:16px;font-weight:600">{scan_name}</div>
    <div style="color:rgba(255,255,255,.6);font-size:12px;margin-top:3px">
      {scan_start} &nbsp;·&nbsp; {dialect} &nbsp;·&nbsp; {mode_label}
    </div>
  </div>
</div>

<div style="max-width:1160px;margin:0 auto;padding:28px 24px">

  {pii_html}

  <!-- Top row: AI Readiness card + Discovery card + Hero card -->
  <div style="display:grid;grid-template-columns:280px 250px 1fr;gap:20px;margin-bottom:22px;align-items:stretch">

    <!-- AI Readiness card (dark navy) -->
    <div style="background:linear-gradient(145deg,#1B3D6F 0%,#2a5298 100%);border-radius:12px;
                padding:24px 20px 20px;display:flex;flex-direction:column;align-items:center;
                box-shadow:0 4px 18px rgba(27,61,111,.35)">
      <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.1em;
                  color:rgba(255,255,255,.5);margin-bottom:16px">AI Readiness</div>
      {ai_readiness_svg(overall, AI_READY_THRESHOLD)}
      <div style="margin-top:10px;text-align:center">
        <div style="font-size:40px;font-weight:800;color:#fff;line-height:1">{overall:.0f}</div>
        <div style="font-size:12px;color:rgba(255,255,255,.45);margin-top:3px">/ {AI_READY_THRESHOLD:.0f} target</div>
      </div>
      <div style="margin:14px 0 16px;text-align:center">
        {'<span style="background:rgba(71,168,72,.2);color:#7ee87f;font-size:13px;font-weight:700;padding:5px 16px;border-radius:20px;border:1px solid rgba(71,168,72,.35)">✓ AI Ready</span>' if overall >= AI_READY_THRESHOLD else
         f'<span style="background:rgba(245,166,35,.15);color:#f5c842;font-size:13px;font-weight:700;padding:5px 16px;border-radius:20px;border:1px solid rgba(245,166,35,.3)">{int(gap_to_ready)} points to AI Ready</span>'}
      </div>
      <button onclick="openReadyModal()"
        style="width:100%;background:rgba(255,255,255,.12);border:1px solid rgba(255,255,255,.25);
               color:#fff;border-radius:8px;padding:9px 0;font-size:12px;font-weight:600;
               cursor:pointer;letter-spacing:.02em;transition:background .15s"
        onmouseover="this.style.background='rgba(255,255,255,.22)'"
        onmouseout="this.style.background='rgba(255,255,255,.12)'">
        Top 5 Actions to Achieve AI Readiness
      </button>
    </div>

    <!-- Discovery Context card (purple → indigo) -->
    <div style="background:linear-gradient(145deg,#3b1f7a 0%,#1e3a8a 100%);border-radius:12px;
                padding:22px 20px;display:flex;flex-direction:column;justify-content:space-between;
                gap:14px;color:#fff;box-shadow:0 4px 24px rgba(59,31,122,.35)">
      <div>
        <div style="font-size:10px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;
                    color:#c4b5fd;margin-bottom:10px">Discovery Context</div>
        <div style="font-size:13px;line-height:1.65;color:#e0d9ff">
          Context Intelligence: Gemini has auto-documented
          <strong style="color:#fff">{sem_col_count} columns</strong>
          to provide semantic RCA insights.
        </div>
        <div style="margin-top:10px;font-size:11px;color:#a78bfa">
          Model: {sem_model} &nbsp;·&nbsp; {sem_tables} tables indexed
        </div>
      </div>
      <button onclick="openSemanticsModal()"
        style="background:rgba(255,255,255,.14);border:1px solid rgba(255,255,255,.28);
               color:#fff;border-radius:8px;padding:9px 0;font-size:12px;font-weight:600;
               cursor:pointer;letter-spacing:.02em;transition:background .15s;width:100%;text-align:center"
        onmouseover="this.style.background='rgba(255,255,255,.26)'"
        onmouseout="this.style.background='rgba(255,255,255,.14)'">
        Explore AI Dictionary →
      </button>
    </div>

    <!-- Hero card (white) -->
    <div class="card" style="margin-bottom:0;display:flex;align-items:center;gap:28px">
      <!-- Score ring -->
      <div style="position:relative;width:180px;height:180px;flex-shrink:0">
        <canvas id="scoreRing"></canvas>
        <div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);text-align:center">
          <div style="font-size:46px;font-weight:800;color:{sc};line-height:1">{overall:.0f}</div>
          <div style="font-size:11px;color:#9ba8c0;font-weight:600;letter-spacing:.05em">&nbsp;/ 100</div>
          <div style="font-size:12px;font-weight:700;color:{sc};margin-top:3px">{score_label(overall)}</div>
        </div>
      </div>
      <!-- Stats -->
      <div style="flex:1">
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px">
          <div class="stat-box"><div class="stat-val">{total}</div><div class="stat-key">Total</div></div>
          <div class="stat-box"><div class="stat-val" style="color:#47A848">{passed}</div><div class="stat-key">Passed</div></div>
          <div class="stat-box"><div class="stat-val" style="color:#E53935">{failed}</div><div class="stat-key">Failed</div></div>
          <div class="stat-box"><div class="stat-val" style="color:#9E9E9E">{skipped}</div><div class="stat-key">Skipped</div></div>
        </div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
          <span style="background:#E8F1FF;color:#1B3D6F;font-size:12px;font-weight:600;padding:4px 14px;border-radius:20px">{mode_label}</span>
          <span style="background:#F0F4F8;color:#6b7a99;font-size:12px;padding:4px 14px;border-radius:20px">⏱ {duration:.1f}s</span>
          <span style="background:#F0F4F8;color:#6b7a99;font-size:12px;padding:4px 14px;border-radius:20px">📐 {len(dimensions)} dimensions</span>
        </div>
      </div>
    </div>

  </div>

  <!-- Dimension Scores: chart full-width, table below -->
  <div class="card">
    <div class="card-title">Quality by Dimension</div>
    <div style="height:{dim_height}px;margin-bottom:24px">
      <canvas id="dimChart"></canvas>
    </div>
    <table>
      <thead><tr>
        <th>Dimension</th><th style="text-align:center">Weight</th>
        <th style="width:120px">Score bar</th><th style="text-align:right">Score</th>
        <th style="text-align:center">✓</th><th style="text-align:center">✗</th><th style="text-align:center">—</th>
      </tr></thead>
      <tbody>{dim_rows}</tbody>
    </table>
  </div>

  <!-- All Checks — hierarchical -->
  <div class="card">
    <div class="card-title" style="display:flex;align-items:center;gap:10px;margin-bottom:14px">
      All Checks
      <span style="background:#1B3D6F;color:#fff;border-radius:20px;padding:1px 9px;font-size:11px">{total}</span>
      <span style="background:#E8F5E9;color:#2E7D32;border-radius:20px;padding:1px 9px;font-size:11px">✓ {passed}</span>
      <span style="background:#FFEBEE;color:#C62828;border-radius:20px;padding:1px 9px;font-size:11px">✗ {failed}</span>
      <span style="margin-left:auto;font-size:11px;color:#9ba8c0;font-weight:400;text-transform:none;letter-spacing:0">Click to expand</span>
    </div>
    <div style="display:flex;gap:8px;margin-bottom:16px">
      <button class="filter-btn active" data-filter="all">All</button>
      <button class="filter-btn" data-filter="fail">Failed only</button>
      <button class="filter-btn" data-filter="pass">Passed only</button>
    </div>
    <div id="hierContainer"></div>
  </div>

</div>

<div style="text-align:center;font-size:12px;color:#9ba8c0;padding:16px 0 28px">
  GovernIQ Data Quality Scanner &nbsp;·&nbsp; {scan_start}
</div>

<!-- AI Readiness Modal -->
<div class="modal-overlay" id="readyOverlay">
  <div class="modal" style="width:680px;max-height:90vh;overflow-y:auto">
    <div class="modal-header">
      <div>
        <div style="font-size:11px;color:#9ba8c0;font-weight:600;text-transform:uppercase;
                    letter-spacing:.06em;margin-bottom:4px">AI Readiness · Score {overall:.1f} / 90.0 target</div>
        <div style="font-size:18px;font-weight:700;color:#1a2433">Top 5 Actions to Achieve AI Readiness</div>
      </div>
      <button class="modal-close" onclick="closeReadyModal()">×</button>
    </div>
    <div class="modal-body" id="readyBody"></div>
  </div>
</div>

<!-- Check detail modal -->
<div class="modal-overlay" id="modalOverlay">
  <div class="modal" id="modal" style="width:740px;max-height:90vh;overflow-y:auto">
    <div class="modal-header">
      <div>
        <div style="font-size:11px;color:#9ba8c0;font-weight:600;text-transform:uppercase;
                    letter-spacing:.06em;margin-bottom:4px" id="mDimension"></div>
        <div style="font-size:18px;font-weight:700;color:#1a2433" id="mName"></div>
      </div>
      <button class="modal-close" onclick="closeModal()">×</button>
    </div>
    <div class="modal-body">
      <!-- Plain-language explanation card -->
      <div id="mPlain" style="display:none;background:#F0F7FF;border-left:4px solid #1B3D6F;
           border-radius:0 8px 8px 0;padding:14px 16px;margin-bottom:18px">
        <div style="font-size:12px;font-weight:700;color:#1B3D6F;text-transform:uppercase;
                    letter-spacing:.06em;margin-bottom:6px">What this check does</div>
        <div style="font-size:14px;font-weight:600;color:#1a2433;margin-bottom:8px"
             id="mPlainWhat"></div>
        <div style="font-size:13px;color:#444;line-height:1.6;margin-bottom:10px"
             id="mPlainWhy"></div>
        <div style="background:#fff;border:1px solid #D1DCE8;border-radius:6px;
             padding:10px 13px;font-size:13px;color:#555;line-height:1.6">
          <span style="font-size:11px;font-weight:700;color:#47A848;text-transform:uppercase;
                       letter-spacing:.05em;display:block;margin-bottom:4px">Example</span>
          <span id="mPlainExample"></span>
        </div>
      </div>
      <!-- AI Semantic context hint -->
      <div id="mSemHint" style="display:none;margin-bottom:14px;
           background:linear-gradient(135deg,rgba(59,31,122,.07),rgba(30,58,138,.07));
           border-left:4px solid #7c3aed;border-radius:0 8px 8px 0;padding:14px 16px">
        <div style="font-size:11px;font-weight:700;color:#7c3aed;letter-spacing:.06em;
                    text-transform:uppercase;margin-bottom:6px">&#x1F52E; AI Context Hint</div>
        <div id="mSemHintText" style="font-size:13px;color:#374151;line-height:1.6"></div>
        <div id="mSemHintMeta" style="font-size:11px;color:#7c3aed;margin-top:6px"></div>
      </div>
      <div class="detail-grid" id="mGrid"></div>
      <div id="mSqlWrap" style="display:none;margin-top:14px">
        <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;
                    color:#9ba8c0;margin-bottom:8px">Executed SQL</div>
        <div class="sql-block" id="mSql"></div>
      </div>
      <div class="error-block" id="mError" style="display:none"></div>
    </div>
  </div>
</div>

<!-- AI Column Dictionary Modal -->
<div id="semOverlay" style="display:none;position:fixed;inset:0;background:rgba(10,15,30,.78);
     backdrop-filter:blur(6px);z-index:1100;padding:20px;overflow:auto"
     onclick="if(event.target===this)closeSemModal()">
  <div style="max-width:980px;margin:40px auto;background:#fff;border-radius:16px;
              overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,.45)">
    <div style="background:linear-gradient(135deg,#3b1f7a,#1e3a8a);padding:20px 26px;
                display:flex;align-items:center;justify-content:space-between">
      <div>
        <div style="color:#c4b5fd;font-size:10px;font-weight:700;letter-spacing:.12em;
                    text-transform:uppercase">AI Column Dictionary</div>
        <div style="color:#fff;font-size:17px;font-weight:700;margin-top:4px">
          Gemini Semantic Documentation
        </div>
      </div>
      <button onclick="closeSemModal()"
        style="background:rgba(255,255,255,.15);border:none;color:#fff;border-radius:8px;
               padding:6px 13px;cursor:pointer;font-size:15px;line-height:1">&#x2715;</button>
    </div>
    <div style="padding:14px 26px;border-bottom:1px solid #EEF2F7;background:#F8FAFC">
      <input id="semSearch" type="text" placeholder="Search by table or column name\u2026"
             oninput="filterSemTable()"
             style="width:100%;padding:9px 14px;border:1px solid #D1DCE8;border-radius:8px;
                    font-size:13px;outline:none;box-sizing:border-box">
    </div>
    <div style="overflow:auto;max-height:520px">
      <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead>
          <tr style="background:#F8FAFC">
            <th style="padding:10px 16px;text-align:left;color:#9ba8c0;font-size:11px;
                       font-weight:600;text-transform:uppercase;border-bottom:1px solid #EEF2F7;
                       position:sticky;top:0;background:#F8FAFC">Table</th>
            <th style="padding:10px 16px;text-align:left;color:#9ba8c0;font-size:11px;
                       font-weight:600;text-transform:uppercase;border-bottom:1px solid #EEF2F7;
                       position:sticky;top:0;background:#F8FAFC">Column</th>
            <th style="padding:10px 16px;text-align:left;color:#9ba8c0;font-size:11px;
                       font-weight:600;text-transform:uppercase;border-bottom:1px solid #EEF2F7;
                       position:sticky;top:0;background:#F8FAFC">Type</th>
            <th style="padding:10px 16px;text-align:center;color:#9ba8c0;font-size:11px;
                       font-weight:600;text-transform:uppercase;border-bottom:1px solid #EEF2F7;
                       position:sticky;top:0;background:#F8FAFC">Confidence</th>
            <th style="padding:10px 16px;text-align:left;color:#9ba8c0;font-size:11px;
                       font-weight:600;text-transform:uppercase;border-bottom:1px solid #EEF2F7;
                       position:sticky;top:0;background:#F8FAFC">Description</th>
          </tr>
        </thead>
        <tbody id="semTableBody"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
// ── Charts ──────────────────────────────────────────────────────────────────
new Chart(document.getElementById('scoreRing'), {{
  type: 'doughnut',
  data: {{ datasets: [{{ data: [{overall:.2f}, {100-overall:.2f}],
    backgroundColor: ['{sc}', '#EEF2F7'], borderWidth: 0, borderRadius: 6 }}] }},
  options: {{ cutout: '80%', responsive: true, maintainAspectRatio: true,
    plugins: {{ legend: {{ display: false }}, tooltip: {{ enabled: false }} }},
    animation: {{ duration: 1000, easing: 'easeInOutQuart' }} }}
}});

new Chart(document.getElementById('dimChart'), {{
  type: 'bar',
  data: {{ labels: {dim_labels}, datasets: [{{
    label: 'Score', data: {dim_scores},
    backgroundColor: {dim_colors}.map(c => c + '28'),
    borderColor: {dim_colors}, borderWidth: 2, borderRadius: 5, borderSkipped: false
  }}] }},
  options: {{
    indexAxis: 'y', responsive: true, maintainAspectRatio: false,
    scales: {{
      x: {{ min: 0, max: 100, grid: {{ color: '#F0F4F8' }},
             ticks: {{ font: {{ size: 11 }} }} }},
      y: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 12 }} }} }}
    }},
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{ callbacks: {{ label: ctx =>
        ` ${{ctx.raw.toFixed(1)}} / 100  ·  ✓ ${{({dim_passed})[ctx.dataIndex]}}  ✗ ${{({dim_failed})[ctx.dataIndex]}}`
      }} }}
    }},
    animation: {{ duration: 900, easing: 'easeInOutQuart' }}
  }}
}});

// ── Check data & hierarchy ───────────────────────────────────────────────────
const CHECKS      = {checks_json};
const CHECK_PLAIN = {check_plain_json};
const HIER        = {hier_json};
const TOP5        = {top5_json};

// ── AI Column Semantics ──────────────────────────────────────────────────────
const SEMANTICS = {sem_json};
const SEM_FLAT  = [];
Object.entries(SEMANTICS.tables || {{}}).forEach(([tbl, td]) => {{
  Object.entries(td.columns || {{}}).forEach(([col, cd]) => {{
    SEM_FLAT.push({{table: tbl, column: col, type: cd.type, confidence: cd.confidence, description: cd.description}});
  }});
}});

function confBadge(v) {{
  const ok  = v >= 90, mid = v >= 75;
  const bg  = ok ? '#e6f4ea' : mid ? '#fff7e6' : '#fdecea';
  const cl  = ok ? '#2e7d32' : mid ? '#b45309' : '#c62828';
  return `<span style="background:${{bg}};color:${{cl}};padding:2px 8px;border-radius:20px;font-size:11px;font-weight:600">${{v}}%</span>`;
}}

function renderSemTable(rows) {{
  const tbody = document.getElementById('semTableBody');
  if (!rows.length) {{
    tbody.innerHTML = '<tr><td colspan="5" style="padding:24px;text-align:center;color:#9ba8c0">No results found</td></tr>';
    return;
  }}
  tbody.innerHTML = rows.map((r, i) => `
    <tr style="border-bottom:1px solid #F0F4F8;background:${{i % 2 ? '#FAFBFC' : '#fff'}}">
      <td style="padding:10px 16px;color:#1B3D6F;font-weight:600;font-size:12px">${{r.table}}</td>
      <td style="padding:10px 16px;font-family:monospace;font-size:12px;color:#374151">${{r.column}}</td>
      <td style="padding:10px 16px;font-family:monospace;font-size:11px;color:#6b7280">${{r.type || '—'}}</td>
      <td style="padding:10px 16px;text-align:center">${{confBadge(r.confidence || 0)}}</td>
      <td style="padding:10px 16px;color:#374151;font-size:13px;line-height:1.5">${{r.description || '—'}}</td>
    </tr>`).join('');
}}

function filterSemTable() {{
  const q = document.getElementById('semSearch').value.toLowerCase();
  renderSemTable(q ? SEM_FLAT.filter(r =>
    r.table.toLowerCase().includes(q) || r.column.toLowerCase().includes(q)
  ) : SEM_FLAT);
}}

function openSemanticsModal() {{
  renderSemTable(SEM_FLAT);
  document.getElementById('semSearch').value = '';
  document.getElementById('semOverlay').style.display = 'block';
  document.body.style.overflow = 'hidden';
}}

function closeSemModal() {{
  document.getElementById('semOverlay').style.display = 'none';
  document.body.style.overflow = '';
}}

// ── AI Readiness modal ──────────────────────────────────────────────────────
function buildFixHint(t) {{
  const pct = v => v != null ? (parseFloat(v) * 100).toFixed(1) + '%' : '?';
  const val = pct(t.metric);
  const thr = pct(t.threshold);
  const loc = t.column && t.column !== '—'
    ? `<code>${{t.table}}.${{t.column}}</code>`
    : `table <code>${{t.table}}</code>`;
  const hints = {{
    1:  `${{loc}} has ${{val}} null values (limit: ${{thr}}). Trace nulls back to the source system or pipeline step that populates this column.`,
    2:  `${{val}} of rows in ${{loc}} are missing multiple key fields simultaneously. Investigate the ETL job that loads this table.`,
    4:  `${{val}} of conditional rows in ${{loc}} are missing a required field (limit: ${{thr}}). Check the business rule and the upstream form/API that feeds this column.`,
    5:  `${{val}} of values in ${{loc}} are duplicate primary keys (limit: ${{thr}}). The source system may be inserting rows multiple times — add a DISTINCT or dedup step in the pipeline.`,
    9:  `${{val}} of values in ${{loc}} fall outside the approved domain (limit: ${{thr}}). Add a mapping or reject-and-alert step to catch unknown codes before they reach this table.`,
    11: `Values in ${{loc}} fall outside the expected range. Clamp or reject out-of-range values in the ingestion layer.`,
    12: `${{loc}} contains ${{val}} negative values which should never be negative. Add a NOT NULL + positive-value constraint or a pipeline validation step.`,
    13: `${{val}} of rows in ${{loc}} violate a date-ordering rule (limit: ${{thr}}). Audit the source timestamps and the pipeline that writes them.`,
    16: `${{val}} of values in ${{loc}} are in inconsistent formats. Apply a normalisation transform (uppercase / trim / regex replace) in the ETL layer.`,
    17: `${{val}} of FK values in ${{loc}} have no matching parent record (limit: ${{thr}}). Load dimension tables before fact tables, or add a referential integrity check to the pipeline.`,
    18: `${{val}} of records in ${{loc}} are orphaned — they reference a non-existent parent. Run a cleanup query to delete or remap orphaned rows.`,
    19: `Only ${{val}} of rows in ${{loc}} join successfully to the reference table. Investigate why FK values are missing before using this data for joins.`,
    22: `${{val}} of events in ${{loc}} are arriving late (beyond the SLA). Investigate the latency in the upstream source or messaging queue.`,
    23: `Row count in ${{loc}} changed by more than ${{thr}} since the last load. Check whether the pipeline ran twice or a DELETE was executed.`,
    25: `Metric totals in ${{loc}} differ from the source by ${{val}}. Reconcile the ETL load to find where rows are being dropped or duplicated.`,
    26: `${{val}} of rows in ${{loc}} violate a business rule (limit: ${{thr}}). Identify which rule is broken from the check config and fix the upstream calculation.`,
  }};
  return hints[t.check_id] || (t.plain && t.plain.example ? t.plain.example : 'Review the failing rows and trace back to the source system.');
}}

function openReadyModal() {{
  const body = document.getElementById('readyBody');
  if (!TOP5.length) {{
    body.innerHTML = '<p style="color:#47A848;font-weight:600;padding:12px 0">All critical checks are passing — your data is AI Ready!</p>';
  }} else {{
    const items = TOP5.map((t, i) => {{
      const dim   = (t.dimension || '').replace(/_/g,' ').replace(/\\b\\w/g, l => l.toUpperCase());
      const plain = t.plain || {{}};
      const val   = t.metric  != null ? parseFloat(t.metric).toFixed(4)     : '—';
      const thr   = t.threshold != null ? parseFloat(t.threshold).toFixed(4) : '—';
      const tier_colors = ['#C62828','#E53935','#E65100','#1565C0','#4527A0'];
      const tc = tier_colors[i] || '#6b7a99';
      const tcLight = ['rgba(198,40,40,.08)','rgba(229,57,53,.07)','rgba(230,101,0,.07)','rgba(21,101,192,.07)','rgba(69,39,160,.07)'][i] || 'rgba(0,0,0,.04)';
      return `
      <div style="border:1px solid #E8ECF0;border-radius:10px;padding:16px 18px;margin-bottom:12px;
                  border-left:4px solid ${{tc}};background:${{tcLight}}">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">
          <span style="background:${{tc}};color:#fff;font-size:12px;font-weight:700;
                       width:26px;height:26px;border-radius:50%;display:flex;align-items:center;
                       justify-content:center;flex-shrink:0">${{i+1}}</span>
          <div>
            <div style="font-size:14px;font-weight:700;color:#1a2433">Check #${{t.check_id}} — ${{t.check_name}}</div>
            <div style="font-size:11px;color:#9ba8c0">
              ${{dim}} &nbsp;·&nbsp;
              <span style="font-family:monospace">${{t.table}}${{t.column !== '—' ? '.' + t.column : ''}}</span>
            </div>
          </div>
          <div style="margin-left:auto;text-align:right;flex-shrink:0">
            <div style="font-size:10px;color:#9ba8c0;text-transform:uppercase;letter-spacing:.05em">Value / Threshold</div>
            <div style="font-family:monospace;font-size:13px;color:${{tc}};font-weight:700">${{val}} / ${{thr}}</div>
          </div>
        </div>
        ${{plain.what ? `<div style="font-size:13px;color:#444;line-height:1.6;margin-bottom:8px">${{plain.why}}</div>` : ''}}
        <div style="background:#EEF4FF;border-radius:6px;padding:9px 12px;font-size:12px;color:#333;line-height:1.6;border-left:3px solid #1B3D6F">
          <span style="font-size:10px;font-weight:700;color:#1B3D6F;text-transform:uppercase;letter-spacing:.05em;display:block;margin-bottom:3px">What to fix</span>
          ${{buildFixHint(t)}}
        </div>
      </div>`;
    }}).join('');
    body.innerHTML = `
      <div style="background:#EEF4FF;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:13px;color:#1B3D6F;font-weight:500;border-left:3px solid #1B3D6F">
        Highest-impact failing checks — fixing these moves the score closest to the 90-point AI Ready threshold.
      </div>
      ${{items}}`;
  }}
  document.getElementById('readyOverlay').classList.add('open');
}}

function closeReadyModal() {{
  document.getElementById('readyOverlay').classList.remove('open');
}}

document.getElementById('readyOverlay').addEventListener('click', e => {{
  if (e.target === document.getElementById('readyOverlay')) closeReadyModal();
}});

// ── Hierarchical rendering ───────────────────────────────────────────────────
function scoreColor(s) {{
  return s >= 90 ? '#47A848' : s >= 70 ? '#F5A623' : '#E53935';
}}

function renderInstances(instances, filter) {{
  const rows = instances.map(inst => {{
    if (filter === 'fail' && (inst.passed || inst.skipped)) return '';
    if (filter === 'pass' && (!inst.passed || inst.skipped)) return '';
    const ps  = ((inst.pass_score || 0) * 100).toFixed(1);
    const sc  = scoreColor(parseFloat(ps));
    const badge = inst.skipped ? '⊘ SKIP' : inst.passed ? '✓ PASS' : '✗ FAIL';
    const bColor = inst.skipped ? '#9E9E9E' : inst.passed ? '#2E7D32' : '#C62828';
    const val = inst.metric_value != null ? parseFloat(inst.metric_value).toFixed(4) : (inst.error ? 'ERR' : '—');
    const thr = inst.threshold    != null ? parseFloat(inst.threshold).toFixed(4)    : '—';
    return `<tr class="inst-row" onclick="openModal(${{inst._idx}})" style="cursor:pointer">
      <td>${{inst.table || '—'}}</td>
      <td>${{inst.column || '—'}}</td>
      <td style="font-family:monospace">${{val}}</td>
      <td style="font-family:monospace">${{thr}}</td>
      <td><span style="color:${{sc}};font-weight:700">${{ps}}%</span></td>
      <td><span style="color:${{bColor}};font-weight:700">${{badge}}</span></td>
    </tr>`;
  }}).join('');
  if (!rows.trim()) return '<div style="padding:12px;color:#9ba8c0;font-size:13px">No matching checks</div>';
  return `<table class="inst-table"><thead><tr>
    <th>Table</th><th>Column</th><th>Value</th><th>Threshold</th><th>Score</th><th>Status</th>
  </tr></thead><tbody>${{rows}}</tbody></table>`;
}}

function renderCheckTypes(checkTypes, di, filter) {{
  return checkTypes.map((ct, ci) => {{
    if (filter === 'fail' && ct.n_failed === 0) return '';
    if (filter === 'pass' && ct.n_passed === 0) return '';
    const sc = scoreColor(ct.avg_score);
    const worstHtml = ct.worst_table
      ? `<span style="color:#E53935;font-size:11px;margin-left:8px">worst: ${{ct.worst_table}}</span>`
      : '';
    return `
      <div class="ct-row" onclick="toggleCheckType(${{di}},${{ci}})">
        <div style="display:flex;align-items:center;gap:10px;flex:1;min-width:0">
          <span class="chevron small" id="chev-${{di}}-${{ci}}">▶</span>
          <span class="ct-id">#${{ct.check_id}}</span>
          <span class="ct-name">${{ct.check_name}}</span>
          ${{worstHtml}}
        </div>
        <div style="display:flex;align-items:center;gap:12px;flex-shrink:0">
          <div style="width:80px;background:#EEF2F7;border-radius:4px;height:6px">
            <div style="width:${{Math.min(ct.avg_score,100)}}%;background:${{sc}};border-radius:4px;height:6px"></div>
          </div>
          <span style="font-size:12px;font-weight:700;color:${{sc}};min-width:38px;text-align:right">${{ct.avg_score.toFixed(1)}}</span>
          <span style="font-size:11px;color:#47A848">✓${{ct.n_passed}}</span>
          <span style="font-size:11px;color:${{ct.n_failed ? '#E53935' : '#ccc'}}">✗${{ct.n_failed}}</span>
          <span style="font-size:11px;color:#9E9E9E">${{ct.n_skipped ? ct.n_skipped + '⊘' : ''}}</span>
        </div>
      </div>
      <div class="inst-body" id="inst-body-${{di}}-${{ci}}" style="display:none">
        ${{renderInstances(ct.instances, filter)}}
      </div>`;
  }}).join('');
}}

function renderHierarchy(filter) {{
  filter = filter || 'all';
  const container = document.getElementById('hierContainer');
  container.innerHTML = '';
  HIER.forEach((dim, di) => {{
    const sc = scoreColor(dim.dimension_score);
    const card = document.createElement('div');
    card.className = 'dim-card';
    card.innerHTML = `
      <div class="dim-header" onclick="toggleDim(${{di}})">
        <div style="display:flex;align-items:center;gap:12px">
          <span class="chevron" id="chev-${{di}}">▶</span>
          <span class="dim-name">${{dim.dimension.replace(/_/g,' ').replace(/\\b\\w/g,l=>l.toUpperCase())}}</span>
          <span class="dim-weight">${{(dim.dimension_weight*100).toFixed(0)}}% weight</span>
        </div>
        <div style="display:flex;align-items:center;gap:16px">
          <span class="score-pill" style="background:${{sc}}22;color:${{sc}}">${{dim.dimension_score.toFixed(1)}}</span>
          <span style="color:#47A848;font-size:13px">✓ ${{dim.checks_passed}}</span>
          <span style="color:#E53935;font-size:13px">✗ ${{dim.checks_failed}}</span>
        </div>
      </div>
      <div class="dim-body" id="dim-body-${{di}}" style="display:none"></div>`;
    container.appendChild(card);
  }});
}}

function toggleDim(di) {{
  const body = document.getElementById(`dim-body-${{di}}`);
  const chev = document.getElementById(`chev-${{di}}`);
  const open = body.style.display === 'none';
  body.style.display = open ? 'block' : 'none';
  chev.textContent = open ? '▼' : '▶';
  if (open) body.innerHTML = renderCheckTypes(HIER[di].check_types, di, currentFilter);
}}

function toggleCheckType(di, ci) {{
  const body = document.getElementById(`inst-body-${{di}}-${{ci}}`);
  const chev = document.getElementById(`chev-${{di}}-${{ci}}`);
  const open = body.style.display === 'none';
  body.style.display = open ? 'block' : 'none';
  chev.textContent = open ? '▼' : '▶';
}}

let currentFilter = 'all';
document.addEventListener('DOMContentLoaded', () => {{
  document.querySelectorAll('.filter-btn').forEach(btn => {{
    btn.addEventListener('click', () => {{
      currentFilter = btn.dataset.filter;
      document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      renderHierarchy(currentFilter);
    }});
  }});
  renderHierarchy();
}});

function openModal(idx) {{
  const c = CHECKS[idx];
  if (!c) return;

  document.getElementById('mDimension').textContent =
    (c.dimension || '').replace(/_/g,' ').replace(/\\b\\w/g, l => l.toUpperCase()) +
    '  ·  Check #' + c.check_id;
  document.getElementById('mName').textContent = c.check_name;

  // Plain-language card
  const plain = CHECK_PLAIN[c.check_id];
  const plainEl = document.getElementById('mPlain');
  if (plain) {{
    document.getElementById('mPlainWhat').textContent    = plain.what;
    document.getElementById('mPlainWhy').textContent     = plain.why;
    document.getElementById('mPlainExample').textContent = plain.example;
    plainEl.style.display = 'block';
  }} else {{
    plainEl.style.display = 'none';
  }}

  // AI Semantic context hint
  const semHint = document.getElementById('mSemHint');
  const semEntry = SEMANTICS.tables?.[c.table]?.columns?.[c.column] || null;
  if (semEntry && semEntry.description) {{
    document.getElementById('mSemHintText').textContent = semEntry.description;
    document.getElementById('mSemHintMeta').textContent =
      `Confidence: ${{semEntry.confidence}}%\u2003·\u2003Type: ${{semEntry.type}}`;
    semHint.style.display = 'block';
  }} else {{
    semHint.style.display = 'none';
  }}

  const ps   = ((c.pass_score || 0) * 100).toFixed(1);
  const psColor = ps >= 90 ? '#47A848' : ps >= 70 ? '#F5A623' : '#E53935';
  const status = c.skipped ? '— Skipped' : c.passed ? '✓ Passed' : '✗ Failed';
  const stColor = c.skipped ? '#9E9E9E' : c.passed ? '#2E7D32' : '#C62828';

  const fields = [
    ['Status',           `<span style="font-weight:700;color:${{stColor}}">${{status}}</span>`],
    ['Pass Score',       `<span style="font-weight:700;color:${{psColor}}">${{ps}}%</span>`],
    ['Metric Value',     c.metric_value != null ? parseFloat(c.metric_value).toFixed(6) : '—'],
    ['Threshold',        c.threshold    != null ? parseFloat(c.threshold).toFixed(6)    : '—'],
    ['Effective Weight', c.effective_weight != null ? (c.effective_weight * 100).toFixed(3) + '%' : '—'],
    ['Weighted Score',   c.weighted_score   != null ? (c.weighted_score   * 100).toFixed(3) + '%' : '—'],
    ['Table',            c.table  || '—'],
    ['Column',           c.column || '—'],
  ];

  document.getElementById('mGrid').innerHTML = fields.map(([label, val]) => `
    <div class="detail-box">
      <div class="detail-label">${{label}}</div>
      <div class="detail-val">${{val}}</div>
    </div>`).join('');

  const sqlWrap = document.getElementById('mSqlWrap');
  const sqlEl   = document.getElementById('mSql');
  if (c.executed_sql) {{
    sqlEl.textContent = c.executed_sql;
    sqlWrap.style.display = 'block';
  }} else {{
    sqlWrap.style.display = 'none';
  }}

  const errEl = document.getElementById('mError');
  if (c.error && c.error !== 'pii_excluded') {{
    errEl.textContent = '⚠ ' + c.error;
    errEl.style.display = 'block';
  }} else if (c.error === 'pii_excluded') {{
    errEl.textContent = '🔒 Column excluded — PII detected by screener';
    errEl.style.display = 'block';
  }} else {{
    errEl.style.display = 'none';
  }}

  document.getElementById('modalOverlay').classList.add('open');
}}

function closeModal() {{
  document.getElementById('modalOverlay').classList.remove('open');
}}

document.getElementById('modalOverlay').addEventListener('click', e => {{
  if (e.target === document.getElementById('modalOverlay')) closeModal();
}});
document.addEventListener('keydown', e => {{
  if (e.key === 'Escape') {{ closeModal(); closeReadyModal(); closeSemModal(); }}
}});

</script>
</body>
</html>"""


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("Usage: python3 generate_report.py <result.json> [--out report.html]")
        sys.exit(1)

    json_path = args[0]
    out_path  = "dqs_report.html"
    if "--out" in args:
        out_path = args[args.index("--out") + 1]

    data     = json.loads(Path(json_path).read_text(encoding="utf-8"))
    logo_b64 = ""
    logo_file = Path("refined_logo.png")
    if logo_file.exists():
        logo_b64 = base64.b64encode(logo_file.read_bytes()).decode()

    Path(out_path).write_text(build_html(data, logo_b64), encoding="utf-8")
    print(f"Report saved → {out_path}")
