"""
Questionnaire → scan_config.yaml generator.

Combines:
  - Auto-discovery (Category 1 checks inferred from questionnaire structure)
  - Questionnaire answers (Category 2 / domain-knowledge checks)

Returns a plain dict that the caller serialises to YAML.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


# ── Public entry point ──────────────────────────────────────────────────────

def generate_scan_config(questionnaire: dict) -> dict:
    """
    Convert a filled questionnaire dict into a DQS scan_config dict.
    """
    meta      = questionnaire.get("meta", {})
    tables    = questionnaire.get("tables", [])

    checks: List[dict] = []
    for tbl in tables:
        checks.extend(_checks_for_table(tbl))

    return {
        "scan_name": meta.get("scan_name", "generated_scan"),
        "mode":      "live",
        "connector": meta.get("connector", {}),
        "checks":    checks,
    }


# ── Per-table check generator ───────────────────────────────────────────────

def _checks_for_table(tbl: dict) -> List[dict]:
    name               = tbl["name"]
    id_cols            = tbl.get("id_columns", []) or []
    monetary_cols      = tbl.get("monetary_columns", []) or []
    categorical_cols   = tbl.get("categorical_columns", []) or []
    numeric_cols       = tbl.get("numeric_columns", []) or []
    timestamp_cols     = tbl.get("timestamp_columns", []) or []
    foreign_keys       = tbl.get("foreign_keys", []) or []
    business_rules     = tbl.get("business_rules", []) or []
    cond_completeness  = tbl.get("conditional_completeness", []) or []
    volume             = tbl.get("volume", {}) or {}

    checks: List[dict] = []

    # ── Category 1: Auto-discovery ──────────────────────────────────────────

    # Check 1 — Null ratio on every non-PK column
    pk_cols = {c["column"] for c in id_cols if c.get("is_primary_key")}
    all_cols = (
        [c["column"] for c in monetary_cols]
        + [c["column"] for c in categorical_cols]
        + [c["column"] for c in numeric_cols]
        + [c["column"] for c in timestamp_cols]
        + [c["column"] for c in id_cols if not c.get("is_primary_key")]
    )
    for col in all_cols:
        checks.append({"check_id": 1, "table": name, "column": col})

    # Check 2 — Group null ratio on critical columns (monetary + PKs)
    critical_group = (
        [c["column"] for c in monetary_cols]
        + [c["column"] for c in id_cols]
    )
    if len(critical_group) >= 2:
        checks.append({"check_id": 2, "table": name, "columns": critical_group})

    # Check 3 — Empty row check across all key columns
    if critical_group:
        checks.append({"check_id": 3, "table": name, "columns": critical_group})

    # Check 5 — PK duplicate ratio
    for c in id_cols:
        if c.get("is_primary_key"):
            checks.append({"check_id": 5, "table": name, "pk_column": c["column"]})

    # Check 6 — Business key uniqueness
    bk_cols = [c["column"] for c in id_cols if c.get("is_business_key")]
    if bk_cols:
        checks.append({"check_id": 6, "table": name, "business_key_columns": bk_cols})

    # Check 7 — Near-duplicate detection on business keys
    if bk_cols:
        checks.append({"check_id": 7, "table": name, "columns": bk_cols})

    # Check 8 — Type conformance on monetary columns (should cast to DOUBLE)
    for c in monetary_cols:
        checks.append({
            "check_id": 8, "table": name, "column": c["column"],
            "extra": {"expected_type": "DOUBLE"},
        })

    # Check 12 — No negatives on monetary + must_be_positive numerics
    for c in monetary_cols:
        if c.get("must_be_positive", True):
            checks.append({"check_id": 12, "table": name, "column": c["column"]})
    for c in numeric_cols:
        if c.get("must_be_positive"):
            checks.append({"check_id": 12, "table": name, "column": c["column"]})

    # Check 17 — FK violation
    for fk in foreign_keys:
        checks.append({
            "check_id":       17,
            "child_table":    name,
            "child_column":   fk["column"],
            "parent_table":   fk["references_table"],
            "parent_column":  fk["references_column"],
        })

    # Check 18 — Orphan record check (same FKs, different metric)
    for fk in foreign_keys:
        checks.append({
            "check_id":       18,
            "child_table":    name,
            "child_column":   fk["column"],
            "parent_table":   fk["references_table"],
            "parent_column":  fk["references_column"],
        })

    # Check 19 — Join coverage (FK → reference)
    for fk in foreign_keys:
        checks.append({
            "check_id":          19,
            "source_table":      name,
            "source_column":     fk["column"],
            "reference_table":   fk["references_table"],
            "reference_column":  fk["references_column"],
        })

    # Check 28 — Outlier detection on all numeric + monetary columns
    for c in numeric_cols + monetary_cols:
        checks.append({"check_id": 28, "table": name, "column": c["column"]})

    # Check 29 — Distribution drift on numeric columns (self-baseline: mean drift)
    for c in numeric_cols:
        checks.append({"check_id": 29, "table": name, "column": c["column"]})

    # ── Category 2: Questionnaire-driven ────────────────────────────────────

    # Check 9 — Allowed values (categorical valid_values)
    for c in categorical_cols:
        vals = c.get("valid_values")
        if vals:
            checks.append({
                "check_id":       9,
                "table":          name,
                "column":         c["column"],
                "allowed_values": vals,
            })

    # Check 11 — Range validation (numeric min/max)
    for c in numeric_cols:
        if c.get("min") is not None or c.get("max") is not None:
            entry: dict = {"check_id": 11, "table": name, "column": c["column"]}
            if c.get("min") is not None:
                entry["min_value"] = c["min"]
            if c.get("max") is not None:
                entry["max_value"] = c["max"]
            checks.append(entry)

    # Check 15 — Currency/unit consistency
    for c in monetary_cols:
        ccy_col = c.get("currency_column")
        valid_ccy = c.get("valid_currencies") or []
        if ccy_col and valid_ccy:
            checks.append({
                "check_id":       15,
                "table":          name,
                "column":         ccy_col,
                "allowed_values": valid_ccy,
            })

    # Check 16 — Standardization (categorical with standardize=true)
    for c in categorical_cols:
        if c.get("standardize") and c.get("valid_values"):
            checks.append({
                "check_id":       16,
                "table":          name,
                "column":         c["column"],
                "allowed_values": c["valid_values"],
            })

    # Check 20 — Freshness lag
    for c in timestamp_cols:
        sla = c.get("freshness_sla_hours")
        if sla is not None:
            checks.append({
                "check_id":            20,
                "table":               name,
                "timestamp_column":    c["column"],
                "freshness_sla_hours": sla,
            })

    # Check 21 — Stale table flag
    for c in timestamp_cols:
        sla = c.get("freshness_sla_hours")
        if sla is not None:
            stale_days = round(sla / 24, 1)
            checks.append({
                "check_id":         21,
                "table":            name,
                "timestamp_column": c["column"],
                "stale_days":       stale_days,
            })

    # Check 22 — Late-arriving data
    for c in timestamp_cols:
        if c.get("is_event_time") and c.get("late_arrival_hours"):
            # We need a load timestamp column — look for a sibling timestamp
            other_ts = [
                o["column"] for o in timestamp_cols
                if o["column"] != c["column"]
            ]
            if other_ts:
                checks.append({
                    "check_id":                  22,
                    "table":                     name,
                    "event_timestamp_column":    c["column"],
                    "load_timestamp_column":     other_ts[0],
                    "late_arrival_hours":        c["late_arrival_hours"],
                })

    # Check 13 — Cross-field consistency (must_be_after)
    for c in timestamp_cols:
        after_col = c.get("must_be_after")
        if after_col:
            checks.append({
                "check_id":  13,
                "table":     name,
                "condition": f"{c['column']} < {after_col}",
            })

    # Check 4 — Conditional completeness
    for cc in cond_completeness:
        cond     = cc.get("condition")
        dep_col  = cc.get("column_must_not_be_null")
        if cond and dep_col:
            checks.append({
                "check_id":         4,
                "table":            name,
                "condition":        cond,
                "dependent_column": dep_col,
            })

    # Check 26 — Business rule validation
    for rule in business_rules:
        if isinstance(rule, str) and rule.strip():
            # Check 26 accepts a generic condition
            checks.append({
                "check_id":  26,
                "table":     name,
                "condition": rule,
            })

    # Check 23 — Volume change (requires prior_table)
    prior = volume.get("prior_table")
    change_pct = volume.get("max_daily_change_pct")
    if prior and change_pct is not None:
        checks.append({
            "check_id":           23,
            "today_table":        name,
            "prior_table":        prior,
            "volume_change_band": change_pct / 100.0,
        })

    return checks
