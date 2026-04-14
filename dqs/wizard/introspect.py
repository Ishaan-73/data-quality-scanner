"""
Schema introspection → questionnaire template.

Given a connector and list of tables, profiles each table and produces
a questionnaire.yaml pre-filled with auto-discovered values.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set

from dqs.config.models import SchemaColumnProfile, SchemaProfile

# ── Column classification heuristics ───────────────────────────────────────

_MONETARY_KW = {
    "price", "cost", "amount", "revenue", "fee", "tax", "discount",
    "paid", "profit", "wage", "salary", "charge", "spend", "budget",
}
_TS_KW = {
    "_at", "_date", "_time", "_ts", "_timestamp",
    "created", "updated", "modified", "loaded", "processed", "ingested",
}
_PK_SK = re.compile(r"(_sk|_key|_pk)$", re.IGNORECASE)
_BK_ID = re.compile(r"(_id|_code|_num|_no|_nbr)$", re.IGNORECASE)

_CATEGORICAL_MAX_UNIQUE = 50     # absolute ceiling
_CATEGORICAL_MAX_RATIO  = 0.02   # unique / row_count ceiling

# Column name patterns that strongly signal numeric (regardless of reported dtype)
_NUMERIC_NAME_KW = {
    "price", "cost", "amount", "revenue", "fee", "tax", "discount",
    "paid", "profit", "wage", "salary", "charge", "spend", "budget",
    "qty", "quantity", "count", "cnt", "num", "nbr", "total",
    "rate", "ratio", "pct", "percent", "score", "weight", "size",
    "width", "height", "length", "age", "year", "month", "day",
    "hour", "minute", "second", "bound",
}


def _is_numeric_dtype(col: SchemaColumnProfile) -> bool:
    """True when the column is definitively numeric (int or float python_dtype)."""
    return col.python_dtype in ("int", "float")


def _looks_numeric_by_name(col: SchemaColumnProfile) -> bool:
    """True when the column name strongly implies a numeric value."""
    n = col.name.lower()
    return any(kw in n for kw in _NUMERIC_NAME_KW)


def _categorize(col: SchemaColumnProfile, row_count: int) -> Set[str]:
    cats: Set[str] = set()
    n = col.name.lower()

    numeric_dtype = _is_numeric_dtype(col)
    # When dtype resolution failed (python_dtype='str'), fall back to name heuristics
    likely_numeric = numeric_dtype or (col.python_dtype == "str" and _looks_numeric_by_name(col))
    # SK columns are almost always integer surrogate keys even when dtype unknown
    likely_int = numeric_dtype or (col.python_dtype == "str" and _PK_SK.search(n))

    # PK / surrogate key
    if _PK_SK.search(n) and likely_int:
        cats.add("pk")
    # Business / natural key — string ID columns
    elif _BK_ID.search(n) and not likely_numeric:
        cats.add("bk")

    # Monetary
    if likely_numeric and any(kw in n for kw in _MONETARY_KW):
        cats.add("monetary")

    # Timestamp (dtype-driven or name-driven; exclude _sk date keys)
    if col.python_dtype == "datetime" or (
        any(kw in n for kw in _TS_KW) and "pk" not in cats
    ):
        cats.add("timestamp")

    # Categorical — low-cardinality string (exclude numeric/pk/bk columns)
    if not likely_numeric and "pk" not in cats and "bk" not in cats and "timestamp" not in cats:
        uc = col.unique_count or 0
        ratio = uc / max(row_count, 1)
        if 0 < uc <= _CATEGORICAL_MAX_UNIQUE or (uc > 0 and ratio <= _CATEGORICAL_MAX_RATIO):
            cats.add("categorical")

    # Generic numeric (not monetary or pk)
    if likely_numeric and "monetary" not in cats and "pk" not in cats:
        cats.add("numeric")

    return cats


# ── Public entry point ──────────────────────────────────────────────────────

def build_questionnaire(
    connector_cfg: dict,
    profiles: Dict[str, SchemaProfile],
    scan_name: str = "my_dqs_scan",
) -> dict:
    """
    Convert a dict of SchemaProfiles into a questionnaire YAML structure.

    Returns a plain dict (caller serialises to YAML).
    """
    tables = []
    for table_name, profile in profiles.items():
        tables.append(_build_table_entry(table_name, profile))

    result = {
        "_instructions": (
            "Fill in every field marked '# TODO'. "
            "Leave null/[] where you have no information. "
            "Then run:  dqs generate-config --questionnaire <this_file> --out scan_config.yaml"
        ),
        "meta": {
            "scan_name": scan_name,
            "connector": connector_cfg,
        },
        "tables": tables,
    }
    return _sanitize(result)


# ── Per-table builder ───────────────────────────────────────────────────────

def _build_table_entry(table_name: str, profile: SchemaProfile) -> dict:
    categorical: List[dict] = []
    numeric:     List[dict] = []
    timestamps:  List[dict] = []
    id_cols:     List[dict] = []
    monetary:    List[dict] = []
    auto_fks:    List[dict] = []          # placeholder; connectors rarely expose FKs

    for col in profile.columns:
        cats = _categorize(col, profile.row_count)

        # ID columns
        if "pk" in cats:
            id_cols.append({"column": col.name, "is_primary_key": True})
        elif "bk" in cats:
            id_cols.append({"column": col.name, "is_business_key": True})

        # Monetary
        if "monetary" in cats:
            monetary.append({
                "column": col.name,
                "must_be_positive": True,
                # currency column name if a sibling column looks like a currency code
                "currency_column": _guess_currency_column(col.name, profile),
                "valid_currencies": [],   # TODO: e.g. [USD, EUR, GBP]
            })

        # Categorical
        if "categorical" in cats:
            top = list((col.top_values or {}).keys())[:20]
            categorical.append({
                "column": col.name,
                # pre-fill from sampled top values; client should verify / trim
                "valid_values": top if top else None,   # None = TODO
                "standardize": False,
            })

        # Timestamp
        if "timestamp" in cats:
            timestamps.append({
                "column": col.name,
                "freshness_sla_hours": None,    # TODO
                "is_event_time": False,
                "late_arrival_hours": None,     # TODO (only if is_event_time=true)
                "must_be_after": None,          # TODO: column name, e.g. created_at
            })

        # Generic numeric
        if "numeric" in cats:
            numeric.append({
                "column": col.name,
                "min": _safe_float(col.min_value),
                "max": _safe_float(col.max_value),
                "must_be_positive": False,      # flip to true if client confirms
            })

    return {
        "name": table_name,
        # What role does this table play in the AI/ML pipeline?
        # Options: training_data | feature_store | target_variable | reference | operational
        "ai_role": "unknown",              # TODO

        "id_columns":          id_cols,
        "monetary_columns":    monetary,
        "categorical_columns": categorical,
        "numeric_columns":     numeric,
        "timestamp_columns":   timestamps,

        # Referential integrity — fill in FK relationships
        # Example: {column: customer_sk, references_table: customers, references_column: customer_sk}
        "foreign_keys": [],                # TODO

        # SQL conditions that must always be TRUE
        # Example: "net_paid = sales_price * quantity"
        # Example: "end_date >= start_date"
        "business_rules": [],              # TODO

        # "When condition X, column Y must not be null"
        # Example: {condition: "status = 'shipped'", column_must_not_be_null: shipped_at}
        "conditional_completeness": [],    # TODO

        "volume": {
            # Alert if row count changes more than this % day-over-day
            "max_daily_change_pct": 30,    # TODO: adjust
            # Name of yesterday's snapshot table (leave null if unavailable)
            "prior_table": None,           # TODO
        },
    }


# ── Helpers ─────────────────────────────────────────────────────────────────

def _safe_float(v: Any) -> Optional[float]:
    """Convert profile min/max to a plain float, ignoring Timestamps and other non-numerics."""
    if v is None:
        return None
    # Reject datetime-like objects (pandas Timestamp, datetime, date)
    try:
        import pandas as pd
        if isinstance(v, (pd.Timestamp,)):
            return None
    except ImportError:
        pass
    import datetime
    if isinstance(v, (datetime.datetime, datetime.date)):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _sanitize(obj: Any) -> Any:
    """Recursively convert any non-YAML-safe objects to plain Python types."""
    import datetime
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    try:
        import pandas as pd
        if isinstance(obj, pd.Timestamp):
            return str(obj)
    except ImportError:
        pass
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(i) for i in obj]
    # Fallback: stringify anything else
    try:
        return float(obj)
    except (TypeError, ValueError):
        return str(obj)


def _guess_currency_column(monetary_col: str, profile: SchemaProfile) -> Optional[str]:
    """
    If a sibling column name contains 'currency' or 'curr_cd', return its name.
    """
    for col in profile.columns:
        n = col.name.lower()
        if "currency" in n or "curr_cd" in n or "ccy" in n:
            return col.name
    return None
