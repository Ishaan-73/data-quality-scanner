"""Pass 1 — column name heuristics. Zero queries fired."""

from __future__ import annotations

from dqs.config.models import PIIColumnResult, SchemaProfile
from dqs.screener.patterns import HIGH_KEYWORDS, MEDIUM_KEYWORDS, KEYWORD_CATEGORY


def column_name_scan(profiles: list[SchemaProfile]) -> list[PIIColumnResult]:
    """Classify every column in every profile using name matching only."""
    results: list[PIIColumnResult] = []
    for profile in profiles:
        for col in profile.columns:
            results.append(_classify(profile.table_name, col.name))
    return results


def _classify(table: str, col_name: str) -> PIIColumnResult:
    col_lower = col_name.lower()

    # Exact match → HIGH
    if col_lower in HIGH_KEYWORDS:
        cat, pii_type = KEYWORD_CATEGORY.get(col_lower, ("indirect", col_lower))
        return PIIColumnResult(
            column=col_name, table=table, status="HIGH",
            category=cat, pii_type=pii_type,
            confidence=0.95, detected_via="name_match",
        )

    # Substring match → MEDIUM
    for kw in MEDIUM_KEYWORDS:
        if kw in col_lower:
            cat, pii_type = KEYWORD_CATEGORY.get(kw, ("indirect", kw))
            return PIIColumnResult(
                column=col_name, table=table, status="MEDIUM",
                category=cat, pii_type=pii_type,
                confidence=0.5, detected_via="name_match",
            )

    return PIIColumnResult(
        column=col_name, table=table, status="CLEAR",
        confidence=0.0, detected_via="none",
    )
