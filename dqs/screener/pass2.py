"""Pass 2 — value sampling + regex matching (one query per flagged column)."""

from __future__ import annotations

from dqs.config.models import PIIColumnResult
from dqs.connectors.base import BaseConnector
from dqs.screener.masker import mask_value
from dqs.screener.patterns import REGEX_PATTERNS


def value_sample_scan(
    connector: BaseConnector,
    columns: list[PIIColumnResult],
    sample_size: int = 100,
) -> list[PIIColumnResult]:
    """Refine confidence for HIGH/MEDIUM columns using sampled values."""
    updated: list[PIIColumnResult] = []

    for col in columns:
        if col.status == "CLEAR":
            updated.append(col)
            continue

        # Pull sample
        try:
            df = connector.execute_query(
                f"SELECT {col.column} FROM {col.table} "
                f"WHERE {col.column} IS NOT NULL LIMIT {sample_size}"
            )
            samples = [str(v) for v in df.iloc[:, 0].tolist() if v is not None]
        except Exception:
            updated.append(col)
            continue

        if not samples:
            updated.append(col)
            continue

        col.sample_count = len(samples)

        # Find best-matching regex
        best_ratio = 0.0
        best_cat = col.category
        best_type = col.pii_type
        best_matches = 0

        for _name, (pattern, category, pii_type) in REGEX_PATTERNS.items():
            hits = sum(1 for s in samples if pattern.match(s))
            ratio = hits / len(samples)
            if ratio > best_ratio:
                best_ratio = ratio
                best_cat = category
                best_type = pii_type
                best_matches = hits

        col.match_count = best_matches

        # Upgrade / downgrade status
        via_suffix = "+regex" if best_ratio > 0 else ""
        if best_ratio >= 0.8:
            col.status = "HIGH"
            col.confidence = min(0.99, 0.5 + best_ratio * 0.5)
            col.category = best_cat
            col.pii_type = best_type
            col.detected_via = col.detected_via + via_suffix
        elif best_ratio >= 0.4:
            if col.status != "HIGH":
                col.status = "MEDIUM"
            col.detected_via = col.detected_via + via_suffix
        elif col.status == "MEDIUM":
            # Regex didn't confirm → downgrade to REVIEW
            col.status = "REVIEW"
            col.detected_via = col.detected_via + "+no_regex"

        # Masked samples (≤3, never raw)
        col.masked_samples = [
            mask_value(s, col.category or "other") for s in samples[:3]
        ]

        updated.append(col)

    return updated
