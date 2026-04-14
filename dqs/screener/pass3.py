"""Pass 3 — semantic / NER scan (opt-in, REVIEW columns only)."""

from __future__ import annotations

from dqs.config.models import PIIColumnResult
from dqs.connectors.base import BaseConnector

_PII_ENTITIES = {"PERSON", "GPE", "LOC", "ORG", "DATE", "CARDINAL"}


def semantic_scan(
    connector: BaseConnector,
    columns: list[PIIColumnResult],
    sample_size: int = 100,
) -> list[PIIColumnResult]:
    """Use spacy NER to resolve REVIEW columns. Silently skips if spacy unavailable."""
    try:
        import spacy
        nlp = spacy.load("en_core_web_sm")
    except (ImportError, OSError):
        return columns

    updated: list[PIIColumnResult] = []
    for col in columns:
        if col.status != "REVIEW":
            updated.append(col)
            continue

        try:
            df = connector.execute_query(
                f"SELECT {col.column} FROM {col.table} "
                f"WHERE {col.column} IS NOT NULL LIMIT {sample_size}"
            )
            samples = [str(v) for v in df.iloc[:, 0].tolist() if v is not None]
        except Exception:
            updated.append(col)
            continue

        hits = sum(
            1 for text in samples
            if any(ent.label_ in _PII_ENTITIES for ent in nlp(text).ents)
        )

        if hits > 0:
            col.status = "HIGH"
            col.confidence = min(0.99, hits / max(len(samples), 1))
            col.detected_via = "semantic"
        else:
            col.status = "CLEAR"

        updated.append(col)

    return updated
