"""PII Screener — prerequisite pass before DQS checks."""

from __future__ import annotations

import uuid
from datetime import datetime

from dqs.config.models import PIIColumnResult, PIIManifest, ScanConfig, ScreenerConfig, SchemaProfile
from dqs.connectors.base import BaseConnector
from dqs.screener.gate import evaluate_gate
from dqs.screener.pass1 import column_name_scan
from dqs.screener.pass2 import value_sample_scan
from dqs.screener.pass3 import semantic_scan


def run_screener(
    config: ScanConfig,
    connector: BaseConnector,
    profiles: list[SchemaProfile],
) -> list[PIIManifest]:
    """
    Screen all tables for PII. Returns one PIIManifest per table.

    Caller is responsible for connector.connect() before and close() after.
    """
    cfg = config.screener or ScreenerConfig()
    return [_screen_table(profile, connector, cfg) for profile in profiles]


def _screen_table(
    profile: SchemaProfile,
    connector: BaseConnector,
    cfg: ScreenerConfig,
) -> PIIManifest:
    screened_at = datetime.utcnow()
    passes_run = [1]

    # Pass 1 — name heuristics only
    col_results = column_name_scan([profile])

    # Pass 2 — value sampling on non-CLEAR columns
    flagged = [r for r in col_results if r.status != "CLEAR"]
    if flagged:
        passes_run.append(2)
        flagged = value_sample_scan(connector, flagged, cfg.sample_size)
        flagged_map = {(r.table, r.column): r for r in flagged}
        col_results = [flagged_map.get((r.table, r.column), r) for r in col_results]

    # Pass 3 — semantic (opt-in)
    if cfg.enable_pass3:
        review = [r for r in col_results if r.status == "REVIEW"]
        if review:
            passes_run.append(3)
            updated = semantic_scan(connector, review, cfg.sample_size)
            updated_map = {(r.table, r.column): r for r in updated}
            col_results = [updated_map.get((r.table, r.column), r) for r in col_results]

    high   = sum(1 for r in col_results if r.status == "HIGH")
    medium = sum(1 for r in col_results if r.status == "MEDIUM")
    review = sum(1 for r in col_results if r.status == "REVIEW")
    clear  = sum(1 for r in col_results if r.status == "CLEAR")

    manifest = PIIManifest(
        scan_id=str(uuid.uuid4()),
        table=profile.table_name,
        row_count=profile.row_count,
        screened_at=screened_at,
        passes_run=passes_run,
        gate_decision="CLEAR",
        gate_reason="",
        columns=col_results,
        high_count=high,
        medium_count=medium,
        review_count=review,
        clear_count=clear,
    )

    gate_decision, gate_reason = evaluate_gate(manifest, cfg)
    manifest.gate_decision = gate_decision
    manifest.gate_reason = gate_reason
    return manifest
