"""Main scanner entry point — dispatches to Live or Synthetic mode."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import yaml

from dqs.config.models import ScanConfig, ScanReport, ScreenerConfig
from dqs.modes.live_scan import run_live_scan
from dqs.modes.synthetic_clone import run_synthetic_clone
from dqs.reporter import render


def scan_from_file(
    config_path: str,
    mode: Optional[Literal["live", "synthetic"]] = None,
    output_format: Literal["console", "json", "csv"] = "console",
    output_path: Optional[str] = None,
    mvp_only: bool = False,
    skip_screener: bool = False,
) -> ScanReport:
    raw = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    config = ScanConfig.model_validate(raw)

    if mode:
        config.mode = mode
    if mvp_only:
        config.mvp_only = True
    if skip_screener and config.screener:
        config.screener.enabled = False

    return scan(config, output_format=output_format, output_path=output_path)


def scan(
    config: ScanConfig,
    output_format: Literal["console", "json", "csv"] = "console",
    output_path: Optional[str] = None,
) -> ScanReport:
    """
    Execute a scan from a ScanConfig object.

    If screener is enabled, runs PII detection first and gates the scan.
    Dispatches to Mode A (live) or Mode B (synthetic) based on config.mode.
    """
    pii_manifests = []
    pii_excluded: set[tuple[str, str]] = set()

    screener_cfg = config.screener or ScreenerConfig()
    if config.screener and config.screener.enabled:
        pii_manifests, pii_excluded, config = _run_screener_phase(config, screener_cfg)

    if config.mode == "synthetic":
        report = run_synthetic_clone(config, pii_excluded=pii_excluded)
    else:
        report = run_live_scan(config, pii_excluded=pii_excluded)

    report.pii_manifests = pii_manifests
    render(report, output_format=output_format, output_path=output_path)
    return report


def _run_screener_phase(
    config: ScanConfig,
    screener_cfg: ScreenerConfig,
) -> tuple[list, set, ScanConfig]:
    """Connect, build schema profiles, run screener, return manifests + excluded columns."""
    from dqs.connectors import get_connector
    from dqs.screener import run_screener
    from dqs.screener.gate import PIIBlockError

    connector = get_connector(config.connector)
    connector.connect()
    try:
        profiles = _get_profiles(connector, config)
        manifests = run_screener(config, connector, profiles)
    finally:
        connector.close()

    # Worst-case gate across all tables
    _PRIORITY = {"BLOCK": 3, "SYNTHETIC_MODE": 2, "PROCEED_EXCLUDE": 1, "CLEAR": 0}
    worst = max(manifests, key=lambda m: _PRIORITY.get(m.gate_decision, 0), default=None)

    if worst and worst.gate_decision == "BLOCK":
        raise PIIBlockError(worst)
    if worst and worst.gate_decision == "SYNTHETIC_MODE":
        config.mode = "synthetic"

    pii_excluded = {
        (col.table, col.column)
        for m in manifests
        for col in m.columns
        if col.status in ("HIGH", "MEDIUM")
    }
    return manifests, pii_excluded, config


def _get_profiles(connector, config: ScanConfig) -> list:
    """Get lightweight schema profiles (column names + types only) for screener."""
    from dqs.config.models import SchemaProfile, SchemaColumnProfile

    # Collect unique tables from all check configs
    tables: set[str] = set()
    for chk in config.checks:
        for field in ("table", "child_table", "parent_table", "source_table",
                      "reference_table", "target_table", "today_table", "prior_table"):
            val = getattr(chk, field, None)
            if val:
                tables.add(val)

    profiles = []
    for table in sorted(tables):
        try:
            col_info = connector._get_column_info(table)
            row_count = connector.get_table_rowcount(table)
            columns = [
                SchemaColumnProfile(name=c, dtype=dt, python_dtype="str")
                for c, dt in col_info
            ]
            profiles.append(SchemaProfile(
                table_name=table,
                row_count=row_count,
                columns=columns,
            ))
        except Exception:
            pass  # table unreachable — skip silently

    return profiles
