"""
Mode A — Live Scan

Direct read-only connection to the target data warehouse.
Runs all enabled checks against production/live data.
Fastest path to a quality score.
"""

from __future__ import annotations

from datetime import datetime
from typing import List

from dqs.checks import CHECK_REGISTRY
from dqs.config.defaults import MVP_PHASE
from dqs.config.models import CheckConfig, CheckResult, ScanConfig
from dqs.connectors import get_connector
from dqs.scorer import compute_report
from dqs.config.models import ScanReport


def run_live_scan(config: ScanConfig) -> ScanReport:
    """
    Execute a live scan against the configured data warehouse.

    1. Connect to the DW via config.connector.
    2. For each enabled CheckConfig in config.checks, run the corresponding check.
    3. Score and return a ScanReport.
    """
    scan_start = datetime.utcnow()
    results: List[CheckResult] = []

    connector = get_connector(config.connector)
    connector.connect()

    try:
        check_configs = _filter_checks(config)

        for check_cfg in check_configs:
            check = CHECK_REGISTRY.get(check_cfg.check_id)
            if check is None:
                continue
            # Skip checks for dimensions not in the requested dimension filter
            if config.dimensions and check.dimension not in config.dimensions:
                continue
            result = check.run(connector, check_cfg)
            results.append(result)

    finally:
        connector.close()

    return compute_report(
        results=results,
        scan_name=config.scan_name,
        mode="live",
        connector_dialect=config.connector.dialect,
        scan_start=scan_start,
        is_synthetic=False,
    )


def _filter_checks(config: ScanConfig) -> List[CheckConfig]:
    """Apply mvp_only filter and return the active check list."""
    checks = config.checks
    if config.mvp_only:
        checks = [c for c in checks if MVP_PHASE.get(c.check_id) == "mvp"]
    return checks
