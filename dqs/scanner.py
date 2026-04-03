"""Main scanner entry point — dispatches to Live or Synthetic mode."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import yaml

from dqs.config.models import ScanConfig, ScanReport
from dqs.modes.live_scan import run_live_scan
from dqs.modes.synthetic_clone import run_synthetic_clone
from dqs.reporter import render


def scan_from_file(
    config_path: str,
    mode: Optional[Literal["live", "synthetic"]] = None,
    output_format: Literal["console", "json", "csv"] = "console",
    output_path: Optional[str] = None,
    mvp_only: bool = False,
) -> ScanReport:
    """
    Load a scan config from YAML and execute the scan.

    Args:
        config_path:   Path to the scan YAML configuration file.
        mode:          Override the mode from the YAML ('live' or 'synthetic').
        output_format: Render format ('console', 'json', 'csv').
        output_path:   If set, write rendered output to this file path.
        mvp_only:      Run only MVP-phase checks.

    Returns:
        A ScanReport with all results and scores.
    """
    raw = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    config = ScanConfig.model_validate(raw)

    if mode:
        config.mode = mode
    if mvp_only:
        config.mvp_only = True

    return scan(config, output_format=output_format, output_path=output_path)


def scan(
    config: ScanConfig,
    output_format: Literal["console", "json", "csv"] = "console",
    output_path: Optional[str] = None,
) -> ScanReport:
    """
    Execute a scan from a ScanConfig object.

    Dispatches to Mode A (live) or Mode B (synthetic) based on config.mode.
    """
    if config.mode == "synthetic":
        report = run_synthetic_clone(config)
    else:
        report = run_live_scan(config)

    render(report, output_format=output_format, output_path=output_path)
    return report
