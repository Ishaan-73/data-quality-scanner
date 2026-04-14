"""Output formatters for ScanReport — console (Rich), JSON, CSV."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Literal, Optional

from dqs.config.models import ScanReport


def render(
    report: ScanReport,
    output_format: Literal["console", "json", "csv"] = "console",
    output_path: Optional[str] = None,
) -> str:
    """
    Render a ScanReport in the requested format.

    Returns the rendered string. If output_path is given, also writes to that file.
    """
    if output_format == "json":
        content = _to_json(report)
    elif output_format == "csv":
        content = _to_csv(report)
    else:
        _print_console(report)
        return ""

    if output_path:
        Path(output_path).write_text(content, encoding="utf-8")

    return content


# ---------------------------------------------------------------------------
# Console output (Rich)
# ---------------------------------------------------------------------------

def _print_console(report: ScanReport) -> None:
    try:
        from rich.console import Console
        from rich.panel import Panel
        from rich.table import Table
        from rich import box
    except ImportError:
        # Fallback to plain text
        print(_plain_text(report))
        return

    console = Console(force_terminal=True)

    # PII manifest banner (printed first, before DQS results)
    if report.pii_manifests:
        _print_pii_banner(console, report.pii_manifests)

    # Header panel
    mode_label = "[bold cyan]SYNTHETIC CLONE[/]" if report.is_synthetic else "[bold green]LIVE SCAN[/]"
    score_color = _score_color(report.overall_score)
    console.print(
        Panel(
            f"[bold]{report.scan_name}[/]  |  Mode: {mode_label}  |  "
            f"Connector: [yellow]{report.connector_dialect}[/]\n"
            f"Overall Quality Score: [{score_color}]{report.overall_score:.1f}/100[/]  |  "
            f"Checks: [green]{report.checks_passed}✓[/] [red]{report.checks_failed}✗[/] "
            f"[dim]{report.checks_skipped} skipped[/]  |  "
            f"Duration: {report.duration_seconds:.1f}s",
            title="[bold]Data Quality Scanner[/]",
            border_style="blue",
        )
    )

    # Dimension summary table
    dim_table = Table(
        "Dimension", "Weight", "Score", "Passed", "Failed", "Skipped",
        box=box.ROUNDED,
        title="Dimension Summary",
        show_header=True,
        header_style="bold magenta",
    )
    for dim in report.dimensions:
        score_str = f"[{_score_color(dim.dimension_score)}]{dim.dimension_score:.1f}[/]"
        dim_table.add_row(
            dim.dimension.replace("_", " ").title(),
            f"{dim.dimension_weight:.0%}",
            score_str,
            str(dim.checks_passed),
            str(dim.checks_failed),
            str(dim.checks_skipped),
        )
    console.print(dim_table)

    # Failed checks detail
    failed_checks = [
        c for dim in report.dimensions for c in dim.checks
        if not c.passed and not c.skipped
    ]
    if failed_checks:
        fail_table = Table(
            "ID", "Check", "Metric", "Value", "Threshold", "Score",
            box=box.SIMPLE,
            title="[bold red]Failed Checks[/]",
            header_style="bold red",
        )
        for c in failed_checks:
            metric_str = f"{c.metric_value:.4f}" if c.metric_value is not None else "N/A"
            thresh_str = f"{c.threshold:.4f}" if c.threshold is not None else "N/A"
            error_note = f"[red]ERROR: {c.error[:50]}[/]" if c.error else metric_str
            fail_table.add_row(
                str(c.check_id),
                c.check_name,
                c.metric_name,
                error_note,
                thresh_str,
                f"{c.pass_score:.2f}",
            )
        console.print(fail_table)
    else:
        console.print("[bold green]All checks passed![/]")

    if report.is_synthetic:
        console.print(
            "[dim italic]Note: Results based on synthetic clone — "
            "no production data was accessed during checks.[/]"
        )


def _print_pii_banner(console, manifests) -> None:
    from rich.table import Table
    from rich import box

    total_high   = sum(m.high_count   for m in manifests)
    total_medium = sum(m.medium_count for m in manifests)
    total_review = sum(m.review_count for m in manifests)

    # Collect worst gate
    _PRIORITY = {"BLOCK": 3, "SYNTHETIC_MODE": 2, "PROCEED_EXCLUDE": 1, "CLEAR": 0}
    worst = max(manifests, key=lambda m: _PRIORITY.get(m.gate_decision, 0))

    gate_color = {
        "BLOCK": "bold red", "SYNTHETIC_MODE": "bold yellow",
        "PROCEED_EXCLUDE": "yellow", "CLEAR": "green",
    }.get(worst.gate_decision, "white")

    console.print(
        f"[{gate_color}]■ PII SCREENER — {worst.gate_decision}[/]  "
        f"HIGH: [red]{total_high}[/]  MEDIUM: [yellow]{total_medium}[/]  "
        f"REVIEW: [dim]{total_review}[/]  "
        f"Tables screened: {len(manifests)}"
    )

    flagged = [
        col for m in manifests for col in m.columns
        if col.status in ("HIGH", "MEDIUM")
    ]
    if flagged:
        tbl = Table(
            "Table", "Column", "Status", "Type", "Via", "Samples",
            box=box.SIMPLE, header_style="bold yellow", show_header=True,
        )
        for col in flagged:
            status_color = "red" if col.status == "HIGH" else "yellow"
            tbl.add_row(
                col.table, col.column,
                f"[{status_color}]{col.status}[/]",
                col.pii_type or "—",
                col.detected_via,
                "  ".join(col.masked_samples[:2]) or "—",
            )
        console.print(tbl)

    console.rule()


def _score_color(score: float) -> str:
    if score >= 90:
        return "green"
    if score >= 70:
        return "yellow"
    return "red"


def _plain_text(report: ScanReport) -> str:
    lines = [
        f"=== {report.scan_name} ===",
        f"Mode: {report.mode}  Connector: {report.connector_dialect}",
        f"Overall Score: {report.overall_score:.1f}/100",
        f"Checks: {report.checks_passed} passed, {report.checks_failed} failed, "
        f"{report.checks_skipped} skipped",
        "",
        "Dimension Scores:",
    ]
    for dim in report.dimensions:
        lines.append(
            f"  {dim.dimension:20s}  {dim.dimension_score:5.1f}/100  "
            f"(w={dim.dimension_weight:.0%})"
        )
    failed = [c for d in report.dimensions for c in d.checks if not c.passed and not c.skipped]
    if failed:
        lines += ["", "Failed Checks:"]
        for c in failed:
            val = f"{c.metric_value:.4f}" if c.metric_value is not None else "N/A"
            thr = f"{c.threshold:.4f}" if c.threshold is not None else "N/A"
            lines.append(
                f"  [{c.check_id:2d}] {c.check_name:40s}  value={val}  threshold={thr}"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

def _to_json(report: ScanReport) -> str:
    return report.model_dump_json(indent=2, exclude_none=False)


# ---------------------------------------------------------------------------
# CSV output (one row per check)
# ---------------------------------------------------------------------------

def _to_csv(report: ScanReport) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "scan_name", "mode", "connector", "overall_score",
        "dimension", "dimension_weight", "dimension_score",
        "check_id", "check_name", "metric_name",
        "metric_value", "threshold", "passed", "pass_score",
        "effective_weight", "weighted_score",
        "table", "column", "error", "skipped", "timestamp",
    ])
    for dim in report.dimensions:
        for c in dim.checks:
            writer.writerow([
                report.scan_name,
                report.mode,
                report.connector_dialect,
                report.overall_score,
                dim.dimension,
                dim.dimension_weight,
                dim.dimension_score,
                c.check_id,
                c.check_name,
                c.metric_name,
                c.metric_value,
                c.threshold,
                c.passed,
                c.pass_score,
                c.effective_weight,
                c.weighted_score,
                c.table,
                c.column,
                c.error,
                c.skipped,
                c.timestamp.isoformat(),
            ])
    return buf.getvalue()
