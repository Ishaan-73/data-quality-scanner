"""Weighted scoring — aggregates CheckResults into a ScanReport."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import List

from dqs.config.defaults import DIMENSION_WEIGHTS
from dqs.config.models import CheckResult, DimensionResult, ScanReport


def compute_report(
    results: List[CheckResult],
    scan_name: str,
    mode: str,
    connector_dialect: str,
    scan_start: datetime,
    is_synthetic: bool = False,
) -> ScanReport:
    """
    Aggregate a flat list of CheckResults into a structured ScanReport.

    Overall quality score = Σ(weighted_score) × 100,
    where weighted_score = effective_weight × pass_score for each check.
    """
    scan_end = datetime.utcnow()
    duration = (scan_end - scan_start).total_seconds()

    # Group results by dimension
    by_dim: dict[str, List[CheckResult]] = defaultdict(list)
    for r in results:
        by_dim[r.dimension].append(r)

    dimension_results: List[DimensionResult] = []
    total_weighted_score = 0.0

    for dim, dim_checks in by_dim.items():
        dim_weight = DIMENSION_WEIGHTS.get(dim, 0.0)
        passed = sum(1 for c in dim_checks if c.passed and not c.skipped)
        failed = sum(1 for c in dim_checks if not c.passed and not c.skipped and not c.error)
        skipped = sum(1 for c in dim_checks if c.skipped)
        errored = sum(1 for c in dim_checks if c.error and not c.skipped)
        failed += errored

        # Dimension score (0–100): weighted average of pass scores within dimension
        dim_check_weight_sum = sum(c.effective_weight for c in dim_checks if not c.skipped)
        if dim_check_weight_sum > 0:
            dim_score = (
                sum(c.weighted_score for c in dim_checks if not c.skipped)
                / dim_check_weight_sum
            ) * 100
        else:
            dim_score = 100.0  # all skipped → neutral

        total_weighted_score += sum(c.weighted_score for c in dim_checks)

        dimension_results.append(
            DimensionResult(
                dimension=dim,
                dimension_weight=dim_weight,
                dimension_score=round(dim_score, 2),
                checks=dim_checks,
                checks_passed=passed,
                checks_failed=failed,
                checks_skipped=skipped,
            )
        )

    # Sort dimensions by weight descending for readability
    dimension_results.sort(key=lambda d: d.dimension_weight, reverse=True)

    overall_score = round(min(total_weighted_score * 100, 100.0), 2)
    total = len(results)
    passed_total = sum(1 for r in results if r.passed and not r.skipped)
    failed_total = sum(1 for r in results if not r.passed and not r.skipped)
    skipped_total = sum(1 for r in results if r.skipped)

    return ScanReport(
        scan_name=scan_name,
        mode=mode,
        connector_dialect=connector_dialect,
        overall_score=overall_score,
        dimensions=dimension_results,
        total_checks=total,
        checks_passed=passed_total,
        checks_failed=failed_total,
        checks_skipped=skipped_total,
        scan_start=scan_start,
        scan_end=scan_end,
        duration_seconds=round(duration, 2),
        is_synthetic=is_synthetic,
    )
