"""Abstract base class for all quality checks."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional, Tuple

from dqs.config.defaults import DEFAULT_THRESHOLDS, effective_weight
from dqs.config.models import CheckConfig, CheckResult
from dqs.connectors.base import BaseConnector


class BaseCheck(ABC):
    """
    Every quality check subclasses this.

    Subclasses must set:
        check_id: int
        name: str
        dimension: str
        metric_name: str

    And implement:
        _build_sql(config) -> str
        _extract_metric(df) -> Optional[float]
    """

    check_id: int
    name: str
    dimension: str
    metric_name: str

    # Direction: "lower_is_better" (most checks) or "higher_is_better" (e.g. join coverage)
    direction: str = "lower_is_better"

    def run(self, connector: BaseConnector, config: CheckConfig) -> CheckResult:
        """Execute the check and return a structured result."""
        threshold = config.threshold if config.threshold is not None else DEFAULT_THRESHOLDS.get(self.check_id)
        weight = effective_weight(self.check_id, self.dimension)

        base = CheckResult(
            check_id=self.check_id,
            check_name=self.name,
            dimension=self.dimension,
            metric_name=self.metric_name,
            threshold=threshold,
            effective_weight=weight,
            table=config.table,
            column=config.column,
        )

        if not config.enabled:
            base.skipped = True
            base.pass_score = 1.0  # skipped checks don't penalise score
            base.weighted_score = weight * 1.0
            return base

        # Advisory checks (threshold = None) are always "pass" with full score
        if threshold is None:
            try:
                sql = self._build_sql(config)
                df = connector.execute_query(sql)
                metric = self._extract_metric(df)
                base.metric_value = metric
                base.executed_sql = sql
            except Exception as exc:
                base.error = str(exc)
            base.passed = True
            base.pass_score = 1.0
            base.weighted_score = weight * 1.0
            return base

        try:
            sql = self._build_sql(config)
            base.executed_sql = sql
            df = connector.execute_query(sql)
            metric = self._extract_metric(df)
            base.metric_value = metric

            if metric is None:
                base.skipped = True
                base.pass_score = 1.0
                base.weighted_score = weight * 1.0
                return base

            passed, pass_score = self._evaluate(metric, threshold)
            base.passed = passed
            base.pass_score = pass_score
            base.weighted_score = weight * pass_score

        except Exception as exc:
            base.error = str(exc)
            base.passed = False
            base.pass_score = 0.0
            base.weighted_score = 0.0

        return base

    @abstractmethod
    def _build_sql(self, config: CheckConfig) -> str:
        """Return the SQL string that computes the quality metric."""

    @abstractmethod
    def _extract_metric(self, df) -> Optional[float]:
        """Pull the scalar metric value from the query result DataFrame."""

    def _evaluate(self, metric: float, threshold: float) -> Tuple[bool, float]:
        """
        Return (passed: bool, pass_score: float ∈ [0, 1]).

        For lower_is_better checks: passes when metric <= threshold.
        For higher_is_better checks: passes when metric >= threshold.
        Partial credit via proximity to threshold.
        """
        if self.direction == "higher_is_better":
            passed = metric >= threshold
            if passed:
                return True, 1.0
            # How far below threshold?  score decays linearly.
            denom = max(threshold, 1e-9)
            pass_score = max(0.0, metric / denom)
            return False, pass_score
        else:
            passed = metric <= threshold
            if passed:
                return True, 1.0
            # How far above threshold?  score decays linearly.
            denom = max(threshold, 1e-9)
            pass_score = max(0.0, 1.0 - (metric - threshold) / denom)
            return False, pass_score


def _ratio_metric(df) -> Optional[float]:
    """Helper: return first numeric value in first cell, or None."""
    if df is None or df.empty:
        return None
    val = df.iloc[0, 0]
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
