"""Gate decision — evaluates a PIIManifest and returns action + reason."""

from __future__ import annotations

from dqs.config.models import PIIManifest, ScreenerConfig


class PIIBlockError(Exception):
    """Raised when gate decision is BLOCK — scan must not proceed."""
    def __init__(self, manifest: PIIManifest) -> None:
        self.manifest = manifest
        super().__init__(
            f"PII BLOCK: {manifest.high_count} HIGH column(s) in '{manifest.table}'. "
            "Remediate before scanning."
        )


class PIIScreenerError(Exception):
    """Raised when the screener fails to run (connection issue, empty schema, etc.)."""


def evaluate_gate(manifest: PIIManifest, cfg: ScreenerConfig) -> tuple[str, str]:
    """Return (gate_decision, gate_reason)."""
    if manifest.high_count > 0:
        if cfg.on_pii_detected == "block":
            return (
                "BLOCK",
                f"{manifest.high_count} HIGH-confidence PII column(s) detected — scan blocked.",
            )
        if cfg.on_pii_detected == "synthetic":
            return (
                "SYNTHETIC_MODE",
                f"{manifest.high_count} HIGH-confidence PII column(s) — forcing synthetic mode.",
            )
        return (
            "PROCEED_EXCLUDE",
            f"{manifest.high_count} HIGH / {manifest.medium_count} MEDIUM column(s) — "
            "excluded from value-sampling checks.",
        )

    if manifest.medium_count > 0 or manifest.review_count > 0:
        return (
            "PROCEED_EXCLUDE",
            f"{manifest.medium_count} MEDIUM / {manifest.review_count} REVIEW column(s) flagged.",
        )

    return "CLEAR", "No PII detected — scan proceeds in full."
