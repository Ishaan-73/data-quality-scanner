"""
Research-based weights and default thresholds for data quality checks.

Weight methodology
------------------
Two-level system:

  Level 1 — DIMENSION_WEIGHTS
    Importance of each quality dimension relative to all others.
    Sourced from: DAMA-DMBOK2, ISO 25012, Gartner DQ research.
    Sum = 1.00.

  Level 2 — CHECK_WITHIN_DIM_WEIGHTS
    Relative importance of each check *within* its dimension.
    Sum = 1.00 per dimension.

  Effective weight per check = DIMENSION_WEIGHTS[dim] × CHECK_WITHIN_DIM_WEIGHTS[dim][check_id]
  Sum of all effective weights = 1.00.

Rationale for dimension ordering:
  completeness > uniqueness/validity  — missing data blocks all downstream analysis
  uniqueness/validity ~= each other   — duplicates corrupt aggregations; invalid types break pipelines
  integrity > accuracy > freshness    — FK violations destroy join trustworthiness
  consistency < integrity             — requires extra cross-table mapping to validate
  volume/anomaly/schema/...           — useful signals but not core correctness dimensions
"""

# ---------------------------------------------------------------------------
# Level 1 — Dimension weights (sum to 1.00)
# ---------------------------------------------------------------------------
DIMENSION_WEIGHTS: dict[str, float] = {
    "completeness":  0.20,
    "uniqueness":    0.14,
    "validity":      0.14,
    "integrity":     0.12,
    "accuracy":      0.10,
    "freshness":     0.09,
    "consistency":   0.08,
    "volume":        0.05,
    "anomaly":       0.03,
    "schema":        0.02,
    "pipeline":      0.01,
    "time_series":   0.01,
    "metadata":      0.01,
}

assert abs(sum(DIMENSION_WEIGHTS.values()) - 1.0) < 1e-9, "Dimension weights must sum to 1.0"

# ---------------------------------------------------------------------------
# Level 2 — Check weights within each dimension (each sub-dict sums to 1.00)
# Keys are check_ids (int) matching the spec catalog.
# ---------------------------------------------------------------------------
CHECK_WITHIN_DIM_WEIGHTS: dict[str, dict[int, float]] = {
    "completeness": {
        # Critical null columns carry 2× the signal vs. general null check
        2: 0.40,   # Null Ratio in Critical Columns
        1: 0.30,   # Null Ratio by Column
        4: 0.20,   # Conditional Completeness
        3: 0.10,   # Empty Row Check
    },
    "uniqueness": {
        5: 0.55,   # Primary Key Duplicate Ratio — zero-tolerance
        6: 0.35,   # Business Key Duplicate Ratio
        7: 0.10,   # Near-Duplicate Candidate Check — advisory/review
    },
    "validity": {
        8:  0.30,  # Data Type Conformance — foundational for pipelines
        9:  0.25,  # Allowed Value / Domain Check — zero-tolerance
        10: 0.20,  # Format Validation — email/phone/postcode
        11: 0.15,  # Range Validation — numeric/date bounds
        12: 0.10,  # Negative Value Check — non-negative measures
    },
    "integrity": {
        17: 0.50,  # Foreign Key Violation — breaks join logic entirely
        18: 0.35,  # Orphan Record Check — unlinked master/dimension rows
        19: 0.15,  # Broken Join Coverage — join success rate
    },
    "accuracy": {
        25: 0.45,  # Source-to-Target Reconciliation — highest evidence of accuracy
        26: 0.35,  # Business Rule Validation — revenue = price × qty
        27: 0.20,  # Reference Data Validation — master data alignment
    },
    "freshness": {
        20: 0.50,  # Freshness Lag Hours — primary SLA metric
        21: 0.30,  # Stale Table Check — binary flag for dead tables
        22: 0.20,  # Late-Arriving Data Check
    },
    "consistency": {
        13: 0.35,  # Cross-Field Consistency — same-row logic (end > start)
        14: 0.35,  # Cross-Table Consistency — same entity across tables
        15: 0.20,  # Unit/Currency Consistency — measurement scale mix
        16: 0.10,  # Standardization Check — label normalisation
    },
    "volume": {
        23: 0.65,  # Row Count Volume Change — leading pipeline failure indicator
        24: 0.35,  # Missing Partition / Date Slice — partition gap
    },
    "anomaly": {
        28: 0.60,  # Outlier Detection — Z-score / IQR extremes
        29: 0.40,  # Distribution Drift Check — PSI / KS shift
    },
    "schema": {
        30: 1.00,  # Schema Drift Detection
    },
    "metadata": {
        31: 1.00,  # Metadata Completeness
    },
    "pipeline": {
        32: 1.00,  # Pipeline Failure Indicator
    },
    "time_series": {
        33: 1.00,  # Timestamp Sequence Integrity
    },
}

# Validate within-dim weights sum to 1.0 per dimension
for _dim, _weights in CHECK_WITHIN_DIM_WEIGHTS.items():
    _total = sum(_weights.values())
    assert abs(_total - 1.0) < 1e-9, f"Check weights for '{_dim}' sum to {_total}, expected 1.0"


def effective_weight(check_id: int, dimension: str) -> float:
    """Return the effective weight for a check = dim_weight × within_dim_weight."""
    return DIMENSION_WEIGHTS[dimension] * CHECK_WITHIN_DIM_WEIGHTS[dimension][check_id]


# ---------------------------------------------------------------------------
# Default thresholds (used when CheckConfig.threshold is not overridden)
# ---------------------------------------------------------------------------
DEFAULT_THRESHOLDS: dict[int, float | None] = {
    1:  0.05,    # Null Ratio by Column <= 5%
    2:  0.01,    # Null Ratio in Critical Columns <= 1%
    3:  0.0,     # Empty Row Count = 0
    4:  0.01,    # Conditional Completeness <= 1%
    5:  0.0,     # PK Duplicate Ratio = 0 (zero tolerance)
    6:  0.005,   # Business Key Duplicate Ratio <= 0.5%
    7:  None,    # Near-Duplicate — advisory; no hard threshold
    8:  0.005,   # Data Type Conformance error <= 0.5%
    9:  0.0,     # Domain Violation = 0 (zero tolerance)
    10: 0.005,   # Format Violation <= 0.5%
    11: 0.005,   # Range Violation <= 0.5%
    12: 0.0,     # Negative Value = 0 (zero tolerance)
    13: 0.0,     # Cross-Field Consistency errors = 0
    14: 0.005,   # Cross-Table Mismatch <= 0.5%
    15: 0.0,     # Unit/Currency Mismatch = 0
    16: 0.01,    # Standardization Issues <= 1%
    17: 0.0,     # FK Violations = 0 (zero tolerance)
    18: 0.0,     # Orphan Records = 0
    19: 0.99,    # Broken Join Coverage >= 99% (threshold is min coverage)
    20: 24.0,    # Freshness Lag <= 24 hours
    21: 0.0,     # Stale Table Flag = 0
    22: 0.01,    # Late-Arriving Data <= 1%
    23: 0.30,    # Volume Change within ±30%
    24: 0.0,     # Missing Partitions = 0
    25: 0.005,   # Source-to-Target Variance <= 0.5%
    26: 0.005,   # Business Rule Error <= 0.5%
    27: 0.005,   # Reference Data Mismatch <= 0.5%
    28: None,    # Outlier Detection — advisory
    29: None,    # Distribution Drift — advisory
    30: 0.0,     # Schema Drift Events = 0
    31: 0.05,    # Metadata Gap <= 5%
    32: 0.01,    # Pipeline Failure Rate <= 1%
    33: 0.005,   # Timestamp Integrity Error <= 0.5%
}

# ---------------------------------------------------------------------------
# MVP Phase classification
# ---------------------------------------------------------------------------
MVP_PHASE: dict[int, str] = {
    # Phase 1 — MVP
    1:  "mvp", 2:  "mvp", 3:  "mvp", 4:  "mvp",   # completeness
    5:  "mvp", 6:  "mvp", 7:  "mvp",               # uniqueness
    8:  "mvp", 9:  "mvp", 10: "mvp", 11: "mvp", 12: "mvp",  # validity
    17: "mvp", 18: "mvp", 19: "mvp",               # integrity
    20: "mvp", 21: "mvp", 22: "mvp",               # freshness
    23: "mvp", 24: "mvp",                           # volume
    # Phase 2
    13: "phase2", 14: "phase2", 15: "phase2", 16: "phase2",  # consistency
    25: "phase2", 26: "phase2", 27: "phase2",      # accuracy
    28: "phase2", 29: "phase2",                    # anomaly
    30: "phase2",                                  # schema
    32: "phase2",                                  # pipeline
    # Later
    31: "later",                                   # metadata
    33: "later",                                   # time_series
}

# Dimension → check IDs mapping (for registry lookups)
DIMENSION_CHECK_IDS: dict[str, list[int]] = {
    dim: list(weights.keys())
    for dim, weights in CHECK_WITHIN_DIM_WEIGHTS.items()
}
