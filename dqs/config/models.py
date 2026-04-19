"""Pydantic models for configuration and results."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator


class ConnectorConfig(BaseModel):
    """Connection settings for a data warehouse."""

    dialect: Literal["snowflake", "bigquery", "redshift", "databricks", "postgres", "duckdb", "synapse", "sqlserver", "azure_blob"]
    # Common fields (used across dialects)
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")
    username: Optional[str] = None
    password: Optional[str] = None

    # Snowflake-specific
    account: Optional[str] = None
    warehouse: Optional[str] = None
    role: Optional[str] = None

    # BigQuery-specific
    project: Optional[str] = None
    credentials_path: Optional[str] = None

    # Databricks-specific
    http_path: Optional[str] = None
    access_token: Optional[str] = None
    server_hostname: Optional[str] = None

    # DuckDB (synthetic clone / testing)
    duckdb_path: Optional[str] = ":memory:"

    # Synapse-specific
    workspace: Optional[str] = None
    pool_type: Optional[Literal["dedicated", "serverless"]] = None
    auth: Optional[Dict[str, str]] = None  # method, tenant_id, client_id, client_secret

    model_config = {"populate_by_name": True}


class CheckConfig(BaseModel):
    """Per-check runtime parameters supplied in a scan YAML."""

    check_id: int
    enabled: bool = True
    table: Optional[str] = None
    column: Optional[str] = None
    columns: Optional[List[str]] = None           # multi-column checks
    pk_column: Optional[str] = None
    business_key_columns: Optional[List[str]] = None
    condition: Optional[str] = None               # WHERE condition for conditional checks
    dependent_column: Optional[str] = None
    allowed_values: Optional[List[Any]] = None
    pattern: Optional[str] = None                 # regex for format validation
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    timestamp_column: Optional[str] = None
    load_timestamp_column: Optional[str] = None
    event_timestamp_column: Optional[str] = None
    freshness_sla_hours: Optional[float] = 24.0
    stale_days: Optional[float] = 1.0
    late_arrival_hours: Optional[float] = 24.0
    reference_table: Optional[str] = None
    reference_column: Optional[str] = None
    source_table: Optional[str] = None
    source_column: Optional[str] = None
    target_table: Optional[str] = None
    target_column: Optional[str] = None
    parent_table: Optional[str] = None
    parent_column: Optional[str] = None
    child_table: Optional[str] = None
    child_column: Optional[str] = None
    price_column: Optional[str] = None
    quantity_column: Optional[str] = None
    revenue_column: Optional[str] = None
    today_table: Optional[str] = None
    prior_table: Optional[str] = None
    expected_partitions_table: Optional[str] = None
    actual_partitions_table: Optional[str] = None
    partition_key: Optional[str] = None
    metadata_catalog_table: Optional[str] = None
    pipeline_runs_table: Optional[str] = None
    dataset_name: Optional[str] = None
    entity_column: Optional[str] = None
    metric_column: Optional[str] = None
    outlier_z_threshold: float = 3.0
    volume_change_band: float = 0.30
    # Override threshold (uses default from defaults.py if not set)
    threshold: Optional[float] = None
    # Extra arbitrary params for custom checks
    extra: Optional[Dict[str, Any]] = None


class ScreenerConfig(BaseModel):
    """PII screener settings — add a 'screener:' block to scan YAML."""

    enabled: bool = True
    enable_pass3: bool = False
    sample_size: int = 100
    on_pii_detected: Literal["block", "exclude", "synthetic"] = "exclude"
    categories: Optional[List[str]] = None  # None = all categories


class PIIColumnResult(BaseModel):
    """PII assessment for one column."""

    column: str
    table: str
    status: Literal["HIGH", "MEDIUM", "REVIEW", "CLEAR"]
    category: Optional[str] = None        # identity|contact|financial|health|behavioral|indirect
    pii_type: Optional[str] = None        # e.g. "Contact — email"
    confidence: float = 0.0
    detected_via: str = "none"            # name_match | name_match+regex | semantic | none
    sample_count: int = 0
    match_count: int = 0
    masked_samples: List[str] = Field(default_factory=list)
    notes: Optional[str] = None


class PIIManifest(BaseModel):
    """PII screening results for one table."""

    scan_id: str
    table: str
    row_count: int
    screened_at: datetime
    passes_run: List[int]
    gate_decision: Literal["BLOCK", "PROCEED_EXCLUDE", "SYNTHETIC_MODE", "CLEAR"]
    gate_reason: str
    columns: List[PIIColumnResult] = Field(default_factory=list)
    high_count: int = 0
    medium_count: int = 0
    review_count: int = 0
    clear_count: int = 0


class ScanConfig(BaseModel):
    """Top-level scan configuration loaded from YAML."""

    scan_name: str = "dqs_scan"
    mode: Literal["live", "synthetic"] = "live"
    connector: ConnectorConfig
    # For synthetic mode: DW connector to profile, then disconnect
    profile_connector: Optional[ConnectorConfig] = None
    synthetic_row_count: int = 10_000
    checks: List[CheckConfig] = Field(default_factory=list)
    # Which dimensions to run (empty = all)
    dimensions: List[str] = Field(default_factory=list)
    # MVP-only shortcut
    mvp_only: bool = False
    # PII screener (runs before any DQS check)
    screener: Optional[ScreenerConfig] = None


class CheckResult(BaseModel):
    """Result of a single quality check."""

    check_id: int
    check_name: str
    dimension: str
    metric_name: str
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    passed: bool = False
    pass_score: float = 0.0        # 0.0–1.0 continuous score
    effective_weight: float = 0.0  # dim_weight × check_within_dim_weight
    weighted_score: float = 0.0    # pass_score × effective_weight
    error: Optional[str] = None    # populated if check failed to execute
    skipped: bool = False
    table: Optional[str] = None
    column: Optional[str] = None
    executed_sql: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class DimensionResult(BaseModel):
    """Aggregated results for one quality dimension."""

    dimension: str
    dimension_weight: float
    dimension_score: float          # 0–100
    checks: List[CheckResult] = Field(default_factory=list)
    checks_passed: int = 0
    checks_failed: int = 0
    checks_skipped: int = 0


class ScanReport(BaseModel):
    """Full report returned after a scan."""

    scan_name: str
    mode: Literal["live", "synthetic"]
    connector_dialect: str
    overall_score: float            # 0–100
    dimensions: List[DimensionResult] = Field(default_factory=list)
    total_checks: int = 0
    checks_passed: int = 0
    checks_failed: int = 0
    checks_skipped: int = 0
    scan_start: datetime = Field(default_factory=datetime.utcnow)
    scan_end: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    is_synthetic: bool = False
    pii_manifests: List["PIIManifest"] = Field(default_factory=list)


class SchemaColumnProfile(BaseModel):
    """Statistical profile of a single column (used by synthetic clone)."""

    name: str
    dtype: str                       # raw SQL type string
    python_dtype: str                # 'str', 'int', 'float', 'datetime', 'bool'
    null_rate: float = 0.0
    unique_count: Optional[int] = None
    cardinality_ratio: Optional[float] = None
    sample_values: List[Any] = Field(default_factory=list)
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean: Optional[float] = None
    stddev: Optional[float] = None
    top_values: Optional[Dict[Any, float]] = None  # value → frequency
    is_nullable: bool = True


class SchemaProfile(BaseModel):
    """Statistical profile of a table (used by synthetic clone)."""

    table_name: str
    row_count: int
    columns: List[SchemaColumnProfile] = Field(default_factory=list)
    profiled_at: datetime = Field(default_factory=datetime.utcnow)
