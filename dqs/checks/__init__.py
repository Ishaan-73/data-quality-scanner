"""Check registry — maps check_id to BaseCheck subclass instances."""

from __future__ import annotations

from dqs.checks.base import BaseCheck
from dqs.checks.completeness import (
    ConditionalCompleteness,
    EmptyRowCheck,
    NullRatioByColumn,
    NullRatioInCriticalColumns,
)
from dqs.checks.uniqueness import (
    BusinessKeyDuplicateRatio,
    NearDuplicateCandidateCheck,
    PrimaryKeyDuplicateRatio,
)
from dqs.checks.validity import (
    AllowedValueDomainCheck,
    DataTypeConformance,
    FormatValidation,
    NegativeValueCheck,
    RangeValidation,
)
from dqs.checks.consistency import (
    CrossFieldConsistency,
    CrossTableConsistency,
    StandardizationCheck,
    UnitCurrencyConsistency,
)
from dqs.checks.integrity import (
    BrokenJoinCoverageCheck,
    ForeignKeyViolationCheck,
    OrphanRecordCheck,
)
from dqs.checks.freshness import (
    FreshnessLagHours,
    LateArrivingDataCheck,
    StaleTableCheck,
)
from dqs.checks.volume import (
    MissingPartitionCheck,
    RowCountVolumeChange,
)
from dqs.checks.accuracy import (
    BusinessRuleValidation,
    ReferenceDataValidation,
    SourceToTargetReconciliation,
)
from dqs.checks.anomaly import (
    DistributionDriftCheck,
    OutlierDetection,
)
from dqs.checks.schema_checks import SchemaDriftDetection
from dqs.checks.metadata_checks import MetadataCompleteness
from dqs.checks.pipeline_checks import PipelineFailureIndicator
from dqs.checks.time_series import TimestampSequenceIntegrity

# Registry: check_id -> instantiated check object
CHECK_REGISTRY: dict[int, BaseCheck] = {
    check.check_id: check
    for check in [
        NullRatioByColumn(),
        NullRatioInCriticalColumns(),
        EmptyRowCheck(),
        ConditionalCompleteness(),
        PrimaryKeyDuplicateRatio(),
        BusinessKeyDuplicateRatio(),
        NearDuplicateCandidateCheck(),
        DataTypeConformance(),
        AllowedValueDomainCheck(),
        FormatValidation(),
        RangeValidation(),
        NegativeValueCheck(),
        CrossFieldConsistency(),
        CrossTableConsistency(),
        UnitCurrencyConsistency(),
        StandardizationCheck(),
        ForeignKeyViolationCheck(),
        OrphanRecordCheck(),
        BrokenJoinCoverageCheck(),
        FreshnessLagHours(),
        StaleTableCheck(),
        LateArrivingDataCheck(),
        RowCountVolumeChange(),
        MissingPartitionCheck(),
        SourceToTargetReconciliation(),
        BusinessRuleValidation(),
        ReferenceDataValidation(),
        OutlierDetection(),
        DistributionDriftCheck(),
        SchemaDriftDetection(),
        MetadataCompleteness(),
        PipelineFailureIndicator(),
        TimestampSequenceIntegrity(),
    ]
}

__all__ = ["CHECK_REGISTRY", "BaseCheck"]
