"""Fix strategies for DQ AutoFix.

This module provides strategies for automatically fixing data quality issues
detected by OpenMetadata test cases.

Strategies are organized by fix type:
- Null imputation: Mean, Median, Mode, Forward Fill
- Normalization: Trim whitespace, Case normalization
- Deduplication: Keep first, Keep last

Usage:
    from dq_autofix.strategies import (
        FailureContext,
        StrategyRegistry,
        create_default_registry,
    )

    # Create a registry with all default strategies
    registry = create_default_registry()

    # Get recommendations for a failure
    context = FailureContext(test_case=test_case, column_profile=profile)
    recommendations = registry.recommend(context)

    for strategy, confidence in recommendations:
        print(f"{strategy.name}: {confidence.score:.1%}")
"""

from dq_autofix.strategies.base import (
    CaseType,
    ConfidenceResult,
    FailureContext,
    FixStrategy,
    PreviewResult,
)
from dq_autofix.strategies.deduplication import (
    KeepFirstStrategy,
    KeepLastStrategy,
)
from dq_autofix.strategies.normalization import (
    NormalizeCaseStrategy,
    TrimWhitespaceStrategy,
)
from dq_autofix.strategies.null_imputation import (
    ForwardFillStrategy,
    MeanImputationStrategy,
    MedianImputationStrategy,
    ModeImputationStrategy,
)
from dq_autofix.strategies.registry import (
    StrategyRegistry,
    create_default_registry,
    get_default_registry,
)

__all__ = [
    "CaseType",
    "ConfidenceResult",
    "FailureContext",
    "FixStrategy",
    "ForwardFillStrategy",
    "KeepFirstStrategy",
    "KeepLastStrategy",
    "MeanImputationStrategy",
    "MedianImputationStrategy",
    "ModeImputationStrategy",
    "NormalizeCaseStrategy",
    "PreviewResult",
    "StrategyRegistry",
    "TrimWhitespaceStrategy",
    "create_default_registry",
    "get_default_registry",
]
