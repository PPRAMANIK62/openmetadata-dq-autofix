"""Strategy registry for matching test types to fix strategies."""

from collections import defaultdict

from dq_autofix.strategies.base import (
    CaseType,
    ConfidenceResult,
    FailureContext,
    FixStrategy,
)
from dq_autofix.strategies.deduplication import KeepFirstStrategy, KeepLastStrategy
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


class StrategyRegistry:
    """Registry for fix strategies.

    Maps test types to applicable strategies and provides recommendation
    based on confidence scoring.
    """

    def __init__(self) -> None:
        self._strategies: dict[str, list[FixStrategy]] = defaultdict(list)
        self._by_name: dict[str, FixStrategy] = {}

    def register(self, strategy: FixStrategy) -> None:
        """Register a strategy for its supported test types.

        Args:
            strategy: The strategy to register.
        """
        for test_type in strategy.supported_test_types:
            if strategy not in self._strategies[test_type]:
                self._strategies[test_type].append(strategy)

        self._by_name[strategy.name] = strategy

    def unregister(self, strategy_name: str) -> bool:
        """Unregister a strategy by name.

        Args:
            strategy_name: Name of the strategy to remove.

        Returns:
            True if removed, False if not found.
        """
        strategy = self._by_name.pop(strategy_name, None)
        if not strategy:
            return False

        for test_type in strategy.supported_test_types:
            if strategy in self._strategies[test_type]:
                self._strategies[test_type].remove(strategy)

        return True

    def get_strategies_for_test_type(self, test_type: str) -> list[FixStrategy]:
        """Get all strategies that support a test type.

        Args:
            test_type: The test definition type (e.g., 'columnValuesToNotBeNull')

        Returns:
            List of applicable strategies.
        """
        return self._strategies.get(test_type, [])

    def get_strategy_by_name(self, name: str) -> FixStrategy | None:
        """Get a specific strategy by its name.

        Args:
            name: The strategy name (e.g., 'mean_imputation')

        Returns:
            The strategy or None if not found.
        """
        return self._by_name.get(name)

    def get_all_strategies(self) -> list[FixStrategy]:
        """Get all registered strategies.

        Returns:
            List of all strategies.
        """
        return list(self._by_name.values())

    def get_all_test_types(self) -> list[str]:
        """Get all supported test types.

        Returns:
            List of test type names.
        """
        return list(self._strategies.keys())

    def recommend(
        self, context: FailureContext, min_confidence: float = 0.4
    ) -> list[tuple[FixStrategy, ConfidenceResult]]:
        """Recommend strategies for a failure, ranked by confidence.

        Args:
            context: The failure context.
            min_confidence: Minimum confidence threshold (default 0.4)

        Returns:
            List of (strategy, confidence) tuples, sorted by confidence descending.
        """
        strategies = self.get_strategies_for_test_type(context.test_type)

        results: list[tuple[FixStrategy, ConfidenceResult]] = []

        for strategy in strategies:
            if not strategy.can_apply(context):
                continue

            confidence = strategy.calculate_confidence(context)
            if confidence.score >= min_confidence:
                results.append((strategy, confidence))

        results.sort(key=lambda x: x[1].score, reverse=True)

        return results

    def recommend_best(
        self, context: FailureContext
    ) -> tuple[FixStrategy, ConfidenceResult] | None:
        """Get the single best strategy recommendation.

        Args:
            context: The failure context.

        Returns:
            Best (strategy, confidence) tuple or None if no applicable strategy.
        """
        recommendations = self.recommend(context)
        return recommendations[0] if recommendations else None


def create_default_registry() -> StrategyRegistry:
    """Create a registry with all built-in strategies registered.

    Returns:
        A StrategyRegistry with default strategies.
    """
    registry = StrategyRegistry()

    registry.register(MeanImputationStrategy())
    registry.register(MedianImputationStrategy())
    registry.register(ModeImputationStrategy())
    registry.register(ForwardFillStrategy())

    registry.register(TrimWhitespaceStrategy())
    registry.register(NormalizeCaseStrategy(CaseType.LOWER))

    registry.register(KeepFirstStrategy())
    registry.register(KeepLastStrategy())

    return registry


_default_registry: StrategyRegistry | None = None


def get_default_registry() -> StrategyRegistry:
    """Get the singleton default registry.

    Returns:
        The default StrategyRegistry instance.
    """
    global _default_registry
    if _default_registry is None:
        _default_registry = create_default_registry()
    return _default_registry
