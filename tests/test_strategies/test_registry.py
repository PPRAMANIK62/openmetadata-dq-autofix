"""Tests for strategy registry."""

from dq_autofix.strategies import (
    FailureContext,
    KeepFirstStrategy,
    MeanImputationStrategy,
    MedianImputationStrategy,
    ModeImputationStrategy,
    StrategyRegistry,
    TrimWhitespaceStrategy,
    create_default_registry,
    get_default_registry,
)


class TestStrategyRegistry:
    """Tests for StrategyRegistry class."""

    def test_register_strategy(self) -> None:
        """Test registering a strategy."""
        registry = StrategyRegistry()
        strategy = MeanImputationStrategy()
        registry.register(strategy)

        strategies = registry.get_strategies_for_test_type("columnValuesToNotBeNull")
        assert strategy in strategies

    def test_register_multiple_strategies(self) -> None:
        """Test registering multiple strategies for same test type."""
        registry = StrategyRegistry()
        mean = MeanImputationStrategy()
        median = MedianImputationStrategy()

        registry.register(mean)
        registry.register(median)

        strategies = registry.get_strategies_for_test_type("columnValuesToNotBeNull")
        assert len(strategies) == 2
        assert mean in strategies
        assert median in strategies

    def test_get_strategy_by_name(self) -> None:
        """Test retrieving strategy by name."""
        registry = StrategyRegistry()
        strategy = MeanImputationStrategy()
        registry.register(strategy)

        found = registry.get_strategy_by_name("mean_imputation")
        assert found is strategy

    def test_get_strategy_by_name_not_found(self) -> None:
        """Test retrieving non-existent strategy by name."""
        registry = StrategyRegistry()
        found = registry.get_strategy_by_name("nonexistent")
        assert found is None

    def test_unregister_strategy(self) -> None:
        """Test unregistering a strategy."""
        registry = StrategyRegistry()
        strategy = MeanImputationStrategy()
        registry.register(strategy)

        removed = registry.unregister("mean_imputation")
        assert removed is True
        assert registry.get_strategy_by_name("mean_imputation") is None
        assert strategy not in registry.get_strategies_for_test_type("columnValuesToNotBeNull")

    def test_unregister_nonexistent(self) -> None:
        """Test unregistering non-existent strategy."""
        registry = StrategyRegistry()
        removed = registry.unregister("nonexistent")
        assert removed is False

    def test_get_all_strategies(self) -> None:
        """Test retrieving all registered strategies."""
        registry = StrategyRegistry()
        registry.register(MeanImputationStrategy())
        registry.register(TrimWhitespaceStrategy())

        all_strategies = registry.get_all_strategies()
        assert len(all_strategies) == 2

    def test_get_all_test_types(self) -> None:
        """Test retrieving all supported test types."""
        registry = StrategyRegistry()
        registry.register(MeanImputationStrategy())
        registry.register(TrimWhitespaceStrategy())
        registry.register(KeepFirstStrategy())

        test_types = registry.get_all_test_types()
        assert "columnValuesToNotBeNull" in test_types
        assert "columnValuesToMatchRegex" in test_types
        assert "columnValuesToBeUnique" in test_types

    def test_strategies_for_unknown_test_type(self) -> None:
        """Test getting strategies for unknown test type."""
        registry = StrategyRegistry()
        strategies = registry.get_strategies_for_test_type("unknownTestType")
        assert strategies == []

    def test_recommend(self, null_failure_context: FailureContext) -> None:
        """Test recommendation ranking."""
        registry = StrategyRegistry()
        registry.register(MeanImputationStrategy())
        registry.register(MedianImputationStrategy())
        registry.register(ModeImputationStrategy())

        recommendations = registry.recommend(null_failure_context)
        assert len(recommendations) >= 2

        scores = [r[1].score for r in recommendations]
        assert scores == sorted(scores, reverse=True)

    def test_recommend_min_confidence_filter(self, null_failure_context: FailureContext) -> None:
        """Test recommendation respects minimum confidence."""
        registry = StrategyRegistry()
        registry.register(MeanImputationStrategy())
        registry.register(MedianImputationStrategy())

        high_threshold = registry.recommend(null_failure_context, min_confidence=0.9)
        low_threshold = registry.recommend(null_failure_context, min_confidence=0.3)

        assert len(low_threshold) >= len(high_threshold)

    def test_recommend_best(self, null_failure_context: FailureContext) -> None:
        """Test getting single best recommendation."""
        registry = StrategyRegistry()
        registry.register(MeanImputationStrategy())
        registry.register(MedianImputationStrategy())

        best = registry.recommend_best(null_failure_context)
        assert best is not None
        _strategy, confidence = best
        assert confidence.score > 0

    def test_recommend_best_none_applicable(self) -> None:
        """Test recommend_best returns None when no strategies apply."""

        from dq_autofix.openmetadata.models import TestCaseResult

        test_case = TestCaseResult(
            id="tc-001",
            name="test",
            test_definition="unknownTestType",
            entity_link="<#E::table::db.schema.table::columns::col>",
        )
        context = FailureContext(test_case=test_case)

        registry = StrategyRegistry()
        registry.register(MeanImputationStrategy())

        best = registry.recommend_best(context)
        assert best is None


class TestCreateDefaultRegistry:
    """Tests for default registry creation."""

    def test_default_registry_has_null_strategies(self) -> None:
        """Test default registry includes null imputation strategies."""
        registry = create_default_registry()
        strategies = registry.get_strategies_for_test_type("columnValuesToNotBeNull")
        names = {s.name for s in strategies}

        assert "mean_imputation" in names
        assert "median_imputation" in names
        assert "mode_imputation" in names
        assert "forward_fill" in names

    def test_default_registry_has_normalization_strategies(self) -> None:
        """Test default registry includes normalization strategies."""
        registry = create_default_registry()
        strategies = registry.get_strategies_for_test_type("columnValuesToMatchRegex")
        names = {s.name for s in strategies}

        assert "trim_whitespace" in names
        assert "normalize_case" in names

    def test_default_registry_has_dedup_strategies(self) -> None:
        """Test default registry includes deduplication strategies."""
        registry = create_default_registry()
        strategies = registry.get_strategies_for_test_type("columnValuesToBeUnique")
        names = {s.name for s in strategies}

        assert "keep_first" in names
        assert "keep_last" in names

    def test_default_registry_total_strategies(self) -> None:
        """Test default registry has expected number of strategies."""
        registry = create_default_registry()
        all_strategies = registry.get_all_strategies()
        assert len(all_strategies) == 8


class TestGetDefaultRegistry:
    """Tests for singleton default registry."""

    def test_returns_same_instance(self) -> None:
        """Test get_default_registry returns singleton."""
        from dq_autofix.strategies import registry as registry_module

        registry_module._default_registry = None

        r1 = get_default_registry()
        r2 = get_default_registry()
        assert r1 is r2


class TestRegistryIntegration:
    """Integration tests for registry with different failure contexts."""

    def test_recommend_for_null_failure(self, null_failure_context: FailureContext) -> None:
        """Test recommendations for null value failure."""
        registry = create_default_registry()
        recommendations = registry.recommend(null_failure_context)

        assert len(recommendations) > 0
        strategy, confidence = recommendations[0]
        assert strategy.name in ["mean_imputation", "median_imputation"]
        assert confidence.score > 0.6

    def test_recommend_for_whitespace_failure(
        self, whitespace_failure_context: FailureContext
    ) -> None:
        """Test recommendations for whitespace failure."""
        registry = create_default_registry()
        recommendations = registry.recommend(whitespace_failure_context)

        assert len(recommendations) > 0
        names = {r[0].name for r in recommendations}
        assert "trim_whitespace" in names

    def test_recommend_for_duplicate_failure(
        self, duplicate_failure_context: FailureContext
    ) -> None:
        """Test recommendations for duplicate failure."""
        registry = create_default_registry()
        recommendations = registry.recommend(duplicate_failure_context)

        assert len(recommendations) > 0
        names = {r[0].name for r in recommendations}
        assert "keep_first" in names or "keep_last" in names
