"""
Unit tests for load patterns and configuration.
"""

import pytest
from schema_mapper.incremental import (
    LoadPattern,
    MergeStrategy,
    DeleteStrategy,
    IncrementalConfig,
    get_pattern_metadata,
    list_patterns_for_use_case,
    get_all_patterns,
    get_simple_patterns,
    get_advanced_patterns,
)


class TestLoadPatternEnum:
    """Test LoadPattern enum values."""

    def test_load_pattern_values(self):
        """Test that LoadPattern enum has expected values."""
        assert LoadPattern.UPSERT.value == "upsert"
        assert LoadPattern.SCD_TYPE2.value == "scd_type2"
        assert LoadPattern.FULL_REFRESH.value == "full_refresh"
        assert LoadPattern.APPEND_ONLY.value == "append"
        assert LoadPattern.CDC_MERGE.value == "cdc"

    def test_all_patterns_exist(self):
        """Test that all expected patterns are defined."""
        expected_patterns = [
            "full_refresh", "append", "upsert", "delete_insert",
            "incremental_timestamp", "incremental_append",
            "scd_type1", "scd_type2", "cdc"
        ]
        actual_values = [p.value for p in LoadPattern]
        for expected in expected_patterns:
            assert expected in actual_values


class TestMergeStrategy:
    """Test MergeStrategy enum."""

    def test_merge_strategy_values(self):
        """Test MergeStrategy enum values."""
        assert MergeStrategy.UPDATE_ALL.value == "update_all"
        assert MergeStrategy.UPDATE_CHANGED.value == "update_changed"
        assert MergeStrategy.UPDATE_SELECTIVE.value == "update_selective"
        assert MergeStrategy.UPDATE_NONE.value == "update_none"


class TestDeleteStrategy:
    """Test DeleteStrategy enum."""

    def test_delete_strategy_values(self):
        """Test DeleteStrategy enum values."""
        assert DeleteStrategy.HARD_DELETE.value == "hard_delete"
        assert DeleteStrategy.SOFT_DELETE.value == "soft_delete"
        assert DeleteStrategy.IGNORE.value == "ignore"


class TestIncrementalConfig:
    """Test IncrementalConfig configuration class."""

    def test_valid_upsert_config(self):
        """Test creating a valid UPSERT config."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['id']
        )
        # Should not raise
        config.validate()
        assert config.load_pattern == LoadPattern.UPSERT
        assert config.primary_keys == ['id']
        assert config.merge_strategy == MergeStrategy.UPDATE_ALL

    def test_valid_scd2_config(self):
        """Test creating a valid SCD Type 2 config."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['customer_id'],
            hash_columns=['name', 'email', 'address']
        )
        config.validate()
        assert config.load_pattern == LoadPattern.SCD_TYPE2
        assert config.hash_columns == ['name', 'email', 'address']

    def test_empty_primary_keys_raises_error(self):
        """Test that empty primary keys raises ValueError."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=[]
        )
        with pytest.raises(ValueError, match="primary_keys cannot be empty"):
            config.validate()

    def test_incremental_timestamp_requires_column(self):
        """Test that INCREMENTAL_TIMESTAMP requires incremental_column."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.INCREMENTAL_TIMESTAMP,
            primary_keys=['id']
        )
        with pytest.raises(ValueError, match="incremental_column required"):
            config.validate()

        # Should work with incremental_column
        config.incremental_column = 'created_at'
        config.validate()

    def test_scd2_requires_hash_columns(self):
        """Test that SCD_TYPE2 requires hash_columns."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['id']
        )
        with pytest.raises(ValueError, match="hash_columns required"):
            config.validate()

        # Should work with hash_columns
        config.hash_columns = ['name', 'email']
        config.validate()

    def test_cdc_requires_operation_column(self):
        """Test that CDC_MERGE requires operation_column."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.CDC_MERGE,
            primary_keys=['id'],
            operation_column=None
        )
        with pytest.raises(ValueError, match="operation_column required"):
            config.validate()

        # Should work with operation_column
        config.operation_column = '_op'
        config.validate()

    def test_update_selective_requires_columns(self):
        """Test that UPDATE_SELECTIVE requires update_columns."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['id'],
            merge_strategy=MergeStrategy.UPDATE_SELECTIVE
        )
        with pytest.raises(ValueError, match="update_columns required"):
            config.validate()

        # Should work with update_columns
        config.update_columns = ['name', 'email']
        config.validate()

    def test_soft_delete_requires_column(self):
        """Test that SOFT_DELETE requires soft_delete_column."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['id'],
            delete_strategy=DeleteStrategy.SOFT_DELETE,
            soft_delete_column=None
        )
        with pytest.raises(ValueError, match="soft_delete_column required"):
            config.validate()

        # Should work with soft_delete_column
        config.soft_delete_column = 'is_deleted'
        config.validate()

    def test_config_with_all_options(self):
        """Test config with all optional parameters."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['user_id'],
            merge_strategy=MergeStrategy.UPDATE_CHANGED,
            update_columns=['name', 'email'],
            incremental_column='updated_at',
            lookback_window='2 hours',
            staging_table='staging_users',
            partition_column='created_date',
            cluster_columns=['user_id', 'created_date'],
            enable_validation=True,
            dry_run=False,
            created_by_column='created_at',
            updated_by_column='updated_at'
        )
        # Should not raise
        config.validate()


class TestPatternMetadata:
    """Test pattern metadata functionality."""

    def test_get_pattern_metadata(self):
        """Test getting metadata for a pattern."""
        metadata = get_pattern_metadata(LoadPattern.UPSERT)
        assert metadata is not None
        assert metadata.name == "Upsert (Merge)"
        assert metadata.requires_primary_key is True
        assert metadata.complexity == "medium"

    def test_full_refresh_metadata(self):
        """Test full refresh pattern metadata."""
        metadata = get_pattern_metadata(LoadPattern.FULL_REFRESH)
        assert metadata.name == "Full Refresh"
        assert metadata.requires_primary_key is False
        assert metadata.supports_delete is True
        assert metadata.complexity == "simple"

    def test_scd2_metadata(self):
        """Test SCD Type 2 pattern metadata."""
        metadata = get_pattern_metadata(LoadPattern.SCD_TYPE2)
        assert metadata.name == "Slowly Changing Dimension Type 2"
        assert metadata.requires_primary_key is True
        assert metadata.complexity == "advanced"

    def test_list_patterns_for_use_case(self):
        """Test finding patterns by use case."""
        # Search for event-related patterns
        patterns = list_patterns_for_use_case("event")
        assert LoadPattern.APPEND_ONLY in patterns
        assert LoadPattern.INCREMENTAL_TIMESTAMP in patterns

        # Search for historical tracking
        patterns = list_patterns_for_use_case("historical")
        assert LoadPattern.SCD_TYPE2 in patterns

    def test_get_all_patterns(self):
        """Test getting all patterns."""
        patterns = get_all_patterns()
        assert len(patterns) == 9
        assert LoadPattern.UPSERT in patterns
        assert LoadPattern.SCD_TYPE2 in patterns

    def test_get_simple_patterns(self):
        """Test getting simple patterns."""
        patterns = get_simple_patterns()
        assert LoadPattern.FULL_REFRESH in patterns
        assert LoadPattern.APPEND_ONLY in patterns
        # Advanced patterns should not be in simple
        assert LoadPattern.SCD_TYPE2 not in patterns

    def test_get_advanced_patterns(self):
        """Test getting advanced patterns."""
        patterns = get_advanced_patterns()
        assert LoadPattern.SCD_TYPE2 in patterns
        assert LoadPattern.CDC_MERGE in patterns
        # Simple patterns should not be in advanced
        assert LoadPattern.FULL_REFRESH not in patterns


class TestConfigDefaults:
    """Test default configuration values."""

    def test_default_merge_strategy(self):
        """Test default merge strategy is UPDATE_ALL."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['id']
        )
        assert config.merge_strategy == MergeStrategy.UPDATE_ALL

    def test_default_delete_strategy(self):
        """Test default delete strategy is IGNORE."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['id']
        )
        assert config.delete_strategy == DeleteStrategy.IGNORE

    def test_default_validation_enabled(self):
        """Test validation is enabled by default."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['id']
        )
        assert config.enable_validation is True

    def test_default_dry_run_false(self):
        """Test dry_run is False by default."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.UPSERT,
            primary_keys=['id']
        )
        assert config.dry_run is False

    def test_default_scd2_columns(self):
        """Test default SCD Type 2 column names."""
        config = IncrementalConfig(
            load_pattern=LoadPattern.SCD_TYPE2,
            primary_keys=['id'],
            hash_columns=['name']
        )
        assert config.effective_date_column == "effective_from"
        assert config.expiration_date_column == "effective_to"
        assert config.is_current_column == "is_current"
