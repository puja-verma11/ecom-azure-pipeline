# tests/unit/test_bronze_checks.py
# Unit tests for bronze_checks.py
# These use FAKE data — no Azure connection needed
# Run with: pytest tests/unit/test_bronze_checks.py -v

import pytest
import pandas as pd
from quality.bronze_checks import (
    check_null_ids,
    check_duplicates,
    run_bronze_checks
)


# ─────────────────────────────────────────────
# FIXTURES — fake data we reuse across tests
# ─────────────────────────────────────────────

@pytest.fixture
def clean_orders_df():
    """5 perfectly clean order rows — all should PASS"""
    return pd.DataFrame({
        'order_id'    : [1, 2, 3, 4, 5],
        'customer_id' : [1, 2, 3, 4, 5],
        'total_amount': [100.0, 200.0, 300.0, 400.0, 500.0],
        'status'      : ['delivered'] * 5
    })


@pytest.fixture
def orders_with_null_ids():
    """3 rows — 2 have NULL order_id — should FAIL null check"""
    return pd.DataFrame({
        'order_id'    : [1, None, None],
        'customer_id' : [1, 2, 3],
        'total_amount': [100.0, 200.0, 300.0],
        'status'      : ['delivered', 'pending', 'shipped']
    })


@pytest.fixture
def orders_with_duplicates():
    """4 rows — order_id=1 appears twice — should FAIL duplicate check"""
    return pd.DataFrame({
        'order_id'    : [1, 1, 2, 3],
        'customer_id' : [1, 1, 2, 3],
        'total_amount': [100.0, 100.0, 200.0, 300.0],
        'status'      : ['delivered'] * 4
    })


# ─────────────────────────────────────────────
# TESTS FOR check_null_ids()
# ─────────────────────────────────────────────

class TestCheckNullIds:

    def test_passes_when_no_nulls(self, clean_orders_df):
        """Clean data → good_df has all rows, quarantine is empty"""
        good_df, quarantine_df = check_null_ids(
            clean_orders_df, 'order_id', 'orders'
        )
        assert len(good_df)       == 5
        assert len(quarantine_df) == 0

    def test_fails_when_nulls_exist(self, orders_with_null_ids):
        """2 NULL order_ids → 2 rows quarantined, 1 row passes"""
        good_df, quarantine_df = check_null_ids(
            orders_with_null_ids, 'order_id', 'orders'
        )
        assert len(good_df)       == 1
        assert len(quarantine_df) == 2

    def test_quarantine_has_reason_column(self, orders_with_null_ids):
        """Quarantined rows must have quarantine_reason column"""
        _, quarantine_df = check_null_ids(
            orders_with_null_ids, 'order_id', 'orders'
        )
        assert 'quarantine_reason' in quarantine_df.columns

    def test_quarantine_has_timestamp(self, orders_with_null_ids):
        """Quarantined rows must have quarantined_at timestamp"""
        _, quarantine_df = check_null_ids(
            orders_with_null_ids, 'order_id', 'orders'
        )
        assert 'quarantined_at' in quarantine_df.columns

    def test_quarantine_has_pipeline_layer(self, orders_with_null_ids):
        """Quarantined rows must be tagged with pipeline_layer=BRONZE"""
        _, quarantine_df = check_null_ids(
            orders_with_null_ids, 'order_id', 'orders'
        )
        assert quarantine_df['pipeline_layer'].iloc[0] == 'BRONZE'

    def test_good_rows_dont_have_quarantine_columns(self, clean_orders_df):
        """Good rows should NOT have quarantine metadata columns"""
        good_df, _ = check_null_ids(
            clean_orders_df, 'order_id', 'orders'
        )
        assert 'quarantine_reason' not in good_df.columns


# ─────────────────────────────────────────────
# TESTS FOR check_duplicates()
# ─────────────────────────────────────────────

class TestCheckDuplicates:

    def test_passes_when_no_duplicates(self, clean_orders_df):
        """Clean data → all rows pass, quarantine is empty"""
        good_df, quarantine_df = check_duplicates(
            clean_orders_df, ['order_id'], 'orders'
        )
        assert len(good_df)       == 5
        assert len(quarantine_df) == 0

    def test_fails_when_duplicates_exist(self, orders_with_duplicates):
        """order_id=1 appears twice → 1 duplicate quarantined"""
        good_df, quarantine_df = check_duplicates(
            orders_with_duplicates, ['order_id'], 'orders'
        )
        assert len(good_df)       == 3  # first of each kept
        assert len(quarantine_df) == 1  # second order_id=1 quarantined

    def test_first_occurrence_kept_not_quarantined(self, orders_with_duplicates):
        """First occurrence of duplicate must be in good_df"""
        good_df, quarantine_df = check_duplicates(
            orders_with_duplicates, ['order_id'], 'orders'
        )
        assert 1 in good_df['order_id'].values

    def test_quarantine_has_reason_column(self, orders_with_duplicates):
        """Quarantined rows must explain why they were rejected"""
        _, quarantine_df = check_duplicates(
            orders_with_duplicates, ['order_id'], 'orders'
        )
        assert 'quarantine_reason' in quarantine_df.columns


# ─────────────────────────────────────────────
# TESTS FOR run_bronze_checks()
# ─────────────────────────────────────────────

class TestRunBronzeChecks:

    def test_clean_data_passes_all_checks(self, clean_orders_df):
        """Perfectly clean data → all rows in clean_df, nothing quarantined"""
        clean_df, quarantine_df = run_bronze_checks(
            df           = clean_orders_df,
            id_column    = 'order_id',
            key_columns  = ['order_id'],
            source_table = 'orders'
        )
        assert len(clean_df)      == 5
        assert len(quarantine_df) == 0

    def test_null_ids_are_quarantined(self, orders_with_null_ids):
        """NULL order_ids must end up in quarantine, not clean"""
        clean_df, quarantine_df = run_bronze_checks(
            df           = orders_with_null_ids,
            id_column    = 'order_id',
            key_columns  = ['order_id'],
            source_table = 'orders'
        )
        assert len(quarantine_df) >= 2
        # NULL rows must NOT be in clean_df
        assert clean_df['order_id'].isnull().sum() == 0

    def test_duplicates_are_quarantined(self, orders_with_duplicates):
        """Duplicate rows must end up in quarantine"""
        clean_df, quarantine_df = run_bronze_checks(
            df           = orders_with_duplicates,
            id_column    = 'order_id',
            key_columns  = ['order_id'],
            source_table = 'orders'
        )
        assert len(quarantine_df) >= 1
        # No duplicate order_ids in clean_df
        assert clean_df['order_id'].duplicated().sum() == 0

    def test_mixed_data_correctly_split(self):
        """Mix of good, null, duplicate rows → correctly split"""
        mixed_df = pd.DataFrame({
            'order_id'    : [1, 2, None, 2, 3],  # None=null, 2=duplicate
            'customer_id' : [1, 2, 3,    2, 5],
            'total_amount': [100, 200, 300, 200, 500],
            'status'      : ['delivered'] * 5
        })
        clean_df, quarantine_df = run_bronze_checks(
            df           = mixed_df,
            id_column    = 'order_id',
            key_columns  = ['order_id'],
            source_table = 'orders'
        )
        # Expect: rows 1, 2, 3 are clean (3 rows)
        # Quarantined: None row + duplicate row 2 (2 rows)
        assert len(clean_df)      == 3
        assert len(quarantine_df) == 2