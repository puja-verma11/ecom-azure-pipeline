# tests/unit/test_silver_checks.py
# Unit tests for silver_checks.py
# These use FAKE data — no Azure connection needed
# Run with: pytest tests/unit/test_silver_checks.py -v

import pytest
import pandas as pd
from quality.silver_checks import (
    check_null_fields,
    check_email_format,
    check_positive_values,
    check_no_future_dates,
    check_referential_integrity,
    run_silver_checks,
)


# ─────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────

@pytest.fixture
def clean_customers_df():
    return pd.DataFrame({
        'customer_id': [1, 2, 3],
        'email':       ['alice@example.com', 'bob@example.com', 'carol@example.com'],
        'first_name':  ['Alice', 'Bob', 'Carol'],
    })


@pytest.fixture
def clean_orders_df():
    return pd.DataFrame({
        'order_id':    [1, 2, 3],
        'customer_id': [1, 2, 3],
        'order_date':  ['2024-01-01', '2024-02-01', '2024-03-01'],
        'total_amount':[100.0, 200.0, 300.0],
    })


@pytest.fixture
def customers_ref_df():
    return pd.DataFrame({'customer_id': [1, 2, 3]})


# ─────────────────────────────────────────────
# check_null_fields
# ─────────────────────────────────────────────

class TestCheckNullFields:

    def test_passes_when_no_nulls(self, clean_customers_df):
        good_df, bad_df = check_null_fields(clean_customers_df, ['customer_id', 'email'], 'customers')
        assert len(good_df) == 3
        assert len(bad_df)  == 0

    def test_catches_null_in_required_field(self):
        df = pd.DataFrame({
            'customer_id': [1, None, 3],
            'email':       ['a@b.com', 'c@d.com', 'e@f.com'],
        })
        good_df, bad_df = check_null_fields(df, ['customer_id'], 'customers')
        assert len(good_df) == 2
        assert len(bad_df)  == 1

    def test_bad_rows_have_metadata(self):
        df = pd.DataFrame({'customer_id': [None], 'email': ['a@b.com']})
        _, bad_df = check_null_fields(df, ['customer_id'], 'customers')
        assert 'quarantine_reason' in bad_df.columns
        assert 'quarantined_at'    in bad_df.columns
        assert 'pipeline_layer'    in bad_df.columns
        assert bad_df['pipeline_layer'].iloc[0] == 'SILVER'
        assert bad_df['source_table'].iloc[0]   == 'customers'

    def test_multiple_required_fields(self):
        df = pd.DataFrame({
            'customer_id': [1, 2, None],
            'email':       ['a@b.com', None, 'c@d.com'],
        })
        good_df, bad_df = check_null_fields(df, ['customer_id', 'email'], 'customers')
        assert len(good_df) == 1
        assert len(bad_df)  == 2


# ─────────────────────────────────────────────
# check_email_format
# ─────────────────────────────────────────────

class TestCheckEmailFormat:

    def test_passes_valid_emails(self, clean_customers_df):
        good_df, bad_df = check_email_format(clean_customers_df, 'customers')
        assert len(good_df) == 3
        assert len(bad_df)  == 0

    def test_catches_invalid_email(self):
        df = pd.DataFrame({'email': ['valid@example.com', 'not-an-email', 'also@good.com']})
        good_df, bad_df = check_email_format(df, 'customers')
        assert len(good_df) == 2
        assert len(bad_df)  == 1

    def test_bad_row_has_quarantine_reason(self):
        df = pd.DataFrame({'email': ['bad-email']})
        _, bad_df = check_email_format(df, 'customers')
        assert bad_df['quarantine_reason'].iloc[0] == 'Invalid email format'

    def test_skips_when_no_email_column(self, clean_orders_df):
        good_df, bad_df = check_email_format(clean_orders_df, 'orders')
        assert len(good_df) == 3
        assert len(bad_df)  == 0


# ─────────────────────────────────────────────
# check_positive_values
# ─────────────────────────────────────────────

class TestCheckPositiveValues:

    def test_passes_positive_values(self, clean_orders_df):
        good_df, bad_df = check_positive_values(clean_orders_df, ['total_amount'], 'orders')
        assert len(good_df) == 3
        assert len(bad_df)  == 0

    def test_catches_negative_value(self):
        df = pd.DataFrame({'price': [10.0, -5.0, 20.0]})
        good_df, bad_df = check_positive_values(df, ['price'], 'products')
        assert len(good_df) == 2
        assert len(bad_df)  == 1

    def test_catches_zero_value(self):
        df = pd.DataFrame({'quantity': [1, 0, 3]})
        good_df, bad_df = check_positive_values(df, ['quantity'], 'order_items')
        assert len(bad_df) == 1

    def test_skips_when_field_not_in_df(self, clean_customers_df):
        good_df, bad_df = check_positive_values(clean_customers_df, ['price'], 'customers')
        assert len(good_df) == 3
        assert len(bad_df)  == 0


# ─────────────────────────────────────────────
# check_no_future_dates
# ─────────────────────────────────────────────

class TestCheckNoFutureDates:

    def test_passes_past_dates(self, clean_orders_df):
        good_df, bad_df = check_no_future_dates(clean_orders_df, ['order_date'], 'orders')
        assert len(good_df) == 3
        assert len(bad_df)  == 0

    def test_catches_future_date(self):
        df = pd.DataFrame({
            'order_id':   [1, 2],
            'order_date': ['2020-01-01', '2099-01-01'],
        })
        good_df, bad_df = check_no_future_dates(df, ['order_date'], 'orders')
        assert len(good_df) == 1
        assert len(bad_df)  == 1

    def test_bad_row_has_metadata(self):
        df = pd.DataFrame({'order_date': ['2099-01-01']})
        _, bad_df = check_no_future_dates(df, ['order_date'], 'orders')
        assert bad_df['pipeline_layer'].iloc[0] == 'SILVER'
        assert 'quarantine_reason'               in bad_df.columns

    def test_skips_when_field_not_in_df(self, clean_customers_df):
        good_df, bad_df = check_no_future_dates(clean_customers_df, ['order_date'], 'customers')
        assert len(good_df) == 3
        assert len(bad_df)  == 0


# ─────────────────────────────────────────────
# check_referential_integrity
# ─────────────────────────────────────────────

class TestCheckReferentialIntegrity:

    def test_passes_when_all_fks_exist(self, clean_orders_df, customers_ref_df):
        good_df, bad_df = check_referential_integrity(
            clean_orders_df, 'customer_id', customers_ref_df, 'customer_id', 'orders'
        )
        assert len(good_df) == 3
        assert len(bad_df)  == 0

    def test_catches_orphaned_record(self, customers_ref_df):
        df = pd.DataFrame({
            'order_id':    [1, 2],
            'customer_id': [1, 999],  # 999 does not exist in customers
        })
        good_df, bad_df = check_referential_integrity(
            df, 'customer_id', customers_ref_df, 'customer_id', 'orders'
        )
        assert len(good_df) == 1
        assert len(bad_df)  == 1

    def test_catches_null_fk(self, customers_ref_df):
        df = pd.DataFrame({'order_id': [1], 'customer_id': [None]})
        good_df, bad_df = check_referential_integrity(
            df, 'customer_id', customers_ref_df, 'customer_id', 'orders'
        )
        assert len(bad_df) == 1

    def test_bad_row_has_quarantine_reason(self, customers_ref_df):
        df = pd.DataFrame({'order_id': [1], 'customer_id': [999]})
        _, bad_df = check_referential_integrity(
            df, 'customer_id', customers_ref_df, 'customer_id', 'orders'
        )
        assert 'quarantine_reason' in bad_df.columns
        assert 'customer_id'       in bad_df['quarantine_reason'].iloc[0]


# ─────────────────────────────────────────────
# run_silver_checks
# ─────────────────────────────────────────────

class TestRunSilverChecks:

    def test_clean_data_passes_all_checks(self, clean_customers_df):
        config = {'required_fields': ['customer_id', 'email'], 'check_email': True}
        good_df, bad_df = run_silver_checks(clean_customers_df, 'customers', config)
        assert len(good_df) == 3
        assert len(bad_df)  == 0

    def test_bad_email_quarantined(self):
        df = pd.DataFrame({
            'customer_id': [1, 2],
            'email':       ['good@email.com', 'bad-email'],
        })
        config = {'required_fields': ['customer_id', 'email'], 'check_email': True}
        good_df, bad_df = run_silver_checks(df, 'customers', config)
        assert len(good_df) == 1
        assert len(bad_df)  == 1

    def test_future_date_quarantined(self):
        df = pd.DataFrame({
            'order_id':    [1, 2],
            'customer_id': [1, 2],
            'order_date':  ['2020-01-01', '2099-01-01'],
            'total_amount':[100.0, 200.0],
        })
        config = {'required_fields': ['order_id'], 'date_fields': ['order_date']}
        good_df, bad_df = run_silver_checks(df, 'orders', config)
        assert len(bad_df) == 1

    def test_referential_integrity_via_run(self):
        orders_df = pd.DataFrame({
            'order_id':    [1, 2],
            'customer_id': [1, 999],
        })
        customers_df = pd.DataFrame({'customer_id': [1, 2, 3]})
        config  = {'fk_column': 'customer_id', 'ref_table': 'customers', 'ref_column': 'customer_id'}
        ref_dfs = {'customers': customers_df}
        good_df, bad_df = run_silver_checks(orders_df, 'orders', config, ref_dfs)
        assert len(good_df) == 1
        assert len(bad_df)  == 1
