# quality/bronze_checks.py
# Bronze layer quality checks
# Bad rows  → bronze.quarantine  (Databricks Delta table — auto created)
# Good rows → bronze.<table_name> (Databricks Delta table)

import pandas as pd
import logging
from datetime  import datetime
from typing    import Tuple, List

logger = logging.getLogger(__name__)


def check_null_ids(
    df:           pd.DataFrame,
    id_column:    str,
    source_table: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Finds rows where the primary key column is NULL.

    Args:
        df           : Raw DataFrame coming from ADF copy
        id_column    : Name of the primary key column  e.g. 'order_id'
        source_table : Name of the source table        e.g. 'orders'

    Returns:
        Tuple of (good_df, quarantine_df)
        good_df      : pd.DataFrame — rows where id_column is NOT NULL
        quarantine_df: pd.DataFrame — rows where id_column IS NULL
                       + extra columns: source_table, quarantine_reason,
                                        quarantined_at, pipeline_layer
    """
    bad_df:  pd.DataFrame = df[df[id_column].isnull()].copy()
    good_df: pd.DataFrame = df[df[id_column].notnull()].copy()

    if len(bad_df) > 0:
        bad_df['source_table']      = source_table
        bad_df['quarantine_reason'] = f'NULL value in {id_column}'
        bad_df['quarantined_at']    = datetime.utcnow()
        bad_df['pipeline_layer']    = 'BRONZE'

        logger.warning(
            f"[BRONZE] NULL CHECK FAILED | "
            f"table={source_table} | "
            f"column={id_column} | "
            f"bad_rows={len(bad_df)}"
        )
    else:
        logger.info(
            f"[BRONZE] NULL CHECK PASSED | "
            f"table={source_table} | "
            f"column={id_column}"
        )

    return good_df, bad_df


def check_duplicates(
    df:           pd.DataFrame,
    key_columns:  List[str],
    source_table: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Finds duplicate rows based on key columns.
    Keeps FIRST occurrence as good, quarantines the rest.

    Args:
        df           : DataFrame after null check
        key_columns  : List of columns to check for duplicates
                       e.g. ['order_id'] or ['customer_id', 'order_date']
        source_table : Name of the source table e.g. 'orders'

    Returns:
        Tuple of (good_df, quarantine_df)
        good_df      : pd.DataFrame — deduplicated rows (first occurrence kept)
        quarantine_df: pd.DataFrame — duplicate rows
                       + extra columns: source_table, quarantine_reason,
                                        quarantined_at, pipeline_layer
    """
    is_duplicate: pd.Series    = df.duplicated(subset=key_columns, keep='first')
    bad_df:       pd.DataFrame = df[is_duplicate].copy()
    good_df:      pd.DataFrame = df[~is_duplicate].copy()

    if len(bad_df) > 0:
        bad_df['source_table']      = source_table
        bad_df['quarantine_reason'] = f'DUPLICATE on {key_columns}'
        bad_df['quarantined_at']    = datetime.utcnow()
        bad_df['pipeline_layer']    = 'BRONZE'

        logger.warning(
            f"[BRONZE] DUPLICATE CHECK FAILED | "
            f"table={source_table} | "
            f"key_columns={key_columns} | "
            f"duplicate_rows={len(bad_df)}"
        )
    else:
        logger.info(
            f"[BRONZE] DUPLICATE CHECK PASSED | "
            f"table={source_table} | "
            f"key_columns={key_columns}"
        )

    return good_df, bad_df


def run_bronze_checks(
    df:           pd.DataFrame,
    id_column:    str,
    key_columns:  List[str],
    source_table: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Runs ALL Bronze checks in sequence on a DataFrame.
    Called by the Databricks Bronze notebook for every table.

    Checks performed:
        1. NULL check on primary key column
        2. Duplicate check on key columns

    Args:
        df           : Raw DataFrame from ADF copy activity
        id_column    : Primary key column name    e.g. 'order_id'
        key_columns  : Columns to dedup on        e.g. ['order_id']
        source_table : Source table name          e.g. 'orders'

    Returns:
        Tuple of (clean_df, quarantine_df)
        clean_df     : pd.DataFrame — rows that passed ALL checks
                       → written to bronze.<source_table> Delta table
        quarantine_df: pd.DataFrame — rows that FAILED any check
                       → written to bronze.quarantine Delta table
    """
    logger.info(
        f"[BRONZE] Starting checks | "
        f"table={source_table} | "
        f"total_rows={len(df)}"
    )

    all_quarantine: List[pd.DataFrame] = []

    # Step 1 — NULL check
    df, null_bad = check_null_ids(df, id_column, source_table)
    if len(null_bad) > 0:
        all_quarantine.append(null_bad)

    # Step 2 — Duplicate check (runs AFTER null check)
    # Why after? Null rows already removed — cleaner dedup
    df, dup_bad = check_duplicates(df, key_columns, source_table)
    if len(dup_bad) > 0:
        all_quarantine.append(dup_bad)

    # Combine all bad rows into one quarantine DataFrame
    quarantine_df: pd.DataFrame = (
        pd.concat(all_quarantine, ignore_index=True)
        if all_quarantine
        else pd.DataFrame()
    )

    logger.info(
        f"[BRONZE] Checks complete | "
        f"table={source_table} | "
        f"clean_rows={len(df)} | "
        f"quarantined_rows={len(quarantine_df)}"
    )

    return df, quarantine_df