import pandas as pd
import re
import logging
from datetime import datetime
from typing import Tuple, List

logger = logging.getLogger(__name__)


def check_null_fields(df: pd.DataFrame, fields: List[str], source_table: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    mask = df[fields].isnull().any(axis=1)
    bad_df = df[mask].copy()
    good_df = df[~mask].copy()
    if not bad_df.empty:
        bad_df['source_table']      = source_table
        bad_df['quarantine_reason'] = f'NULL value in required fields {fields}'
        bad_df['quarantined_at']    = datetime.utcnow()
        bad_df['pipeline_layer']    = 'SILVER'
    return good_df, bad_df


def check_email_format(df: pd.DataFrame, source_table: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if 'email' not in df.columns:
        return df, pd.DataFrame()
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    mask = ~df['email'].astype(str).str.match(pattern)
    bad_df = df[mask].copy()
    good_df = df[~mask].copy()
    if not bad_df.empty:
        bad_df['source_table']      = source_table
        bad_df['quarantine_reason'] = 'Invalid email format'
        bad_df['quarantined_at']    = datetime.utcnow()
        bad_df['pipeline_layer']    = 'SILVER'
    return good_df, bad_df


def check_positive_values(df: pd.DataFrame, fields: List[str], source_table: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    existing = [f for f in fields if f in df.columns]
    if not existing:
        return df, pd.DataFrame()
    mask = (df[existing] <= 0).any(axis=1)
    bad_df = df[mask].copy()
    good_df = df[~mask].copy()
    if not bad_df.empty:
        bad_df['source_table']      = source_table
        bad_df['quarantine_reason'] = f'Non-positive value in {existing}'
        bad_df['quarantined_at']    = datetime.utcnow()
        bad_df['pipeline_layer']    = 'SILVER'
    return good_df, bad_df


def check_no_future_dates(df: pd.DataFrame, date_fields: List[str], source_table: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    existing = [f for f in date_fields if f in df.columns]
    if not existing:
        return df, pd.DataFrame()
    now = pd.Timestamp.utcnow()
    mask = pd.Series(False, index=df.index)
    for field in existing:
        mask = mask | (pd.to_datetime(df[field], utc=True) > now)
    bad_df = df[mask].copy()
    good_df = df[~mask].copy()
    if not bad_df.empty:
        bad_df['source_table']      = source_table
        bad_df['quarantine_reason'] = f'Future date in {existing}'
        bad_df['quarantined_at']    = datetime.utcnow()
        bad_df['pipeline_layer']    = 'SILVER'
    return good_df, bad_df


def check_referential_integrity(df: pd.DataFrame, fk_column: str, ref_df: pd.DataFrame, ref_column: str, source_table: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    valid_ids = set(ref_df[ref_column].dropna().unique())
    mask = ~df[fk_column].isin(valid_ids)
    bad_df = df[mask].copy()
    good_df = df[~mask].copy()
    if not bad_df.empty:
        bad_df['source_table']      = source_table
        bad_df['quarantine_reason'] = f'{fk_column} not found in {ref_column}'
        bad_df['quarantined_at']    = datetime.utcnow()
        bad_df['pipeline_layer']    = 'SILVER'
    return good_df, bad_df


def run_silver_checks(df: pd.DataFrame, source_table: str, config: dict, ref_dfs: dict = {}) -> Tuple[pd.DataFrame, pd.DataFrame]:
    all_bad: List[pd.DataFrame] = []

    # Null fields check
    if config.get('required_fields'):
        df, bad = check_null_fields(df, config['required_fields'], source_table)
        if not bad.empty: all_bad.append(bad)

    # Email format check
    if config.get('check_email'):
        df, bad = check_email_format(df, source_table)
        if not bad.empty: all_bad.append(bad)

    # Positive values check
    if config.get('positive_fields'):
        df, bad = check_positive_values(df, config['positive_fields'], source_table)
        if not bad.empty: all_bad.append(bad)

    # No future dates check
    if config.get('date_fields'):
        df, bad = check_no_future_dates(df, config['date_fields'], source_table)
        if not bad.empty: all_bad.append(bad)

    # Referential integrity check
    if config.get('fk_column') and config.get('ref_table') in ref_dfs:
        ref_df = ref_dfs[config['ref_table']]
        df, bad = check_referential_integrity(df, config['fk_column'], ref_df, config['ref_column'], source_table)
        if not bad.empty: all_bad.append(bad)

    quarantine_df = pd.concat(all_bad, ignore_index=True) if all_bad else pd.DataFrame()
    return df, quarantine_df