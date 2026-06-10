# # quality/silver_checks.py
# # Silver layer quality checks
# # More strict than Bronze — checks VALUES and negative/postive test also, not just existence
# # Bad rows  → silver.quarantine  (Databricks Delta table — auto created)
# # Good rows → silver.<table_name> (Databricks Delta table)

# import pandas as pd  # type: ignore[import]
# import logging 
# import re 
# from datetime import datetime
# from typing import Tuple, list, Dict, Any

# logger = logging.getLogger(__name__)

# def check_null_fields(
#     df : pd.DataFrame,
#     columns : List[str],
#     source_table : str) -> Tuple[pd.DataFrame, pd.DataFrame]:
#     """
#     Check null values on important field(not just id's 
#     Also key business fields must not be null. 
#     Args:
#         df           : DataFrame from Bronze layer
#         columns      : List of columns that must not be NULL
#                        e.g. ['email', 'country', 'status']
#         source_table : Source table name e.g. 'customers'

#     Returns:
#         Tuple of (good_df, quarantine_df)
#     """    
    
#     null_mask:     pd.Series    = df[columns].isnull().any(axis=1)
#     bad_df:        pd.DataFrame = df[null_mask].copy()
#     good_df:       pd.DataFrame = df[~null_mask].copy()
    
#     if len(bad_df) > 0 :
#         bad_df['source_table'] = source_table
#         bad_df['quartantine_reason'] = (df[columns].isnull().apply(
#             lamda row: f'Null in [{",  ".join(row[row.index.tolist()])}]',
#             axis = 1 )
#         )
        x
#         bad_df['quartiined_at']  = datetime.utcnow()
#         bad_df['pipline_layer']  = 'SILVER'
        
#         logger.warning(
#             f"[SILVER] Null field check Failed |"
#             f"table = {source_table} |"
#             f"columns = {columns} | "
#             f"bad_rows = {len(bad_df)}"
            
#         )
#     else:
#         logger.info(f"[Silver] NULL field check passed |")
#         f"[table ={source_table} | "
#         f"columns = {columns} | "
#         f"bad_rows = {len(bad_df)}"
#     return good_df, bad_df