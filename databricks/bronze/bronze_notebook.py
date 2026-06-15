# Databricks notebook source

# COMMAND ----------
# Cell 1 — Install Azure Storage SDK
%pip install azure-storage-blob

# COMMAND ----------
# Cell 2 — ADLS Configuration via Azure Python SDK
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient

STORAGE_ACCOUNT_NAME = "ecompipelinelakepuja"
CONTAINER_NAME       = "landing"
STORAGE_ACCOUNT_KEY  = dbutils.secrets.get(scope="ecom-secrets", key="storage-account-key")

blob_service_client = BlobServiceClient(
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
    credential  = STORAGE_ACCOUNT_KEY
)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

print("ADLS connection configured via Azure SDK")

# COMMAND ----------
# Cell 3 — Debug: list ALL blobs in landing container to see exact file paths
print("Files found in landing container:")
all_blobs = list(container_client.list_blobs())
for b in all_blobs:
    print(f"  {b.name}  ({b.size} bytes)")

# COMMAND ----------
# Cell 4 — Helper function to read parquet files from ADLS
def read_table_from_adls(table_name: str) -> pd.DataFrame:
    blobs = list(container_client.list_blobs(name_starts_with=f"{table_name}/"))

    print(f"   Found {len(blobs)} blob(s) for {table_name}:")
    for b in blobs:
        print(f"      -> {b.name} ({b.size} bytes)")

    if not blobs:
        print(f"   No files found for {table_name}")
        return pd.DataFrame()

    dfs = []
    for blob in blobs:
        if blob.size == 0:
            print(f"   Skipping empty file: {blob.name}")
            continue
        blob_client = container_client.get_blob_client(blob.name)
        blob_data   = blob_client.download_blob().readall()
        df          = pd.read_parquet(io.BytesIO(blob_data))
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)

print("Helper function defined")

# COMMAND ----------
# Cell 5 — Add repo root to path so we can import quality checks
import sys
sys.path.insert(0, "/Workspace/Users/pujavrma.11@gmail.com/ecom-azure-pipeline")

from quality.bronze_checks import run_bronze_checks

print("Bronze quality checks imported")

# COMMAND ----------
# Cell 6 — Create Bronze database
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

print("Bronze database ready")

# COMMAND ----------
# Cell 7 — Define table configs
TABLE_CONFIGS = {
    "customers":   {"id_col": "customer_id",    "key_cols": ["customer_id"]},
    "categories":  {"id_col": "category_id",    "key_cols": ["category_id"]},
    "products":    {"id_col": "product_id",     "key_cols": ["product_id"]},
    "orders":      {"id_col": "order_id",       "key_cols": ["order_id"]},
    "order_items": {"id_col": "order_item_id",  "key_cols": ["order_item_id"]},
}

print("Table configs defined")

# COMMAND ----------
# Cell 8 — Process each table through bronze quality checks
# Quarantine written per-table to avoid mixed schema issue ✅

for table_name, config in TABLE_CONFIGS.items():
    print(f"\nProcessing {table_name}...")

    df_pandas = read_table_from_adls(table_name)
    print(f"   Read {len(df_pandas)} rows from landing")

    if df_pandas.empty:
        print(f"   Skipping {table_name} — no data found in landing")
        continue

    good_df, bad_df = run_bronze_checks(
        df           = df_pandas,
        id_column    = config["id_col"],
        key_columns  = config["key_cols"],
        source_table = table_name
    )
    print(f"   Good rows : {len(good_df)}")
    print(f"   Bad rows  : {len(bad_df)}")

    # Write good rows to Bronze Delta table
    if not good_df.empty:
        spark.createDataFrame(good_df) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"bronze.{table_name}")
        print(f"   Saved -> bronze.{table_name}")

    # Write bad rows to quarantine immediately (per table — avoids mixed schema!) ✅
    if not bad_df.empty:
        spark.createDataFrame(bad_df) \
            .write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("bronze.quarantine")
        print(f"   {len(bad_df)} bad rows -> bronze.quarantine")

# COMMAND ----------
# Cell 10 — Verify Bronze layer counts
print("Bronze Layer Summary:")
for table_name in TABLE_CONFIGS:
    if spark.catalog.tableExists(f"bronze.{table_name}"):
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM bronze.{table_name}").collect()[0]["cnt"]
        print(f"   bronze.{table_name:<15} -> {count} rows")
    else:
        print(f"   bronze.{table_name:<15} -> table not created (empty data)")

if spark.catalog.tableExists("bronze.quarantine"):
    q_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.quarantine").collect()[0]["cnt"]
    print(f"   bronze.quarantine       -> {q_count} rows")
