# Databricks notebook source

# COMMAND ----------
# Cell 1 — Install Azure Storage SDK
# Bypasses Spark Connect restriction — no spark.conf.set needed ✅
%pip install azure-storage-blob

# COMMAND ----------
# Cell 2 — ADLS Configuration via Azure Python SDK
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient

STORAGE_ACCOUNT_NAME = "ecompipelinelakepuja"
CONTAINER_NAME       = "landing"
STORAGE_ACCOUNT_KEY  = dbutils.secrets.get(scope="ecom-secrets", key="storage-account-key")

# Connect using Azure SDK (not Spark ADLS connector)
blob_service_client = BlobServiceClient(
    account_url  = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
    credential   = STORAGE_ACCOUNT_KEY
)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

print("✅ ADLS connection configured via Azure SDK")

# COMMAND ----------
# Cell 3 — Helper function to read all parquet files from a folder in ADLS
def read_table_from_adls(table_name: str) -> pd.DataFrame:
    """Read all parquet files from landing/<table_name>/ and return as pandas DataFrame"""
    blobs = list(container_client.list_blobs(name_starts_with=f"{table_name}/"))

    if not blobs:
        print(f"   ⚠️  No files found for {table_name}")
        return pd.DataFrame()

    dfs = []
    for blob in blobs:
        blob_client = container_client.get_blob_client(blob.name)
        blob_data   = blob_client.download_blob().readall()
        df          = pd.read_parquet(io.BytesIO(blob_data))
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

print("✅ Helper function defined")

# COMMAND ----------
# Cell 4 — Add repo root to path so we can import quality checks
import sys
sys.path.insert(0, "/Workspace/Users/pujavrma.11@gmail.com/ecom-azure-pipeline")

from quality.bronze_checks import run_bronze_checks

print("✅ Bronze quality checks imported")

# COMMAND ----------
# Cell 5 — Create Bronze database
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

print("✅ Bronze database ready")

# COMMAND ----------
# Cell 6 — Define table configs
TABLE_CONFIGS = {
    "customers":   {"id_col": "customer_id",    "key_cols": ["customer_id"]},
    "categories":  {"id_col": "category_id",    "key_cols": ["category_id"]},
    "products":    {"id_col": "product_id",     "key_cols": ["product_id"]},
    "orders":      {"id_col": "order_id",       "key_cols": ["order_id"]},
    "order_items": {"id_col": "order_item_id",  "key_cols": ["order_item_id"]},
}

print("✅ Table configs defined")

# COMMAND ----------
# Cell 7 — Process each table through bronze quality checks
all_quarantine = []

for table_name, config in TABLE_CONFIGS.items():
    print(f"\n⏳ Processing {table_name}...")

    # Read parquet from ADLS via Azure SDK → pandas (no spark.conf.set needed!)
    df_pandas = read_table_from_adls(table_name)
    print(f"   📥 Read {len(df_pandas)} rows from landing")

    # Run bronze quality checks (null ID + deduplication)
    good_df, bad_df = run_bronze_checks(
        df           = df_pandas,
        id_column    = config["id_col"],
        key_columns  = config["key_cols"],
        source_table = table_name
    )
    print(f"   ✅ Good rows : {len(good_df)}")
    print(f"   ❌ Bad rows  : {len(bad_df)}")

    # Convert pandas → Spark → write as Delta table
    spark.createDataFrame(good_df) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"bronze.{table_name}")
    print(f"   💾 Saved → bronze.{table_name}")

    if len(bad_df) > 0:
        all_quarantine.append(bad_df)

# COMMAND ----------
# Cell 8 — Write quarantine table
if all_quarantine:
    quarantine_df = pd.concat(all_quarantine, ignore_index=True)
    spark.createDataFrame(quarantine_df) \
        .write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("bronze.quarantine")
    print(f"\n⚠️  {len(quarantine_df)} bad rows written to bronze.quarantine")
else:
    print("\n✅ No bad rows found — nothing quarantined")

# COMMAND ----------
# Cell 9 — Verify Bronze layer counts
print("\n📊 Bronze Layer Summary:")
for table_name in TABLE_CONFIGS:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM bronze.{table_name}").collect()[0]["cnt"]
    print(f"   bronze.{table_name:<15} → {count} rows")

if spark.catalog.tableExists("bronze.quarantine"):
    q_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.quarantine").collect()[0]["cnt"]
    print(f"   bronze.quarantine       → {q_count} rows")
