# Databricks notebook source

# COMMAND ----------
# Cell 1 — ADLS Configuration
# Credentials are read from Cluster Environment Variables (set once in cluster config)
# Never stored in code or GitHub ✅
# Cell 1 — ADLS Configuration
# Credentials read from Databricks Secrets — never stored in code or GitHub ✅
STORAGE_ACCOUNT_NAME = "ecompipelinelakepuja"
STORAGE_ACCOUNT_KEY  = dbutils.secrets.get(scope="ecom-secrets", key="storage-account-key")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
    STORAGE_ACCOUNT_KEY
)

LANDING_PATH = f"abfss://landing@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"

print("✅ ADLS connection configured")

# COMMAND ----------
# Cell 2 — Add repo root to path so we can import quality checks
import sys
sys.path.insert(0, "/Workspace/Repos/pujavrma.11@gmail.com/ecom-azure-pipeline")

from quality.bronze_checks import run_bronze_checks

print("✅ Bronze quality checks imported")

# COMMAND ----------
# Cell 3 — Create Bronze database
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

print("✅ Bronze database ready")

# COMMAND ----------
# Cell 4 — Define table configs (which column is the ID, which columns are the key)
TABLE_CONFIGS = {
    "customers":   {"id_col": "customer_id",    "key_cols": ["customer_id"]},
    "categories":  {"id_col": "category_id",    "key_cols": ["category_id"]},
    "products":    {"id_col": "product_id",     "key_cols": ["product_id"]},
    "orders":      {"id_col": "order_id",       "key_cols": ["order_id"]},
    "order_items": {"id_col": "order_item_id",  "key_cols": ["order_item_id"]},
}

print("✅ Table configs defined")

# COMMAND ----------
# Cell 5 — Process each table through bronze quality checks
all_quarantine = []

for table_name, config in TABLE_CONFIGS.items():
    print(f"\n⏳ Processing {table_name}...")

    # Read parquet files from ADLS landing zone
    df_spark  = spark.read.parquet(f"{LANDING_PATH}/{table_name}/")
    df_pandas = df_spark.toPandas()
    print(f"  Read {len(df_pandas)} rows from landing")

    # Run bronze quality checks (null ID + deduplication)
    good_df, bad_df = run_bronze_checks(
        df           = df_pandas,
        id_column    = config["id_col"],
        key_columns  = config["key_cols"],
        source_table = table_name
    )
    print(f"   ✅ Good rows : {len(good_df)}")
    print(f"   ❌ Bad rows  : {len(bad_df)}")

    # Write good data to Bronze Delta table
    spark.createDataFrame(good_df) \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"bronze.{table_name}")
    print(f"   💾 Saved → bronze.{table_name}")

    # Collect bad rows for quarantine
    if len(bad_df) > 0:
        all_quarantine.append(bad_df)

# COMMAND ----------
# Cell 6 — Write quarantine table
import pandas as pd

if all_quarantine:
    quarantine_df = pd.concat(all_quarantine, ignore_index=True)
    spark.createDataFrame(quarantine_df) \
        .write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("bronze.quarantine")
    print(f"\n  {len(quarantine_df)} bad rows written to bronze.quarantine")
else:
    print("\n No bad rows found — nothing quarantined")

# COMMAND ----------
# Cell 7 — Verify Bronze layer counts
print("\n📊 Bronze Layer Summary:")
for table_name in TABLE_CONFIGS:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM bronze.{table_name}").collect()[0]["cnt"]
    print(f"   bronze.{table_name:<15} → {count} rows")

if spark.catalog.tableExists("bronze.quarantine"):
    q_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.quarantine").collect()[0]["cnt"]
    print(f"   bronze.quarantine       → {q_count} rows")
