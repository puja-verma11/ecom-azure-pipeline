# Databricks notebook source

# COMMAND ----------
# Cell 1 — Add repo to path
import sys
sys.path.insert(0, "/Workspace/Users/pujavrma.11@gmail.com/ecom-azure-pipeline")

from quality.silver_checks import run_silver_checks
print("Silver quality checks imported")

# COMMAND ----------
# Cell 2 — Create Silver database
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
print("Silver database ready")

# COMMAND ----------
# Cell 3 — Define silver checks config per table
TABLE_CONFIGS = {
    "customers": {
        "required_fields": ["customer_id", "email"],
        "check_email":     True,
    },
    "categories": {
        "required_fields": ["category_id", "category_name"],
    },
    "products": {
        "required_fields": ["product_id", "product_name"],
        "positive_fields": ["price"],
    },
    "orders": {
        "required_fields": ["order_id", "customer_id"],
        "date_fields":     ["order_date"],
        "fk_column":       "customer_id",
        "ref_table":       "customers",
        "ref_column":      "customer_id",
    },
    "order_items": {
        "required_fields": ["order_item_id", "order_id", "product_id"],
        "positive_fields": ["quantity", "price"],
        "fk_column":       "order_id",
        "ref_table":       "orders",
        "ref_column":      "order_id",
    },
}

# COMMAND ----------
# Cell 4 — Load reference tables from Bronze (needed for referential integrity)
ref_dfs = {}
for ref_table in ["customers", "orders"]:
    if spark.catalog.tableExists(f"bronze.{ref_table}"):
        ref_dfs[ref_table] = spark.table(f"bronze.{ref_table}").toPandas()
        print(f"   Loaded bronze.{ref_table} as reference")

# COMMAND ----------
# Cell 5 — Process each table through silver checks
for table_name, config in TABLE_CONFIGS.items():
    print(f"\nProcessing {table_name}...")

    if not spark.catalog.tableExists(f"bronze.{table_name}"):
        print(f"   Skipping — bronze.{table_name} not found")
        continue

    df_pandas = spark.table(f"bronze.{table_name}").toPandas()
    print(f"   Read {len(df_pandas)} rows from bronze")

    good_df, bad_df = run_silver_checks(
        df           = df_pandas,
        source_table = table_name,
        config       = config,
        ref_dfs      = ref_dfs
    )
    print(f"   Good rows : {len(good_df)}")
    print(f"   Bad rows  : {len(bad_df)}")

    if not good_df.empty:
        spark.createDataFrame(good_df) \
            .write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"silver.{table_name}")
        print(f"   Saved -> silver.{table_name}")

    if not bad_df.empty:
        spark.createDataFrame(bad_df) \
            .write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable("silver.quarantine")
        print(f"   {len(bad_df)} bad rows -> silver.quarantine")

# COMMAND ----------
# Cell 6 — Verify Silver layer counts
print("Silver Layer Summary:")
for table_name in TABLE_CONFIGS:
    if spark.catalog.tableExists(f"silver.{table_name}"):
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM silver.{table_name}").collect()[0]["cnt"]
        print(f"   silver.{table_name:<15} -> {count} rows")

if spark.catalog.tableExists("silver.quarantine"):
    q_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.quarantine").collect()[0]["cnt"]
    print(f"   silver.quarantine       -> {q_count} rows")