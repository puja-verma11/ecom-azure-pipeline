# Databricks notebook source

# COMMAND ----------
# Cell 1 — Create Gold database and reset tables
spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql("DROP TABLE IF EXISTS gold.customer_revenue")
spark.sql("DROP TABLE IF EXISTS gold.product_sales")
spark.sql("DROP TABLE IF EXISTS gold.daily_revenue")
spark.sql("DROP TABLE IF EXISTS gold.orders_by_status")
spark.sql("DROP TABLE IF EXISTS gold.category_performance")
print("Gold database ready")

# COMMAND ----------
# Cell 2 — Revenue per customer
spark.sql("""
    CREATE OR REPLACE TABLE gold.customer_revenue AS
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.country,
        c.city,
        COUNT(o.order_id)        AS total_orders,
        SUM(o.total_amount)      AS total_revenue,
        AVG(o.total_amount)      AS avg_order_value,
        MAX(o.order_date)        AS last_order_date
    FROM silver.customers c
    LEFT JOIN silver.orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name,
             c.email, c.country, c.city
""")
print("gold.customer_revenue done")

# COMMAND ----------
# Cell 3 — Sales per product
spark.sql("""
    CREATE OR REPLACE TABLE gold.product_sales AS
    SELECT
        p.product_id,
        p.product_name,
        p.price             AS unit_price,
        p.stock_qty,
        COUNT(oi.order_item_id)  AS times_ordered,
        SUM(oi.quantity)         AS total_units_sold,
        SUM(oi.line_total)       AS total_revenue
    FROM silver.products p
    LEFT JOIN silver.order_items oi ON p.product_id = oi.product_id
    GROUP BY p.product_id, p.product_name, p.price, p.stock_qty
""")
print("gold.product_sales done")

# COMMAND ----------
# Cell 4 — Daily revenue
spark.sql("""
    CREATE OR REPLACE TABLE gold.daily_revenue AS
    SELECT
        DATE(order_date)         AS order_day,
        COUNT(order_id)          AS total_orders,
        SUM(total_amount)        AS total_revenue,
        AVG(total_amount)        AS avg_order_value
    FROM silver.orders
    GROUP BY DATE(order_date)
    ORDER BY order_day
""")
print("gold.daily_revenue done")

# COMMAND ----------
# Cell 5 — Orders by status
spark.sql("""
    CREATE OR REPLACE TABLE gold.orders_by_status AS
    SELECT
        status,
        COUNT(order_id)          AS total_orders,
        SUM(total_amount)        AS total_revenue,
        AVG(total_amount)        AS avg_order_value
    FROM silver.orders
    GROUP BY status
    ORDER BY total_orders DESC
""")
print("gold.orders_by_status done")

# COMMAND ----------
# Cell 6 — Category performance
spark.sql("""
    CREATE OR REPLACE TABLE gold.category_performance AS
    SELECT
        c.category_id,
        c.category_name,
        COUNT(oi.order_item_id)  AS total_orders,
        SUM(oi.quantity)         AS total_units_sold,
        SUM(oi.line_total)       AS total_revenue
    FROM silver.categories c
    LEFT JOIN silver.products p     ON c.category_id  = p.category_id
    LEFT JOIN silver.order_items oi ON p.product_id   = oi.product_id
    GROUP BY c.category_id, c.category_name
    ORDER BY total_revenue DESC
""")
print("gold.category_performance done")

# COMMAND ----------
# Cell 7 — Verify Gold layer
print("Gold Layer Summary:")
for table in ["customer_revenue", "product_sales", "daily_revenue",
              "orders_by_status", "category_performance"]:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM gold.{table}").collect()[0]["cnt"]
    print(f"   gold.{table:<25} -> {count} rows")
