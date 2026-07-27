# eCommerce Azure Data Pipeline

An end-to-end ELT data pipeline built on Azure, implementing the Medallion Architecture (Bronze → Silver → Gold) with automated data quality checks.

---

## Architecture

```
Azure SQL (Source)
      │
      ▼
Azure Data Factory (ADF)
  ├── Full Load Pipeline        → loads all data on first run
  └── Incremental Pipeline      → loads only new records (watermark pattern)
      │
      ▼
ADLS Gen2 - Landing Zone        → raw parquet files
      │
      ▼
Databricks — Bronze Layer       → raw data + null/duplicate checks
      │
      ▼
Databricks — Silver Layer       → cleaned data + quality checks
      │
      ▼
Databricks — Gold Layer         → business metrics & aggregations
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Azure SQL Database | Source system (eCommerce data) |
| Azure Data Factory | Data ingestion (full + incremental load) |
| Azure Data Lake Gen2 | Landing zone (parquet files) |
| Azure Databricks | Data transformation (Bronze/Silver/Gold) |
| Delta Lake | ACID-compliant table storage |
| Python + Pandas | Data quality check framework |
| GitHub Actions | CI/CD — runs tests on every push |

---

## Project Structure

```
ecom-azure-pipeline/
├── databricks/
│   ├── bronze/
│   │   └── bronze_notebook.py      # reads ADLS → quality checks → Bronze Delta tables
│   ├── silver/
│   │   └── silver_notebook.py      # Bronze → silver checks → Silver Delta tables
│   └── gold/
│       └── gold_notebook.py        # Silver → business metrics → Gold Delta tables
├── quality/
│   ├── bronze_checks.py            # null ID + duplicate checks
│   └── silver_checks.py            # future dates, email, positive values, referential integrity
├── tests/
│   └── unit/
│       ├── test_bronze_checks.py   # unit tests for bronze quality checks
│       └── test_silver_checks.py   # unit tests for silver quality checks
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions CI/CD pipeline
└── data/
    └── README.md                   # data schema documentation
```

---

## Medallion Architecture

### Bronze Layer
- Reads raw parquet files from ADLS Gen2 landing zone
- Runs basic quality checks: null IDs, duplicate records
- Bad rows → `bronze.quarantine` with reason tagged
- Good rows → `bronze.{table_name}` Delta tables

### Silver Layer
- Reads from Bronze Delta tables
- Runs deeper quality checks:
  - Null required fields
  - Invalid email format
  - Non-positive values (price, quantity)
  - Future dates in order_date
  - Referential integrity (customer_id in orders exists in customers)
- Bad rows → `silver.quarantine`
- Good rows → `silver.{table_name}` Delta tables

### Gold Layer
Business metrics built from clean Silver data:

| Table | Description |
|-------|-------------|
| `gold.customer_revenue` | Total revenue, order count per customer |
| `gold.product_sales` | Units sold, revenue per product |
| `gold.daily_revenue` | Revenue trend by day |
| `gold.orders_by_status` | Order counts by status (pending, delivered etc.) |
| `gold.category_performance` | Revenue per product category |

---

## Data Quality Framework

```python
# Example — silver quality checks
good_df, bad_df = run_silver_checks(
    df           = orders_df,
    source_table = 'orders',
    config       = {
        'required_fields': ['order_id', 'customer_id'],
        'date_fields':     ['order_date'],
        'fk_column':       'customer_id',
        'ref_table':       'customers',
        'ref_column':      'customer_id',
    },
    ref_dfs = {'customers': customers_df}
)
```

Bad rows are tagged with:
- `source_table` — which table the bad row came from
- `quarantine_reason` — why it was rejected
- `quarantined_at` — timestamp when it was caught
- `pipeline_layer` — BRONZE or SILVER

---

## ADF Incremental Load — Watermark Pattern

```
Run 1: load all records (full load)
Run 2: load only records where updated_at > last_run_timestamp
Run 3: load only records where updated_at > last_run_timestamp
```

Watermark stored in `bo.watermarktable` in Azure SQL. Updated after each successful run.

---

## Running Tests Locally

```bash
# Install dependencies
pip install pandas pytest pytest-cov pyarrow

# Run all unit tests
pytest tests/unit/ -v

# Run with coverage report
pytest tests/unit/ -v --cov=quality --cov-report=term-missing
```

---

## CI/CD

GitHub Actions runs automatically on every push to `main` or `feature/*` branches:
1. Installs dependencies
2. Runs Bronze unit tests
3. Runs Silver unit tests
4. Reports test coverage

---

## Source Data

5 tables from Azure SQL eCommerce database:

| Table | Description | Rows |
|-------|-------------|------|
| customers | Customer profiles | 24 |
| categories | Product categories | 12 |
| products | Product catalogue | 30 |
| orders | Customer orders | 53 |
| order_items | Line items per order | 89 |
