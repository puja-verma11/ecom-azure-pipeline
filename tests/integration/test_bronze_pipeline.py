# tests/integration/test_bronze_pipeline.py
# Integration tests for Bronze layer
#
# These tests run AFTER:
#   1. ADF has copied data from Azure SQL → ADLS Gen2
#   2. Databricks Bronze notebook has run
#      (reads ADLS → runs bronze_checks → writes Delta tables)
#
# These tests verify:
#   - bronze.orders Delta table has correct clean rows
#   - bronze.quarantine Delta table has caught bad rows
#
# TODO: implement after Databricks Bronze notebook is set up
# Will use Databricks REST API or Delta table connection
# to query bronze.orders and bronze.quarantine

# Placeholder — tests written in Step D (Databricks setup)