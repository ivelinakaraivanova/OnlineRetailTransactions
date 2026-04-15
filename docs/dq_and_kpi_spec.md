# Data Quality & KPI Specification

## DQ Checks — Bronze (Observational)

Script: `src/pyspark/dq_bronze.py`
Report: `logs/dq_bronze_report.txt`
Mode: **Observational** — logs findings, does not block the pipeline.

| Category | Check | Description |
|----------|-------|-------------|
| Volume | Row count | Total rows in Bronze Parquet |
| Completeness | NULL counts | Count NULLs per column (InvoiceNo, StockCode, Description, Quantity, UnitPrice, InvoiceDate, CustomerID, Country) with percentage |
| Validity | Quantity ≤ 0 | Count of non-positive quantities |
| Validity | UnitPrice ≤ 0 | Count of non-positive prices |
| Validity | NULL dates | Count of NULL InvoiceDate values |
| Uniqueness | Duplicates | Count of duplicate rows by (InvoiceNo, StockCode, InvoiceDate) |
| Business Rules | Cancellations | Count of invoices starting with "C" |
| Type Checks | Schema types | List all column names and data types |

## DQ Checks — Silver (Enforcement)

Script: `src/pyspark/dq_silver.py`
Report: `logs/dq_silver_report.txt`
Mode: **Enforcement** — each check produces PASS/FAIL. If any check fails, the script exits with code 1, blocking downstream tasks in Airflow.

| Category | Check | Expected | PASS/FAIL |
|----------|-------|----------|-----------|
| Completeness | NULL InvoiceNo | 0 | count > 0 → FAIL |
| Completeness | NULL StockCode | 0 | count > 0 → FAIL |
| Completeness | NULL Quantity | 0 | count > 0 → FAIL |
| Completeness | NULL UnitPrice | 0 | count > 0 → FAIL |
| Completeness | NULL InvoiceDate | 0 | count > 0 → FAIL |
| Validity | Quantity ≤ 0 | 0 | count > 0 → FAIL |
| Validity | UnitPrice ≤ 0 | 0 | count > 0 → FAIL |
| Validity | NULL dates | 0 | count > 0 → FAIL |
| Uniqueness | Duplicates (InvoiceNo+StockCode+InvoiceDate) | 0 | count > 0 → FAIL |
| Business Rules | Cancellations (InvoiceNo starts with "C") | 0 | count > 0 → FAIL |
| Type Checks | InvoiceDate=timestamp, Quantity=int, UnitPrice=double, Amount=double, CustomerID=int | exact match | mismatch → FAIL |

## Silver Cleaning Rules

Applied in `src/pyspark/bronze_to_silver.py`, in this order:

| # | Rule | Effect |
|---|------|--------|
| 1 | Drop NULL InvoiceNo or StockCode | Remove rows with missing keys |
| 2 | Remove Quantity ≤ 0 | No zero/negative quantities |
| 3 | Remove UnitPrice ≤ 0 | No zero/negative prices |
| 4 | Remove cancellations | InvoiceNo starting with "C" |
| 5 | Text cleaning | upper(StockCode), trim(Description) |
| 6 | Drop duplicates | By (InvoiceNo, StockCode, InvoiceDate) — **after** text cleaning |
| 7 | Cast CustomerID | double → int |
| 8 | Add Amount | round(Quantity × UnitPrice, 2) |
| 9 | Add _processed_at | current_timestamp() |

> **Note:** Text cleaning (step 5) is done **before** dedup (step 6) to avoid false duplicates caused by case differences in StockCode.

## KPI Definitions — Gold Layer (PySpark)

Script: `src/pyspark/gold_kpis.py`
Source: Silver Parquet (`data/silver/transactions_clean`)

### daily_sales

| KPI | Formula |
|-----|---------|
| total_orders | COUNT(DISTINCT InvoiceNo) per Date |
| total_items | SUM(Quantity) per Date |
| total_revenue | ROUND(SUM(Amount), 2) per Date |
| avg_order_value | ROUND(AVG(Amount), 2) per Date |

### customer_kpis

Filtered to non-null CustomerID only.

| KPI | Formula |
|-----|---------|
| total_orders | COUNT(DISTINCT InvoiceNo) per (CustomerID, Country) |
| total_items | SUM(Quantity) per (CustomerID, Country) |
| total_spent | ROUND(SUM(Amount), 2) per (CustomerID, Country) |
| avg_item_value | ROUND(AVG(Amount), 2) per (CustomerID, Country) |
| first_purchase | MIN(Date) per (CustomerID, Country) |
| last_purchase | MAX(Date) per (CustomerID, Country) |

### product_kpis

| KPI | Formula |
|-----|---------|
| total_orders | COUNT(DISTINCT InvoiceNo) per (StockCode, Description) |
| total_sold | SUM(Quantity) per (StockCode, Description) |
| total_revenue | ROUND(SUM(Amount), 2) per (StockCode, Description) |
| avg_price | ROUND(AVG(UnitPrice), 2) per (StockCode, Description) |
| first_sold | MIN(Date) per (StockCode, Description) |
| last_sold | MAX(Date) per (StockCode, Description) |

## KPI Definitions — PostgreSQL Views

Script: `src/sql/create_kpi_views.sql`
Source: Star schema tables in Retail_DB

Same logic as Gold PySpark KPIs but with additional dimension columns from joins:

- **v_daily_sales**: adds year, month, day_name from dim_date
- **v_customer_kpis**: adds country from dim_customer
- **v_product_kpis**: adds description from dim_product