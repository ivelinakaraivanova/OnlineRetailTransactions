# Data Dictionary

## Raw Layer

Source: `data/raw/online_retail.csv` (541,909 rows)

| Column | Type | Description |
|--------|------|-------------|
| InvoiceNo | string | Invoice number. Prefix "C" = cancellation |
| StockCode | string | Product code |
| Description | string | Product name |
| Quantity | int | Number of items per line |
| InvoiceDate | string | Invoice date and time |
| UnitPrice | double | Price per unit (GBP) |
| CustomerID | double | Customer identifier (nullable) |
| Country | string | Customer's country |

## Bronze Layer

Source: `data/bronze/online_retail.parquet` (541,909 rows)

Exact copy of Raw with schema enforcement (inferSchema) and a metadata column.

| Column | Type | Description |
|--------|------|-------------|
| InvoiceNo | string | Unchanged |
| StockCode | string | Unchanged |
| Description | string | Unchanged |
| Quantity | int | Unchanged |
| InvoiceDate | timestamp | Parsed from string |
| UnitPrice | double | Unchanged |
| CustomerID | double | Unchanged (nullable) |
| Country | string | Unchanged |
| _processed_at | timestamp | Ingestion timestamp |

## Silver Layer

Source: `data/silver/transactions_clean` (519,607 rows)

Cleaned and enriched. 7 transformation rules applied (see `dq_and_kpi_spec.md`).

| Column | Type | Transformation |
|--------|------|----------------|
| InvoiceNo | string | Cancellations (prefix "C") removed |
| StockCode | string | Uppercased |
| Description | string | Trimmed |
| Quantity | int | Rows with Quantity ≤ 0 removed |
| InvoiceDate | timestamp | Unchanged |
| UnitPrice | double | Rows with UnitPrice ≤ 0 removed |
| CustomerID | int | Cast from double to int |
| Country | string | Unchanged |
| Amount | double | Added: round(Quantity × UnitPrice, 2) |
| _processed_at | timestamp | Processing timestamp |

## Gold Layer

### daily_sales (`data/gold/daily_sales`, 305 rows)

| Column | Type | Derivation |
|--------|------|------------|
| Date | date | to_date(InvoiceDate) |
| total_orders | long | countDistinct(InvoiceNo) |
| total_items | long | sum(Quantity) |
| total_revenue | double | round(sum(Amount), 2) |
| avg_order_value | double | round(avg(Amount), 2) |

### customer_kpis (`data/gold/customer_kpis`, 4,346 rows)

| Column | Type | Derivation |
|--------|------|------------|
| CustomerID | int | Group key (non-null only) |
| Country | string | Group key |
| total_orders | long | countDistinct(InvoiceNo) |
| total_items | long | sum(Quantity) |
| total_spent | double | round(sum(Amount), 2) |
| avg_item_value | double | round(avg(Amount), 2) |
| first_purchase | date | min(Date) |
| last_purchase | date | max(Date) |

### product_kpis (`data/gold/product_kpis`, 4,043 rows)

| Column | Type | Derivation |
|--------|------|------------|
| StockCode | string | Group key |
| Description | string | Group key |
| total_orders | long | countDistinct(InvoiceNo) |
| total_sold | long | sum(Quantity) |
| total_revenue | double | round(sum(Amount), 2) |
| avg_price | double | round(avg(UnitPrice), 2) |
| first_sold | date | min(Date) |
| last_sold | date | max(Date) |

## PostgreSQL Star Schema (Retail_DB)

### dim_customer

| Column | Type | Source |
|--------|------|--------|
| customer_id | int (PK) | CustomerID |
| country | text | MAX(Country) per customer |

### dim_product

| Column | Type | Source |
|--------|------|--------|
| stock_code | text (PK) | StockCode |
| description | text | MAX(Description) per product |

### dim_date

| Column | Type | Source |
|--------|------|--------|
| date_id | date (PK) | CAST(InvoiceDate AS DATE) |
| year | numeric | EXTRACT(YEAR) |
| quarter | numeric | EXTRACT(QUARTER) |
| month | numeric | EXTRACT(MONTH) |
| day | numeric | EXTRACT(DAY) |
| day_of_week | numeric | EXTRACT(DOW) — 0=Sun, 6=Sat |
| day_name | text | TO_CHAR('Day') |
| month_name | text | TO_CHAR('Month') |

### fact_sales

| Column | Type | FK |
|--------|------|----|
| invoice_no | text | — |
| stock_code | text | → dim_product(stock_code) |
| date_id | date | → dim_date(date_id) |
| customer_id | int | No FK (NULLs exist) |
| quantity | int | — |
| unit_price | double precision | — |
| amount | double precision | — |

## PostgreSQL KPI Views

### v_daily_sales

| Column | Type | Source |
|--------|------|--------|
| date_id | date | dim_date.date_id |
| year | numeric | dim_date.year |
| month | numeric | dim_date.month |
| day_name | text | dim_date.day_name |
| total_orders | bigint | COUNT(DISTINCT invoice_no) |
| total_items | bigint | SUM(quantity) |
| total_revenue | numeric | ROUND(SUM(amount), 2) |
| avg_order_value | numeric | ROUND(AVG(amount), 2) |

### v_customer_kpis

| Column | Type | Source |
|--------|------|--------|
| customer_id | int | dim_customer.customer_id |
| country | text | dim_customer.country |
| total_orders | bigint | COUNT(DISTINCT invoice_no) |
| total_items | bigint | SUM(quantity) |
| total_spent | numeric | ROUND(SUM(amount), 2) |
| avg_order_value | numeric | ROUND(AVG(amount), 2) |
| first_purchase | date | MIN(date_id) |
| last_purchase | date | MAX(date_id) |

### v_product_kpis

| Column | Type | Source |
|--------|------|--------|
| stock_code | text | dim_product.stock_code |
| description | text | dim_product.description |
| total_orders | bigint | COUNT(DISTINCT invoice_no) |
| total_sold | bigint | SUM(quantity) |
| total_revenue | numeric | ROUND(SUM(amount), 2) |
| avg_unit_price | numeric | ROUND(AVG(unit_price), 2) |
| first_sold | date | MIN(date_id) |
| last_sold | date | MAX(date_id) |