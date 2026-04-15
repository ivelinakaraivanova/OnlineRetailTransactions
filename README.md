# Online Retail Transactions — ETL Pipeline

End-to-end ETL pipeline for processing Online Retail transactional data using **PySpark**, **PostgreSQL**, and **Apache Airflow**, all running on **Docker**.

## Architecture

**Medallion Architecture**: Raw CSV → Bronze (Parquet) → Silver (cleaned Parquet) → Gold (KPI Parquet) → PostgreSQL star schema

```
raw_to_bronze → dq_bronze → bronze_to_silver → dq_silver → silver_to_postgresql → gold_kpis
```

See [docs/architecture.md](docs/architecture.md) for the full Mermaid diagram.

## Tech Stack

| Technology | Purpose |
|------------|---------|
| PySpark 3.5 | ETL transformations, DQ checks, KPI computation |
| PostgreSQL | Star schema warehouse (dim/fact tables, KPI views) |
| Apache Airflow 3.1 | Pipeline orchestration via DockerOperator |
| Docker | All components containerized on `spark-net` network |
| Jupyter Notebook | Exploratory data analysis |

## Project Structure

```
├── data/
│   ├── raw/            # Original CSV (immutable)
│   ├── bronze/         # Raw → Parquet (no cleaning)
│   ├── silver/         # Cleaned, validated Parquet
│   └── gold/           # KPI aggregations (Parquet)
├── src/
│   ├── pyspark/        # ETL scripts, DQ checks, shared config
│   ├── sql/            # Star schema, KPI views, analytics queries
│   └── airflow/        # Airflow DAG
├── notebooks/          # EDA notebook
├── docs/               # Architecture, data dictionary, DQ spec, runbook
├── logs/               # Script logs and DQ reports
└── dashboards/         # Power BI dashboard
```

## Data Pipeline

| Step | Script | Input | Output | Rows |
|------|--------|-------|--------|------|
| 1 | raw_to_bronze.py | CSV | Bronze Parquet | 541,909 |
| 2 | dq_bronze.py | Bronze | DQ report (observational) | — |
| 3 | bronze_to_silver.py | Bronze | Silver Parquet | 519,607 |
| 4 | dq_silver.py | Silver | DQ report (enforcement) | — |
| 5 | silver_to_postgresql.py | Silver | PostgreSQL table | 519,607 |
| 6 | gold_kpis.py | Silver | Gold Parquet (3 datasets) | 305 / 4,346 / 4,043 |

## Silver Cleaning Rules

1. Drop rows with NULL InvoiceNo or StockCode
2. Remove Quantity ≤ 0 and UnitPrice ≤ 0
3. Remove cancellations (InvoiceNo starting with "C")
4. Uppercase StockCode, trim Description
5. Drop duplicates by (InvoiceNo, StockCode, InvoiceDate)
6. Cast CustomerID to int
7. Add Amount = round(Quantity × UnitPrice, 2)

## PostgreSQL Star Schema

- **dim_customer** — customer_id, country
- **dim_product** — stock_code, description
- **dim_date** — date_id, year, quarter, month, day, day_of_week, day_name, month_name
- **fact_sales** — invoice_no, stock_code, date_id, customer_id, quantity, unit_price, amount
- **KPI Views**: v_daily_sales, v_customer_kpis, v_product_kpis

## Quick Start

1. Start Docker containers (see [docs/runbook.md](docs/runbook.md))
2. Create `.env` in project root:
   ```
   POSTGRES_HOST=my-postgres
   POSTGRES_PORT=5432
   POSTGRES_DB=Retail_DB
   POSTGRES_USER=<your_user>
   POSTGRES_PASSWORD=<your_password>
   ```
3. Run the pipeline manually or via Airflow (see runbook)

## Documentation

- [Architecture Diagram](docs/architecture.md)
- [Data Dictionary](docs/data_dictionary.md)
- [DQ & KPI Specification](docs/dq_and_kpi_spec.md)
- [Runbook](docs/runbook.md)

## Dataset

[Online Retail Dataset](https://archive.ics.uci.edu/ml/datasets/online+retail) — 541,909 transactions from a UK-based online retailer (Dec 2010 – Dec 2011).
