# Pipeline Architecture

## Overview

Online Retail ETL pipeline using the **Medallion Architecture** pattern
(Bronze → Silver → Gold), orchestrated by Apache Airflow on Docker.

## Architecture Diagram

```mermaid
flowchart LR
    subgraph Docker Network: spark-net
        subgraph Airflow
            DAG[retail_daily_pipeline<br/>schedule: @daily]
        end

        subgraph "Spark Containers (ephemeral)"
            R2B[raw_to_bronze.py]
            DQB[dq_bronze.py]
            B2S[bronze_to_silver.py]
            DQS[dq_silver.py]
            S2P[silver_to_postgresql.py]
            GK[gold_kpis.py]
        end

        PG[(PostgreSQL<br/>my-postgres)]
    end

    CSV[/"online_retail.csv<br/>(Raw)"/] --> R2B
    R2B -->|Parquet| Bronze[("data/bronze/<br/>online_retail.parquet")]
    Bronze --> DQB
    DQB -->|observational<br/>report| LOG1[/"dq_bronze_report.txt"/]
    Bronze --> B2S
    B2S -->|cleaned<br/>Parquet| Silver[("data/silver/<br/>transactions_clean")]
    Silver --> DQS
    DQS -->|enforcement<br/>report| LOG2[/"dq_silver_report.txt"/]
    Silver --> S2P
    S2P -->|JDBC| PG
    Silver --> GK
    GK -->|Parquet| Gold[("data/gold/<br/>daily_sales<br/>customer_kpis<br/>product_kpis")]

    DAG -.->|DockerOperator| R2B
    DAG -.->|DockerOperator| DQB
    DAG -.->|DockerOperator| B2S
    DAG -.->|DockerOperator| DQS
    DAG -.->|DockerOperator| S2P
    DAG -.->|DockerOperator| GK

    subgraph "PostgreSQL Star Schema"
        PG --> DIM_C[dim_customer]
        PG --> DIM_P[dim_product]
        PG --> DIM_D[dim_date]
        PG --> FACT[fact_sales]
        PG --> V1[v_daily_sales]
        PG --> V2[v_customer_kpis]
        PG --> V3[v_product_kpis]
    end
```

## Task Execution Order

```
raw_to_bronze → dq_bronze → bronze_to_silver → dq_silver → silver_to_postgresql → gold_kpis
```

## Infrastructure

| Component | Container | Image | Port |
|-----------|-----------|-------|------|
| Spark / Jupyter | spark-jupyter | jupyter/pyspark-notebook | 8888 |
| PostgreSQL | my-postgres | postgres | 5432 |
| Airflow | airflow | apache/airflow | 8080 |

All containers run on the `spark-net` Docker network.

## Data Flow Summary

| Layer | Format | Location | Records |
|-------|--------|----------|---------|
| Raw | CSV | data/raw/online_retail.csv | 541,909 |
| Bronze | Parquet | data/bronze/online_retail.parquet | 541,909 |
| Silver | Parquet | data/silver/transactions_clean | 519,607 |
| Gold | Parquet | data/gold/{daily_sales, customer_kpis, product_kpis} | 305 / 4,346 / 4,043 |
| PostgreSQL | Tables | Retail_DB.public.* | Star schema |
