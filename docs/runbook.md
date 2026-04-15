# Runbook

## Prerequisites

- Docker Desktop installed and running
- `.env` file in project root with PostgreSQL credentials (see `.env.example`)
- Three containers on the `spark-net` network:
  - `spark-jupyter` (jupyter/pyspark-notebook) — port 8888
  - `my-postgres` (postgres) — port 5432
  - `airflow` (apache/airflow) — port 8080

## Starting the Containers

> **Note:** Replace `$POSTGRES_USER`, `$POSTGRES_PASSWORD`, `$POSTGRES_DB` etc.
> with values from your `.env` file.

```bash
# Create network (if not exists)
docker network create spark-net

# PostgreSQL
docker run -d --name my-postgres --network spark-net -p 5432:5432 \
  -e POSTGRES_USER=$POSTGRES_USER -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
  postgres

# Spark / Jupyter
docker run -d --name spark-jupyter --network spark-net -p 8888:8888 \
  -v D:/Prj/PySpark/OnlineRetailTransactions:/home/jovyan/work \
  jupyter/pyspark-notebook

# Airflow
docker run -d --name airflow --network spark-net -p 8080:8080 \
  -v //var/run/docker.sock:/var/run/docker.sock \
  -v D:/Prj/PySpark/OnlineRetailTransactions/src/airflow:/opt/airflow/dags \
  -v D:/Prj/PySpark/OnlineRetailTransactions:/opt/airflow/project \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
  -e POSTGRES_HOST=$POSTGRES_HOST -e POSTGRES_PORT=$POSTGRES_PORT \
  -e POSTGRES_DB=$POSTGRES_DB -e POSTGRES_USER=$POSTGRES_USER \
  -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
  apache/airflow airflow standalone
```

## Running Scripts Manually

All commands run inside the `spark-jupyter` container:

```bash
docker exec -it spark-jupyter bash
cd /home/jovyan/work/src/pyspark
```

### ETL Pipeline (in order)

```bash
spark-submit --py-files config.py,logger.py,utils.py raw_to_bronze.py
spark-submit --py-files config.py,logger.py,utils.py dq_bronze.py
spark-submit --py-files config.py,logger.py,utils.py bronze_to_silver.py
spark-submit --py-files config.py,logger.py,utils.py dq_silver.py
spark-submit --py-files config.py,logger.py,utils.py --jars /home/jovyan/work/jars/postgresql-42.7.1.jar silver_to_postgresql.py
spark-submit --py-files config.py,logger.py,utils.py gold_kpis.py
```

### JDBC Driver

The PostgreSQL JDBC jar must exist at `jars/postgresql-42.7.1.jar`. To download:

```bash
wget -P /home/jovyan/work/jars https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
```

## Running via Airflow

1. Open http://localhost:8080
2. Get credentials:
   ```bash
   docker exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
   ```
3. Login with username `admin` and the generated password
4. Find `retail_daily_pipeline` in the DAG list
5. Toggle the DAG to "Active" (unpause)
6. Click "Trigger DAG" to run manually, or let the `@daily` schedule handle it

## PostgreSQL Setup

### Create Database and Star Schema

1. Connect to PostgreSQL (DBeaver or psql) using credentials from `.env`
2. Create the database: `CREATE DATABASE "Retail_DB";`
3. Connect to the database and run `src/sql/create_star_schema.sql`
4. Run `src/sql/create_kpi_views.sql`

### Sample Queries

See `src/sql/analytics_queries.sql` for 8 ready-to-use BI queries.

## Log Files

All logs are written to the `logs/` directory:

| File | Content |
|------|---------|
| raw_to_bronze.log | Bronze ingestion log |
| bronze_to_silver.log | Silver cleaning log |
| silver_to_postgresql.log | PostgreSQL load log |
| gold_kpis.log | Gold KPI computation log |
| dq_bronze.log | Bronze DQ check log |
| dq_bronze_report.txt | Bronze DQ report (observational) |
| dq_silver.log | Silver DQ check log |
| dq_silver_report.txt | Silver DQ report (PASS/FAIL) |

## Troubleshooting

### "ModuleNotFoundError: No module named 'dotenv'"

This happens in Airflow-spawned containers. The `config.py` handles this gracefully with `try/except ImportError`. Environment variables are injected by the DockerOperator instead. If running manually in `spark-jupyter`, install it:

```bash
pip install python-dotenv
```

### "database does not exist"

Check `.env` has the correct `POSTGRES_DB` value. If using Airflow, verify the container was started with the matching `-e POSTGRES_DB=...` flag.

### DQ Silver FAIL — pipeline stops

This is by design. `dq_silver.py` exits with code 1 if any check fails, which blocks downstream Airflow tasks. Fix the data issue in `bronze_to_silver.py`, then re-run.

### JDBC connection refused

Ensure `my-postgres` is running and on `spark-net`:

```bash
docker ps | grep my-postgres
docker network inspect spark-net
```

### Airflow DAG not visible

Check for import errors:

```bash
docker exec airflow airflow dags list-import-errors
```
