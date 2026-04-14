"""Airflow DAG: Online Retail ETL pipeline (Medallion Architecture)."""

import os
from datetime import datetime, timedelta
from airflow.sdk import DAG  # type: ignore
from airflow.providers.docker.operators.docker import DockerOperator  # type: ignore
from docker.types import Mount  # type: ignore

# ----- Configuration -----
SPARK_IMAGE = "jupyter/pyspark-notebook:latest"
NETWORK = "spark-net"
PROJECT_MOUNT = "/home/jovyan/work"
PYSPARK_DIR = f"{PROJECT_MOUNT}/src/pyspark"
PY_FILES = "config.py,logger.py,utils.py"
JDBC_JAR = f"{PROJECT_MOUNT}/jars/postgresql-42.7.1.jar"

# Host path (Windows) — Docker Desktop translates this automatically
HOST_PROJECT_PATH = "D:/Prj/PySpark/OnlineRetailTransactions"

PROJECT_MOUNT_OBJ = Mount(target=PROJECT_MOUNT, source=HOST_PROJECT_PATH, type="bind")

# Read from Airflow container env vars (passed via docker run -e flags)
_PG_KEYS = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
ENV_VARS = {k: os.environ[k] for k in _PG_KEYS}

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def spark_submit_cmd(script, use_jdbc=False):
    """Build a spark-submit command string."""
    cmd = f"cd {PYSPARK_DIR} && spark-submit --py-files {PY_FILES}"
    if use_jdbc:
        cmd += f" --jars {JDBC_JAR}"
    cmd += f" {script}"
    return ["bash", "-c", cmd]


with DAG(
    dag_id="retail_daily_pipeline",
    default_args=default_args,
    description="Online Retail ETL: Raw → Bronze → Silver → PostgreSQL → Gold",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "etl", "medallion"],
) as dag:

    raw_to_bronze = DockerOperator(
        task_id="raw_to_bronze",
        image=SPARK_IMAGE,
        command=spark_submit_cmd("raw_to_bronze.py"),
        mounts=[PROJECT_MOUNT_OBJ],
        network_mode=NETWORK,
        environment=ENV_VARS,
        auto_remove="success",
        mount_tmp_dir=False, docker_url="unix://var/run/docker.sock",
    )

    dq_bronze = DockerOperator(
        task_id="dq_bronze",
        image=SPARK_IMAGE,
        command=spark_submit_cmd("dq_bronze.py"),
        mounts=[PROJECT_MOUNT_OBJ],
        network_mode=NETWORK,
        environment=ENV_VARS,
        auto_remove="success",
        mount_tmp_dir=False, docker_url="unix://var/run/docker.sock",
    )

    bronze_to_silver = DockerOperator(
        task_id="bronze_to_silver",
        image=SPARK_IMAGE,
        command=spark_submit_cmd("bronze_to_silver.py"),
        mounts=[PROJECT_MOUNT_OBJ],
        network_mode=NETWORK,
        environment=ENV_VARS,
        auto_remove="success",
        mount_tmp_dir=False, docker_url="unix://var/run/docker.sock",
    )

    dq_silver = DockerOperator(
        task_id="dq_silver",
        image=SPARK_IMAGE,
        command=spark_submit_cmd("dq_silver.py"),
        mounts=[PROJECT_MOUNT_OBJ],
        network_mode=NETWORK,
        environment=ENV_VARS,
        auto_remove="success",
        mount_tmp_dir=False, docker_url="unix://var/run/docker.sock",
    )

    silver_to_postgresql = DockerOperator(
        task_id="silver_to_postgresql",
        image=SPARK_IMAGE,
        command=spark_submit_cmd("silver_to_postgresql.py", use_jdbc=True),
        mounts=[PROJECT_MOUNT_OBJ],
        network_mode=NETWORK,
        environment=ENV_VARS,
        auto_remove="success",
        mount_tmp_dir=False, docker_url="unix://var/run/docker.sock",
    )

    gold_kpis = DockerOperator(
        task_id="gold_kpis",
        image=SPARK_IMAGE,
        command=spark_submit_cmd("gold_kpis.py"),
        mounts=[PROJECT_MOUNT_OBJ],
        network_mode=NETWORK,
        environment=ENV_VARS,
        auto_remove="success",
        mount_tmp_dir=False, docker_url="unix://var/run/docker.sock",
    )

    # Pipeline order (per skeleton.txt)
    raw_to_bronze >> dq_bronze >> bronze_to_silver >> dq_silver >> silver_to_postgresql >> gold_kpis
