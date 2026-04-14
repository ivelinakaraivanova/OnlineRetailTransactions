"""Central configuration: paths, PostgreSQL, environment settings."""

import os

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        ".env",
    ))
except ImportError:
    pass  # Running inside Docker — env vars injected by container

# --- Base paths (portable, derived from this file's location) ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# --- File paths ---
RAW_CSV = os.path.join(RAW_DIR, "online_retail.csv")
BRONZE_PARQUET = os.path.join(BRONZE_DIR, "online_retail.parquet")
SILVER_CLEAN = os.path.join(SILVER_DIR, "transactions_clean")
GOLD_DAILY_SALES = os.path.join(GOLD_DIR, "daily_sales")
GOLD_CUSTOMER_KPIS = os.path.join(GOLD_DIR, "customer_kpis")
GOLD_PRODUCT_KPIS = os.path.join(GOLD_DIR, "product_kpis")

# --- DQ report paths ---
DQ_BRONZE_REPORT = os.path.join(LOG_DIR, "dq_bronze_report.txt")
DQ_SILVER_REPORT = os.path.join(LOG_DIR, "dq_silver_report.txt")

# --- PostgreSQL ---
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT")
PG_DB = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

PG_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}
