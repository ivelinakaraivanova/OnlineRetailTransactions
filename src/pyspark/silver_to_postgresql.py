"""Silver → PostgreSQL: load transactions_clean into PostgreSQL."""

import pyspark.sql.functions as F

from config import SILVER_CLEAN, PG_JDBC_URL, PG_PROPERTIES
from logger import get_logger
from utils import get_spark, read_parquet

logger = get_logger("silver_to_postgresql")


def main():
    logger.info("Starting silver_to_postgresql")
    spark = get_spark("SilverToPostgreSQL")

    df = read_parquet(spark, SILVER_CLEAN)
    logger.info(f"Read {df.count()} rows from Silver")

    # Select business columns only (drop _processed_at metadata)
    df_load = df.select(
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "Amount",
        "CustomerID",
        "Country",
    )

    # Write to PostgreSQL table transactions_clean
    logger.info(f"Writing to PostgreSQL: {PG_JDBC_URL} → table transactions_clean")
    df_load.write.jdbc(
        url=PG_JDBC_URL,
        table="transactions_clean",
        mode="overwrite",
        properties=PG_PROPERTIES,
    )

    logger.info(f"Loaded {df_load.count()} rows into PostgreSQL transactions_clean")
    spark.stop()
    logger.info("silver_to_postgresql completed successfully")


if __name__ == "__main__":
    main()
    