"""Bronze → Silver: apply all cleaning rules and write transactions_clean."""

import pyspark.sql.functions as F

from config import BRONZE_PARQUET, SILVER_CLEAN
from logger import get_logger
from utils import get_spark, read_parquet, write_parquet

logger = get_logger("bronze_to_silver")


def main():
    logger.info("Starting bronze_to_silver")
    spark = get_spark("BronzeToSilver")

    df = read_parquet(spark, BRONZE_PARQUET)
    total = df.count()
    logger.info(f"Read {total} rows from Bronze")

    # Drop rows with NULL in key columns
    df = df.dropna(subset=["InvoiceNo", "StockCode", "Quantity", "UnitPrice", "InvoiceDate"])
    logger.info(f"After dropping NULL keys: {df.count()} rows")

    # Remove non-positive Quantity and UnitPrice
    df = df.filter((F.col("Quantity") > 0) & (F.col("UnitPrice") > 0))
    logger.info(f"After removing Quantity/UnitPrice <= 0: {df.count()} rows")

    # Remove cancellations (InvoiceNo starts with "C")
    df = df.filter(~F.col("InvoiceNo").startswith("C"))
    logger.info(f"After removing cancellations: {df.count()} rows")

    # Clean text columns BEFORE dedup (so upper(StockCode) doesn't create new dupes)
    df = (
        df
        .withColumn("Description", F.trim(F.col("Description")))
        .withColumn("StockCode", F.upper(F.col("StockCode")))
    )

    # Remove duplicates by InvoiceNo + StockCode + InvoiceDate
    df = df.dropDuplicates(["InvoiceNo", "StockCode", "InvoiceDate"])
    logger.info(f"After removing duplicates: {df.count()} rows")

    # Cast types
    df = df.withColumn("CustomerID", F.col("CustomerID").cast("int"))

    # Add Amount = Quantity * UnitPrice
    df = df.withColumn("Amount", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))

    # Update processing timestamp for Silver
    df = df.withColumn("_processed_at", F.current_timestamp())

    logger.info(f"Final schema: {df.dtypes}")
    write_parquet(df, SILVER_CLEAN)
    logger.info(f"Wrote {df.count()} rows to Silver: {SILVER_CLEAN}")

    spark.stop()
    logger.info("bronze_to_silver completed successfully")


if __name__ == "__main__":
    main()
