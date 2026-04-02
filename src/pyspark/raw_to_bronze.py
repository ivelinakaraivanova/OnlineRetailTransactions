"""Raw to Bronze: CSV → Parquet ingestion."""

import pyspark.sql.functions as F

from config import RAW_CSV, BRONZE_PARQUET
from logger import get_logger
from utils import get_spark, read_csv, write_parquet

logger = get_logger("raw_to_bronze")


def main():
    logger.info("Starting raw_to_bronze")

    spark = get_spark("RawToBronze")

    logger.info(f"Reading CSV from {RAW_CSV}")
    df = read_csv(spark, RAW_CSV)

    row_count = df.count()
    logger.info(f"Read {row_count} rows, {len(df.columns)} columns")
    logger.info(f"Schema: {df.dtypes}")

    df = df.withColumn("_processed_at", F.current_timestamp())

    logger.info(f"Writing Parquet to {BRONZE_PARQUET}")
    write_parquet(df, BRONZE_PARQUET)

    logger.info("raw_to_bronze completed successfully")
    spark.stop()


if __name__ == "__main__":
    main()
