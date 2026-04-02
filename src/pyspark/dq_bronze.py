"""Data Quality checks on Bronze layer — observational report."""

import os
from datetime import datetime

import pyspark.sql.functions as F

from config import BRONZE_PARQUET, LOG_DIR, DQ_BRONZE_REPORT
from logger import get_logger
from utils import get_spark, read_parquet

logger = get_logger("dq_bronze")


def count_nulls(df, col_name):
    return df.filter(F.col(col_name).isNull()).count()


def main():
    logger.info("Starting DQ checks on Bronze")
    spark = get_spark("DQBronze")

    df = read_parquet(spark, BRONZE_PARQUET)
    total = df.count()

    report = []
    report.append("=" * 60)
    report.append(f"  BRONZE DQ REPORT — {datetime.now():%Y-%m-%d %H:%M:%S}")
    report.append("=" * 60)

    # VOLUME
    report.append("\n[VOLUME]")
    report.append(f"  Total rows: {total}")

    # COMPLETENESS
    report.append("\n[COMPLETENESS — NULL counts]")
    all_cols = ["InvoiceNo", "StockCode", "Description", "Quantity",
                "UnitPrice", "InvoiceDate", "CustomerID", "Country"]
    for col in all_cols:
        n = count_nulls(df, col)
        pct = round(n / total * 100, 2) if total > 0 else 0
        report.append(f"  {col}: {n} ({pct}%)")

    # VALIDITY
    report.append("\n[VALIDITY]")
    qty_le_0 = df.filter(F.col("Quantity") <= 0).count()
    price_le_0 = df.filter(F.col("UnitPrice") <= 0).count()
    report.append(f"  Quantity <= 0:  {qty_le_0}")
    report.append(f"  UnitPrice <= 0: {price_le_0}")

    null_dates = df.filter(F.col("InvoiceDate").isNull()).count()
    report.append(f"  Invalid dates (NULL): {null_dates}")

    # UNIQUENESS
    report.append("\n[UNIQUENESS]")
    distinct = df.dropDuplicates(["InvoiceNo", "StockCode", "InvoiceDate"]).count()
    dupes = total - distinct
    report.append(f"  Duplicates (InvoiceNo+StockCode+InvoiceDate): {dupes}")

    # BUSINESS RULES
    report.append("\n[BUSINESS RULES]")
    cancellations = df.filter(F.col("InvoiceNo").startswith("C")).count()
    report.append(f"  Cancellations (InvoiceNo starts with 'C'): {cancellations}")

    # TYPE CHECKS
    report.append("\n[TYPE CHECKS]")
    for col_name, col_type in df.dtypes:
        report.append(f"  {col_name}: {col_type}")

    report.append("\n" + "=" * 60)

    report_text = "\n".join(report)
    print(report_text)

    os.makedirs(LOG_DIR, exist_ok=True)
    with open(DQ_BRONZE_REPORT, "w") as f:
        f.write(report_text)
    logger.info(f"Report written to {DQ_BRONZE_REPORT}")

    spark.stop()
    logger.info("DQ Bronze checks completed")


if __name__ == "__main__":
    main()
