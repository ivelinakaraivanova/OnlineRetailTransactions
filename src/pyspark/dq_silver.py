"""Data Quality checks on Silver layer — enforcement with PASS/FAIL."""

import os
import sys
from datetime import datetime

import pyspark.sql.functions as F

from config import BRONZE_PARQUET, DQ_SILVER_REPORT, SILVER_CLEAN, LOG_DIR
from logger import get_logger
from utils import get_spark, read_parquet

logger = get_logger("dq_silver")


def count_nulls(df, col_name):
    return df.filter(F.col(col_name).isNull()).count()


def check(report, description, value, expected=0):
    """Append a PASS/FAIL line. Returns True if passed."""
    status = "PASS" if value == expected else "FAIL"
    report.append(f"  {description}: {value}  [{status}]")
    return value == expected


def main():
    logger.info("Starting DQ checks on Silver")
    spark = get_spark("DQSilver")

    bronze = read_parquet(spark, BRONZE_PARQUET)
    silver = read_parquet(spark, SILVER_CLEAN)

    bronze_count = bronze.count()
    silver_count = silver.count()

    report = []
    all_passed = True

    report.append("=" * 60)
    report.append(f"  SILVER DQ REPORT — {datetime.now():%Y-%m-%d %H:%M:%S}")
    report.append("=" * 60)

    # VOLUME
    report.append("\n[VOLUME]")
    report.append(f"  Bronze rows:  {bronze_count}")
    report.append(f"  Silver rows:  {silver_count}")
    report.append(f"  Rows removed: {bronze_count - silver_count}")

    # COMPLETENESS — key columns must have 0 NULLs
    report.append("\n[COMPLETENESS — key columns must have 0 NULLs]")
    key_cols = ["InvoiceNo", "StockCode", "Quantity", "UnitPrice", "InvoiceDate"]
    for col in key_cols:
        n = count_nulls(silver, col)
        all_passed &= check(report, f"NULL {col}", n)

    # VALIDITY — no non-positive values, no invalid dates
    report.append("\n[VALIDITY]")
    qty_le_0 = silver.filter(F.col("Quantity") <= 0).count()
    all_passed &= check(report, "Quantity <= 0", qty_le_0)

    price_le_0 = silver.filter(F.col("UnitPrice") <= 0).count()
    all_passed &= check(report, "UnitPrice <= 0", price_le_0)

    null_dates = silver.filter(F.col("InvoiceDate").isNull()).count()
    all_passed &= check(report, "Invalid dates (NULL)", null_dates)

    # UNIQUENESS
    report.append("\n[UNIQUENESS]")
    distinct = silver.dropDuplicates(["InvoiceNo", "StockCode", "InvoiceDate"]).count()
    dupes = silver_count - distinct
    all_passed &= check(report, "Duplicates (InvoiceNo+StockCode+InvoiceDate)", dupes)

    # BUSINESS RULES — no cancellations in Silver
    report.append("\n[BUSINESS RULES]")
    cancellations = silver.filter(F.col("InvoiceNo").startswith("C")).count()
    all_passed &= check(report, "Cancellations in Silver", cancellations)

    # TYPE CHECKS
    report.append("\n[TYPE CHECKS]")
    expected_types = {
        "InvoiceDate": "timestamp",
        "Quantity": "int",
        "UnitPrice": "double",
        "Amount": "double",
        "CustomerID": "int",
    }
    schema_map = dict(silver.dtypes)
    for col_name, expected in expected_types.items():
        actual = schema_map.get(col_name, "MISSING")
        passed = actual == expected
        all_passed &= passed
        status = "PASS" if passed else "FAIL"
        report.append(f"  {col_name}: {actual} (expected {expected})  [{status}]")

    # SUMMARY
    report.append("\n" + "=" * 60)
    passes = sum(1 for l in report if "[PASS]" in l)
    fails = sum(1 for l in report if "[FAIL]" in l)
    verdict = "ALL PASSED" if all_passed else "SOME CHECKS FAILED"
    report.append(f"  TOTAL: {passes} PASS, {fails} FAIL — {verdict}")
    report.append("=" * 60)

    report_text = "\n".join(report)
    print(report_text)

    os.makedirs(LOG_DIR, exist_ok=True)
    with open(DQ_SILVER_REPORT, "w") as f:
        f.write(report_text)
    logger.info(f"Report written to {DQ_SILVER_REPORT}")

    spark.stop()

    if not all_passed:
        logger.error("DQ Silver FAILED — pipeline should stop")
        sys.exit(1)

    logger.info("DQ Silver checks completed — ALL PASSED")


if __name__ == "__main__":
    main()
