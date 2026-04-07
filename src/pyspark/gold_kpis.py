"""Gold KPIs: compute daily_sales, customer_kpis, product_kpis from Silver."""

import pyspark.sql.functions as F

from config import SILVER_CLEAN, GOLD_DAILY_SALES, GOLD_CUSTOMER_KPIS, GOLD_PRODUCT_KPIS
from logger import get_logger
from utils import get_spark, read_parquet, write_parquet

logger = get_logger("gold_kpis")


def main():
    logger.info("Starting gold_kpis")
    spark = get_spark("GoldKPIs")

    df = read_parquet(spark, SILVER_CLEAN)
    logger.info(f"Read {df.count()} rows from Silver")

    # Add date column for grouping
    df = df.withColumn("Date", F.to_date("InvoiceDate"))

    # ---- daily_sales ----
    daily = (
        df.groupBy("Date")
        .agg(
            F.countDistinct("InvoiceNo").alias("total_orders"),
            F.sum("Quantity").alias("total_items"),
            F.round(F.sum("Amount"), 2).alias("total_revenue"),
            F.round(F.avg("Amount"), 2).alias("avg_order_value"),
        )
        .orderBy("Date")
    )
    write_parquet(daily, GOLD_DAILY_SALES)
    logger.info(f"daily_sales: {daily.count()} rows → {GOLD_DAILY_SALES}")

    # ---- customer_kpis ----
    customers = (
        df.filter(F.col("CustomerID").isNotNull())
        .groupBy("CustomerID", "Country")
        .agg(
            F.countDistinct("InvoiceNo").alias("total_orders"),
            F.sum("Quantity").alias("total_items"),
            F.round(F.sum("Amount"), 2).alias("total_spent"),
            F.round(F.avg("Amount"), 2).alias("avg_order_value"),
            F.min("Date").alias("first_purchase"),
            F.max("Date").alias("last_purchase"),
        )
        .orderBy(F.desc("total_spent"))
    )
    write_parquet(customers, GOLD_CUSTOMER_KPIS)
    logger.info(f"customer_kpis: {customers.count()} rows → {GOLD_CUSTOMER_KPIS}")

    # ---- product_kpis ----
    products = (
        df.groupBy("StockCode", "Description")
        .agg(
            F.countDistinct("InvoiceNo").alias("total_orders"),
            F.sum("Quantity").alias("total_sold"),
            F.round(F.sum("Amount"), 2).alias("total_revenue"),
            F.round(F.avg("UnitPrice"), 2).alias("avg_unit_price"),
            F.min("Date").alias("first_sold"),
            F.max("Date").alias("last_sold"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    write_parquet(products, GOLD_PRODUCT_KPIS)
    logger.info(f"product_kpis: {products.count()} rows → {GOLD_PRODUCT_KPIS}")

    spark.stop()
    logger.info("gold_kpis completed successfully")


if __name__ == "__main__":
    main()
