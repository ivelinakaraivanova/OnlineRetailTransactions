"""Shared utilities: SparkSession factory, I/O helpers."""

from pyspark.sql import DataFrame, SparkSession


def get_spark(app_name: str) -> SparkSession:
    """Create or get a SparkSession with standard config."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read a CSV file with header and inferred schema."""
    return spark.read.csv(path, header=True, inferSchema=True)


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """Read a Parquet file/directory."""
    return spark.read.parquet(path)


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Write a DataFrame as Parquet."""
    df.write.mode(mode).parquet(path)
