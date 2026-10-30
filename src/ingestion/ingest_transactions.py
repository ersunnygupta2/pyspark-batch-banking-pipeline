from pyspark.sql import DataFrame, SparkSession

from src.common.schemas import TRANSACTION_SCHEMA

def read_transactions(
    spark: SparkSession,
    source_path: str,
) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header", True)
        .schema(TRANSACTION_SCHEMA)
        .load(source_path)
    )