from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    DoubleType,
    TimestampType
)

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("event_timestamp", TimestampType(), False),
])