from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, sum, count

# Checking valid records
def get_valid_transactions(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("transaction_id").isNotNull())
        & (col("account_id").isNotNull())
        & (col("amount") > 0)
        & (col("transaction_type").isin("DEPOSIT", "WITHDRAWAL"))
        & (col("event_timestamp").isNotNull())
    )

#Checking invalid records
def get_invalid_transactions(df: DataFrame) -> DataFrame:
    return df.filter(
        (col("transaction_id").isNull())
        | (col("account_id").isNull())
        | (col("amount") <= 0)
        | (~col("transaction_type").isin("DEPOSIT", "WITHDRAWAL"))
        | (col("event_timestamp").isNull())
    )

# Drop Duplicate
def deduplicate_transactions(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["transaction_id"])

# First Business Transformation
def add_transaction_amount(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "signed_amount",
        when(
            col("transaction_type") == "DEPOSIT",
            col("amount")
        ).otherwise(-col("amount"))
    )

# GOLD Layer code
def create_account_summary(df: DataFrame) -> DataFrame:
    return(
        df.groupBy("account_id")
        .agg(
            sum(
                when(
                    col("transaction_type") == "DEPOSIT",
                    col("amount")
                ).otherwise(0)
            ).alias("total_deposits"),
            sum(
                when(
                    col("transaction_type") == "WITHDRAWAL",
                    col("amount")
                ).otherwise(0)
            ).alias("total_withdrawals"),
            
            sum("signed_amount").alias("net_amount"),
            
            count("*").alias("transaction_count"),
        )
    )