import os

from delta.tables import DeltaTable
from pyspark.sql.functions import col

from src.common.config import load_config
from src.common.control_manager import (
    get_processed_batches,
    mark_batch_as_processed,
)
from src.common.spark_session import create_spark_session

from src.transformations.transaction_transformations import (
    get_invalid_transactions, 
    get_valid_transactions,
    deduplicate_transactions,
    add_transaction_amount,
)

from src.common.logger import get_logger
logger = get_logger(__name__)


def main() -> None:
    config = load_config()
    
    bronze_path = config["paths"]["bronze_transactions"]
    silver_control_file = config["paths"]["silver_control_file"]
    quarantine_path = config["paths"]["quarantine_transactions"]
    silver_path = config["paths"]["silver_transactions"]
    
    spark = create_spark_session("SilverTransactions")
    
    bronze_df = spark.read.format("parquet").load(bronze_path)
    
    processed_batches = get_processed_batches(silver_control_file)
    
    all_batch_ids = [
        row["batch_id"]
        for row in bronze_df
        .select("batch_id")
        .distinct()
        .collect()
    ]
    
    new_batch_ids = [
        batch_id
        for batch_id in all_batch_ids
        if batch_id not in processed_batches
    ]
    
    #print(f"New Bronze Batches: {new_batch_ids}")
    logger.info("New Bronze Batches: %s", new_batch_ids)
    
    if not new_batch_ids:
        #print(f"No new batches to process.")
        logger.info("No new batches to process.")
        spark.stop()
        return
    
    incremental_bronze_df = bronze_df.filter(
        col("batch_id").isin(new_batch_ids)
    )
    #incremental_bronze_df.show(truncate=False)
    
    valid_df = get_valid_transactions(incremental_bronze_df)
    invalid_df = get_invalid_transactions(incremental_bronze_df)
    
    #print(f"Valid records: {valid_df.count()}")
    #print(f"Invalid records: {invalid_df.count()}")
    logger.info("Valid records: %s", valid_df.count())
    logger.info("Valid records: %s", invalid_df.count())
    
    (
        invalid_df.write
        .format("parquet")
        .mode("append")
        .save(quarantine_path)
    )
    
    deduplicated_df = deduplicate_transactions(valid_df)
    #print(f"Records after deduplication: {deduplicated_df.count()}")
    logger.info("Records after deduplication: %s", deduplicated_df.count())
    
    silver_df = add_transaction_amount(deduplicated_df)
    silver_df.show(truncate=False)
    
    if DeltaTable.isDeltaTable(spark, silver_path):
        silver_table = DeltaTable.forPath(
            spark,
            silver_path,
        )
        
        (
            silver_table.alias("target")
            .merge(
                silver_df.alias("source"),
                "target.transaction_id = source.transaction_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .save(silver_path)
        )
    
    
    for batch_id in new_batch_ids:
        mark_batch_as_processed(
            silver_control_file,
            batch_id,
        )
    
    spark.stop()

if __name__ == "__main__":
    main()