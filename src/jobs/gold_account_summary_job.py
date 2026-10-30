from delta.tables import DeltaTable
from pyspark.sql.functions import col

from src.common.config import load_config
from src.common.spark_session import create_spark_session
from src.transformations.transaction_transformations import create_account_summary
from src.common.control_manager import (
    get_processed_batches, 
    mark_batch_as_processed,
)
from src.common.logger import get_logger
logger = get_logger(__name__)

def main() -> None:
    config = load_config()
    
    silver_path = config["paths"]["silver_transactions"]
    gold_path = config["paths"]["gold_transactions"]
    gold_control_file = config["paths"]["gold_control_file"]
    
    spark = create_spark_session("GoldAccountSummary")
    
    silver_df = (
        spark.read
        .format("delta")
        .load(silver_path)
    )
    
    processed_batches = get_processed_batches(gold_control_file)
    
    all_batch_ids = [
        row["batch_id"]
        for row in silver_df
        .select("batch_id")
        .distinct()
        .collect()
    ]
    
    new_batch_ids = [
        batch_id
        for batch_id in all_batch_ids
        if batch_id not in processed_batches
    ]
    
    #print(f"New Silver batches: {new_batch_ids}")
    logger.info("New Silver batches: %s", new_batch_ids)
    
    if not new_batch_ids:
        #print("No new Silver batches to process")
        logger.info("No new Silver batches to process")
        spark.stop()
        return
    
    #Find accounts affected by the new batches
    affected_accounts_df = (
        silver_df.filter(col("batch_id").isin(new_batch_ids))
        .select("account_id")
        .distinct()
    )
    
    #Get complete silver history only for affected accounts
    affected_silver_df = silver_df.join(
        affected_accounts_df,
        on="account_id",
        how="inner",
    )
    
    #Recalculate only affected accounts
    account_summary_df = create_account_summary(
        affected_silver_df
    )
    account_summary_df.show(truncate=False)
    
    if DeltaTable.isDeltaTable(spark, gold_path):
        gold_table = DeltaTable.forPath(spark, gold_path)
        
        (
            gold_table.alias("target")
            .merge(
                account_summary_df.alias("source"),
                "target.account_id = source.account_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            account_summary_df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )
        
    #Mark batches processed only after successful Gold write
    for batch_id in new_batch_ids:
        mark_batch_as_processed(
            gold_control_file,
            batch_id,
        )
        
    spark.stop()

if __name__ == "__main__":
    main()