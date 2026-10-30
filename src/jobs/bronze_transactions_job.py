import os
from pyspark.sql.functions import current_timestamp, input_file_name, lit

from src.common.config import load_config
from src.common.control_manager import (
    get_processed_files,
    mark_file_as_processed
)
from src.common.spark_session import create_spark_session
from src.ingestion.ingest_transactions import read_transactions

from src.common.logger import get_logger
logger = get_logger(__name__)

def main() -> None:
    config = load_config()
    
    source_path = config["paths"]["source_transactions"]
    bronze_path = config["paths"]["bronze_transactions"]
    control_file = config["paths"]["control_file"]
    
    spark = create_spark_session("BronzeTransactions")
    
    processed_files = get_processed_files(control_file)
    
    source_files = [
        file_name for file_name in os.listdir(source_path) if file_name.endswith(".csv")
    ]
    
    new_files = [
        file_name for file_name in source_files if file_name not in processed_files
    ]
    
    #print(f"New files found: {new_files}")
    logger.info("New files found: %s", new_files)
    
    for file_name in new_files:
        batch_id = os.path.splitext(file_name)[0]
        file_path = os.path.join(source_path, file_name)
        transactions_df = read_transactions(spark, file_path,)
        #transactions_df.show(truncate=False)
        #transactions_df.printSchema()
        
        bronze_df = (
            transactions_df
            .withColumn("batch_id", lit(batch_id))
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
        )
        #bronze_df.show(truncate=False)
    
        (
            bronze_df.write
            .format("parquet")
            .mode("append")
            .save(bronze_path)
        )
        
        mark_file_as_processed(
            control_file,
            file_name,
        )

        #print(f"Successfully processed: {file_name}")
        logger.info("Successfully processed: %s", file_name)
    
    spark.stop()
    

if __name__ == "__main__":
    main()