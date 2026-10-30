from src.common.config import load_config
from src.common.spark_session import create_spark_session
from src.common.logger import get_logger
logger = get_logger(__name__)

def main() -> None:
    config = load_config()
    bronze_path = config["paths"]["bronze_transactions"]
    
    spark = create_spark_session("VerifyBronze")
    
    bronze_df = (
        spark.read
        .format("parquet")
        .load(bronze_path)
    )
    
    bronze_df.orderBy("event_timestamp").show(truncate=False)
    
    #print(f"Bronze record count: {bronze_df.count()}")
    logger.info("Bronze record count: %s", bronze_df.count())
    
    spark.stop()

if __name__ == "__main__":
    main()