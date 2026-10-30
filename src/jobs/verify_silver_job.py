from src.common.config import load_config
from src.common.spark_session import create_spark_session
from src.common.logger import get_logger
logger = get_logger(__name__)

def main() -> None:
    config = load_config()
    silver_path = config["paths"]["silver_transactions"]
    
    spark = create_spark_session("VerifySilver")
    
    silver_df = (
        spark.read.format("parquet")
        .load(silver_path)
    )
    
    silver_df.orderBy("event_timestamp").show(truncate=False)
    
    #print(f"Silver record count: {silver_df.count()}")
    logger.info("Silver record count: %s", silver_df.count())
    
    spark.stop()

if __name__ == "__main__":
    main()