from src.common.spark_session import create_spark_session

def main() -> None:
    spark = create_spark_session("HelloSpark")
    
    data = [
        (1, "Checking", 1500.00),
        (2, "Savings", 4200.00),
        (3, "Checking", 850.00),
    ]
    
    columns = ["account_id", "account_type", "balance"]
    
    df = spark.createDataFrame(data, columns)
    
    df.show()
    
    df.printSchema()
    
    spark.stop()
    
if __name__ == "__main__":
    main()