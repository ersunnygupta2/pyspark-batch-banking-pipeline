from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

#configure_spark_with_delta_pip() makes the required Delta JARs available to Spark when using Python package.
    
def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession.
    """
    
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    return spark