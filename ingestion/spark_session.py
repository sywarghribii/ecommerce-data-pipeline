import os
import sys
from pyspark.sql import SparkSession


def get_spark(app_name: str = "ecom-pipeline") -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
