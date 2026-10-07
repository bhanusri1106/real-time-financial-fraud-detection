import os

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FraudDetectionStreaming") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    ) \
    .getOrCreate()

print("Spark Kafka connector configured successfully")

spark.stop()