from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

import pandas as pd
import joblib


# Create Spark session
spark = SparkSession.builder \
    .appName("RealTimeFraudDetection") \
    .master("local[*]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Load trained model and threshold
model_path = (
    "/mnt/c/Users/bhanu/OneDrive/Documents/Desktop/"
    "real-time-financial-fraud-detection/models/xgboost_fraud_model.pkl"
)

threshold_path = (
    "/mnt/c/Users/bhanu/OneDrive/Documents/Desktop/"
    "real-time-financial-fraud-detection/models/xgboost_best_threshold.pkl"
)

model = joblib.load(model_path)
threshold = joblib.load(threshold_path)

print("XGBoost model loaded successfully")
print("Fraud threshold:", threshold)


# Define incoming transaction schema
schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFlaggedFraud", IntegerType(), True)
])


# Read transactions from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.31.240.1:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()


# Convert Kafka messages from JSON
transactions = kafka_df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")


# Process each streaming batch
def process_batch(batch_df, batch_id):

    record_count = batch_df.count()
    print(f"\nBatch {batch_id} received: {record_count} records")

    if record_count == 0:
        return

    pdf = batch_df.toPandas()

    # Handle missing values
    pdf["isFlaggedFraud"] = pdf["isFlaggedFraud"].fillna(0)

    numeric_columns = [
        "step",
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
        "isFlaggedFraud"
    ]

    pdf[numeric_columns] = pdf[numeric_columns].fillna(0)

    # One-hot encode transaction type
    type_columns = [
        "type_CASH_IN",
        "type_CASH_OUT",
        "type_DEBIT",
        "type_PAYMENT",
        "type_TRANSFER"
    ]

    for column in type_columns:
        pdf[column] = 0

    for index, row in pdf.iterrows():
        transaction_type = "type_" + str(row["type"])

        if transaction_type in type_columns:
            pdf.loc[index, transaction_type] = 1

    # Use the same feature order as model training
    feature_columns = [
        "step",
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
        "isFlaggedFraud",
        "type_CASH_IN",
        "type_CASH_OUT",
        "type_DEBIT",
        "type_PAYMENT",
        "type_TRANSFER"
    ]

    X = pdf[feature_columns]

    # Predict fraud probability
    fraud_probability = model.predict_proba(X)[:, 1]

    pdf["fraud_probability"] = fraud_probability

    # Apply trained threshold
    pdf["prediction"] = (
        pdf["fraud_probability"] >= threshold
    ).astype(int)

    # Display results for every transaction
    print("\nTransaction prediction results:")

    print(
        pdf[
            ["type", "amount", "fraud_probability", "prediction"]
        ].to_string(index=False)
    )

    # Select transactions predicted as fraud
    fraud_transactions = pdf[pdf["prediction"] == 1]

    if len(fraud_transactions) > 0:

        print("\n========== FRAUD ALERT ==========")

        for _, transaction in fraud_transactions.iterrows():

            print(
                "FRAUD DETECTED | "
                f"Type: {transaction['type']} | "
                f"Amount: {transaction['amount']} | "
                f"Probability: {transaction['fraud_probability']:.4f}"
            )

        print("=================================\n")

    else:

        print(
            f"Batch {batch_id}: "
            f"{len(pdf)} transaction(s) processed - "
            "No fraud detected"
        )


# Start the streaming query
query = transactions.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option(
        "checkpointLocation",
        "/tmp/fraud_detection_checkpoint"
    ) \
    .trigger(processingTime="5 seconds") \
    .start()

print("\nReal-time fraud detection streaming started...")
print("Listening to Kafka topic: transactions")

query.awaitTermination()
