## Project Status

The real-time fraud detection pipeline has been tested successfully.

### Tested Components

* **Kafka:** Transaction producer sent a transaction to the `transactions` topic.
* **Apache Spark Structured Streaming:** Received and processed the transaction.
* **XGBoost:** Loaded the trained fraud detection model and generated a prediction.

### Sample Test Result

| Field             | Result                    |
| ----------------- | ------------------------- |
| Transaction Type  | TRANSFER                  |
| Amount            | 1,000,000                 |
| Fraud Probability | 0.000024                  |
| Prediction        | 0 — Not detected as fraud |

**Note:** This is one test transaction. The result does not establish overall model accuracy.

### Current Technology Stack

* Python
* Apache Kafka
* Apache Spark Structured Streaming
* XGBoost
* PaySim transaction dataset

