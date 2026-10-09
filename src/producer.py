from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="172.31.240.1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

transaction = {
    "step": 1,
    "type": "TRANSFER",
    "amount": 1000000,
    "oldbalanceOrg": 10000,
    "newbalanceOrig": 5000,
    "oldbalanceDest": 2000,
    "newbalanceDest": 7000
}

producer.send("transactions", value=transaction)
producer.flush()

print("Transaction sent successfully")

producer.close()