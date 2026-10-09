from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="172.31.240.1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

transaction = {
    "step": 2,
    "type": "TRANSFER",
    "amount": 2000000,
    "oldbalanceOrg": 2000000,
    "newbalanceOrig": 0,
    "oldbalanceDest": 0,
    "newbalanceDest": 2000000
}

producer.send("transactions", value=transaction)
producer.flush()

print("Transaction sent successfully")

producer.close()
