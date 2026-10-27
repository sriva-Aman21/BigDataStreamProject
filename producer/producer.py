from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "supply_chain_events"

products = [
    "Chocolate",
    "Candy",
    "Biscuit",
    "Gum",
    "Toffee"
]

regions = [
    "North",
    "South",
    "East",
    "West"
]

print("Starting supply-chain data producer...")

while True:
    event = {
        "transaction_id": random.randint(100000, 999999),
        "timestamp": datetime.now().isoformat(),
        "product": random.choice(products),
        "region": random.choice(regions),
        "quantity": random.randint(1, 100),
        "unit_price": round(random.uniform(10, 500), 2),
        "delivery_time": random.randint(1, 15)
    }

    producer.send(TOPIC, value=event)
    producer.flush()

    print(event)

    time.sleep(1)