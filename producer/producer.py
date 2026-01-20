import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "orders_raw"
BOOTSTRAP_SERVERS = "localhost:9092"

conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
}

producer = Producer(conf)

products = ["laptop", "phone", "headphones", "keyboard", "mouse"]
cities = ["New York", "Buffalo", "Chicago", "Dallas", "San Jose"]

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

def make_event():
    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "order_id": random.randint(100000, 999999),
        "customer_id": random.randint(1000, 1999),
        "product": random.choice(products),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(10, 500), 2),
        "city": random.choice(cities),
    }

if __name__ == "__main__":
    print(f"Producing to topic: {TOPIC}")

    while True:
        evt = make_event()
        evt["total_amount"] = round(evt["quantity"] * evt["unit_price"], 2)

        producer.produce(
            topic=TOPIC,
            value=json.dumps(evt).encode("utf-8"),
            on_delivery=delivery_report,
        )

        producer.poll(0)
        time.sleep(1)
