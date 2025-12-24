import json
import random
import time
import os
from datetime import datetime
from kafka import KafkaProducer


# ----------------------------
# Configuration
# ----------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "transactions")
EVENT_INTERVAL = float(os.getenv("EVENT_INTERVAL", "1.0"))  # seconds


# ----------------------------
# Sample Reference Data
# ----------------------------
USERS = [101, 102, 103, 104]
LOCATIONS = ["IN", "US", "UK", "AE"]


# ----------------------------
# Producer Factory
# ----------------------------
def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        retries=3
    )


# ----------------------------
# Event Generator
# ----------------------------
def generate_transaction(tx_id: int) -> dict:
    return {
        "transaction_id": f"tx_{tx_id}",
        "user_id": random.choice(USERS),
        "amount": round(random.uniform(500, 100000), 2),
        "location": random.choice(LOCATIONS),
        "timestamp": datetime.utcnow().isoformat()
    }


# ----------------------------
# Main Loop
# ----------------------------
def main():
    producer = create_producer()
    tx_id = 1

    print(f"Starting producer → topic '{TOPIC_NAME}' on {KAFKA_BROKER}")

    try:
        while True:
            event = generate_transaction(tx_id)
            producer.send(TOPIC_NAME, value=event)
            print(f"Sent: {event}")
            tx_id += 1
            time.sleep(EVENT_INTERVAL)

    except KeyboardInterrupt:
        print("Stopping producer...")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
