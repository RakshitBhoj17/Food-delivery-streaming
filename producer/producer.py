import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer


# Load configuration from the project .env file
project_root = Path(__file__).resolve().parents[1]
load_dotenv(project_root / ".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")


# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# One test food-delivery event
event = {
    "event_id": f"EVT-{uuid.uuid4().hex[:8].upper()}",
    "order_id": "ORD1001",
    "event_timestamp": datetime.now(timezone.utc).isoformat(),
    "event_type": "ORDER_PLACED",
    "customer_id": "CUS1001",
    "restaurant_id": "RES001",
    "restaurant_name": "Spice Junction",
    "area": "Saket",
    "cuisine": "North Indian",
    "order_value": 685.00,
    "payment_method": "UPI",
    "rider_id": None,
    "estimated_delivery_minutes": 30,
    "order_status": "PLACED"
}


# Send event to Kafka
producer.send(KAFKA_TOPIC, value=event)
producer.flush()

print("Event sent successfully:")
print(json.dumps(event, indent=2))

producer.close()
