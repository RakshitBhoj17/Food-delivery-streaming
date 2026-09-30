import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer


# --------------------------------------------------
# Load project configuration
# --------------------------------------------------

project_root = Path(__file__).resolve().parents[1]
load_dotenv(project_root / ".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


# --------------------------------------------------
# Connect to Kafka
# --------------------------------------------------

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="food-delivery-db-consumer",
    value_deserializer=lambda message: json.loads(
        message.decode("utf-8")
    )
)


# --------------------------------------------------
# Connect to PostgreSQL
# --------------------------------------------------

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

cursor = conn.cursor()


insert_query = """
    INSERT INTO order_events (
        event_id,
        order_id,
        event_timestamp,
        event_type,
        customer_id,
        restaurant_id,
        restaurant_name,
        area,
        cuisine,
        order_value,
        payment_method,
        rider_id,
        estimated_delivery_minutes,
        order_status
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (event_id) DO NOTHING;
"""


print("Food Delivery Consumer started.")
print(f"Listening to Kafka topic: {KAFKA_TOPIC}")
print("Press CTRL+C to stop.\n")


try:

    for message in consumer:

        event = message.value

        print(
            f"Received: {event['event_type']} "
            f"| Order: {event['order_id']} "
            f"| Area: {event.get('area')}"
        )

        cursor.execute(
            insert_query,
            (
                event["event_id"],
                event["order_id"],
                event["event_timestamp"],
                event["event_type"],
                event.get("customer_id"),
                event.get("restaurant_id"),
                event.get("restaurant_name"),
                event.get("area"),
                event.get("cuisine"),
                event.get("order_value"),
                event.get("payment_method"),
                event.get("rider_id"),
                event.get("estimated_delivery_minutes"),
                event.get("order_status")
            )
        )

        conn.commit()

        print("Stored in PostgreSQL.\n")


except KeyboardInterrupt:

    print("\nConsumer stopped by user.")


finally:

    cursor.close()
    conn.close()
    consumer.close()

    print("Kafka and PostgreSQL connections closed.")
