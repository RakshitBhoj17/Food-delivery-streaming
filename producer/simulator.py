import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer


# --------------------------------------------------
# Load configuration
# --------------------------------------------------

project_root = Path(__file__).resolve().parents[1]
load_dotenv(project_root / ".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")


# --------------------------------------------------
# Kafka producer
# --------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# --------------------------------------------------
# Business reference data
# --------------------------------------------------

restaurants = [
    {
        "restaurant_id": "RES001",
        "restaurant_name": "Spice Junction",
        "area": "Saket",
        "cuisine": "North Indian",
        "prep_delay": 0
    },
    {
        "restaurant_id": "RES002",
        "restaurant_name": "Pizza Central",
        "area": "Hauz Khas",
        "cuisine": "Italian",
        "prep_delay": 2
    },
    {
        "restaurant_id": "RES003",
        "restaurant_name": "Wok Express",
        "area": "Dwarka",
        "cuisine": "Chinese",
        "prep_delay": 4
    },
    {
        "restaurant_id": "RES004",
        "restaurant_name": "Burger Hub",
        "area": "Rohini",
        "cuisine": "Fast Food",
        "prep_delay": 2
    },
    {
        "restaurant_id": "RES005",
        "restaurant_name": "South Bowl",
        "area": "Saket",
        "cuisine": "South Indian",
        "prep_delay": 1
    },
    {
        "restaurant_id": "RES006",
        "restaurant_name": "Biryani House",
        "area": "Dwarka",
        "cuisine": "Biryani",
        "prep_delay": 5
    }
]

payment_methods = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Cash on Delivery"
]

area_delivery_delay = {
    "Saket": 0,
    "Hauz Khas": 2,
    "Dwarka": 5,
    "Rohini": 3
}


# --------------------------------------------------
# Helper functions
# --------------------------------------------------

def create_event_id():
    return f"EVT-{uuid.uuid4().hex[:10].upper()}"


def create_order(order_number, simulation_run):

    restaurant = random.choice(restaurants)

    return {
        "order_id": f"ORD-{simulation_run}-{order_number:04d}",
        "customer_id": f"CUS{random.randint(1000, 9999)}",
        "restaurant_id": restaurant["restaurant_id"],
        "restaurant_name": restaurant["restaurant_name"],
        "area": restaurant["area"],
        "cuisine": restaurant["cuisine"],
        "prep_delay": restaurant["prep_delay"],
        "order_value": round(random.uniform(200, 1500), 2),
        "payment_method": random.choice(payment_methods),
        "estimated_delivery_minutes": random.choice(
            [30, 35, 40, 45]
        )
    }


def send_event(
    order,
    event_type,
    order_status,
    event_time,
    rider_id=None
):

    event = {
        "event_id": create_event_id(),
        "order_id": order["order_id"],
        "event_timestamp": event_time.isoformat(),
        "event_type": event_type,
        "customer_id": order["customer_id"],
        "restaurant_id": order["restaurant_id"],
        "restaurant_name": order["restaurant_name"],
        "area": order["area"],
        "cuisine": order["cuisine"],
        "order_value": order["order_value"],
        "payment_method": order["payment_method"],
        "rider_id": rider_id,
        "estimated_delivery_minutes":
            order["estimated_delivery_minutes"],
        "order_status": order_status
    }

    producer.send(KAFKA_TOPIC, value=event)
    producer.flush()

    print(
        f"{event_time.strftime('%H:%M')} | "
        f"{event_type:<22} | "
        f"{order['order_id']:<22} | "
        f"{order['area']:<10} | "
        f"INR {order['order_value']}"
    )


def next_delay(order, stage):

    if stage == "RESTAURANT_ACCEPTED":
        return random.randint(1, 3)

    if stage == "PREPARING":
        return random.randint(1, 2)

    if stage == "READY_FOR_PICKUP":
        return (
            random.randint(6, 10)
            + order["prep_delay"]
        )

    if stage == "RIDER_ASSIGNED":
        return (
            random.randint(2, 4)
            + area_delivery_delay[order["area"]] // 2
        )

    if stage == "PICKED_UP":
        return random.randint(2, 4)

    if stage == "OUT_FOR_DELIVERY":
        return 1

    if stage == "DELIVERED":
        return (
            random.randint(6, 12)
            + area_delivery_delay[order["area"]]
        )

    return 1


# --------------------------------------------------
# Order lifecycle
# --------------------------------------------------

stages = [
    ("RESTAURANT_ACCEPTED", "ACCEPTED"),
    ("PREPARING", "PREPARING"),
    ("READY_FOR_PICKUP", "READY_FOR_PICKUP"),
    ("RIDER_ASSIGNED", "RIDER_ASSIGNED"),
    ("PICKED_UP", "PICKED_UP"),
    ("OUT_FOR_DELIVERY", "OUT_FOR_DELIVERY"),
    ("DELIVERED", "DELIVERED")
]


# --------------------------------------------------
# Simulation configuration
# --------------------------------------------------

# 1 simulated business minute passes every 0.7 real seconds.
SIMULATED_MINUTE_SECONDS = 0.7

simulation_time = datetime.now(timezone.utc)
simulation_run = datetime.now().strftime("%m%d%H%M")

order_number = 1
tick = 0

active_orders = {}


print("\nReal-Time Food Delivery Operations Simulator")
print("--------------------------------------------")
print(f"Kafka Topic: {KAFKA_TOPIC}")
print("1 simulated minute = 0.7 real seconds")
print("Press CTRL+C to stop.\n")


try:

    while True:

        tick += 1

        # Move the BUSINESS clock forward by one minute.
        simulation_time += timedelta(minutes=1)

        # --------------------------------------------------
        # Generate new customer demand
        # --------------------------------------------------

        # Periodic rush period creates more orders.
        rush_period = 15 <= (tick % 40) <= 30

        if rush_period:
            order_count = random.choices(
                [0, 1, 2, 3],
                weights=[10, 40, 35, 15]
            )[0]
        else:
            order_count = random.choices(
                [0, 1, 2],
                weights=[30, 55, 15]
            )[0]

        for _ in range(order_count):

            order = create_order(
                order_number,
                simulation_run
            )

            send_event(
                order,
                "ORDER_PLACED",
                "PLACED",
                simulation_time
            )

            # Around 8% of orders will cancel.
            cancelled = random.random() < 0.08

            if cancelled:

                active_orders[order["order_id"]] = {
                    "order": order,
                    "cancelled": True,
                    "stage_index": None,
                    "rider_id": None,
                    "next_event_time":
                        simulation_time
                        + timedelta(
                            minutes=random.randint(2, 4)
                        )
                }

            else:

                first_stage = stages[0][0]

                active_orders[order["order_id"]] = {
                    "order": order,
                    "cancelled": False,
                    "stage_index": 0,
                    "rider_id": None,
                    "next_event_time":
                        simulation_time
                        + timedelta(
                            minutes=next_delay(
                                order,
                                first_stage
                            )
                        )
                }

            order_number += 1


        # --------------------------------------------------
        # Advance active orders through their lifecycle
        # --------------------------------------------------

        completed_orders = []

        for order_id in list(active_orders.keys()):

            state = active_orders[order_id]

            if state["next_event_time"] > simulation_time:
                continue

            order = state["order"]

            # Cancellation path
            if state["cancelled"]:

                send_event(
                    order,
                    "ORDER_CANCELLED",
                    "CANCELLED",
                    simulation_time
                )

                completed_orders.append(order_id)
                continue


            stage_index = state["stage_index"]

            event_type, status = stages[stage_index]


            # Assign rider once we reach rider assignment.
            if event_type == "RIDER_ASSIGNED":
                state["rider_id"] = (
                    f"RID{random.randint(100, 999)}"
                )


            send_event(
                order,
                event_type,
                status,
                simulation_time,
                state["rider_id"]
            )


            # Delivery finishes the lifecycle.
            if event_type == "DELIVERED":

                completed_orders.append(order_id)

            else:

                state["stage_index"] += 1

                next_stage = stages[
                    state["stage_index"]
                ][0]

                state["next_event_time"] = (
                    simulation_time
                    + timedelta(
                        minutes=next_delay(
                            order,
                            next_stage
                        )
                    )
                )


        for order_id in completed_orders:
            del active_orders[order_id]


        print(
            f"--- Simulated Time: "
            f"{simulation_time.strftime('%H:%M')} | "
            f"Active Orders: {len(active_orders)} ---"
        )


        # The demo runs fast even though business time
        # advances realistically.
        time.sleep(SIMULATED_MINUTE_SECONDS)


except KeyboardInterrupt:

    print("\nSimulator stopped by user.")


finally:

    producer.close()
    print("Kafka producer connection closed.")

