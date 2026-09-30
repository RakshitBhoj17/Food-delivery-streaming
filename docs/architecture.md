# System Architecture

## Project

Real-Time Food Delivery Operations Analytics using Apache Kafka, PostgreSQL, Python and Grafana.

## End-to-End Data Flow

Food Delivery Simulator
        |
        v
Python Kafka Producer
        |
        v
Apache Kafka
Topic: food-delivery-events
        |
        v
Python Kafka Consumer
        |
        v
PostgreSQL
order_events
        |
        v
order_summary View
        |
        v
Grafana Dashboard

## 1. Data Producer

The Python simulator generates synthetic food-delivery events representing the lifecycle of customer orders.

Event stages include:

- ORDER_PLACED
- RESTAURANT_ACCEPTED
- PREPARING
- READY_FOR_PICKUP
- RIDER_ASSIGNED
- PICKED_UP
- OUT_FOR_DELIVERY
- DELIVERED
- ORDER_CANCELLED

Multiple orders progress concurrently to simulate a real food-delivery operations environment.

## 2. Apache Kafka

Kafka acts as the streaming backbone.

Topic:

food-delivery-events

Each order can generate multiple lifecycle events.

Kafka decouples event generation from downstream data processing.

## 3. Kafka Consumer

The Python consumer continuously listens to the Kafka topic.

For each event it:

1. Deserializes the JSON message.
2. Extracts operational fields.
3. Inserts the event into PostgreSQL.
4. Prevents duplicate event insertion using event_id.

## 4. PostgreSQL

### order_events

Stores the raw streaming event history.

Each row represents one event rather than one complete order.

### order_summary

An analytical SQL view that converts the event stream into one row per order.

It derives:

- Current order status
- Order placement time
- Delivery completion time
- Promised delivery time
- Actual delivery time
- SLA breach flag
- Restaurant
- Area
- Order value

## 5. Grafana

Grafana connects directly to PostgreSQL and automatically refreshes the dashboard.

Core dashboard panels:

1. Active Orders
2. Average Delivery Time
3. SLA Breach Rate
4. Orders Over Time
5. Active Orders by Status
6. Average Delivery Time by Area

## Event Time vs Processing Time

The simulator separates business event time from processing time.

The demonstration runs faster than real life, while the event timestamps represent realistic delivery durations.

This allows Average Delivery Time and SLA Breach Rate to remain analytically meaningful while still allowing complete order lifecycles to be demonstrated quickly.

## Live Streaming Pipeline

Simulator
   |
   v
Kafka Producer
   |
   v
Apache Kafka
   |
   v
Kafka Consumer
   |
   v
PostgreSQL
   |
   v
Grafana Auto Refresh

Changes in generated orders are reflected on the dashboard in near real time.
