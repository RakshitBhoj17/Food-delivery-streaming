-- ============================================================
-- Food Delivery Streaming Analytics
-- PostgreSQL Database Schema
-- ============================================================


-- ------------------------------------------------------------
-- 1. Raw streaming event table
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS order_events (
    event_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    restaurant_id VARCHAR(50),
    restaurant_name VARCHAR(150),
    area VARCHAR(100),
    cuisine VARCHAR(100),
    order_value NUMERIC(10,2),
    payment_method VARCHAR(50),
    rider_id VARCHAR(50),
    estimated_delivery_minutes INTEGER,
    order_status VARCHAR(50)
);


-- ------------------------------------------------------------
-- 2. Order-level analytical view
-- ------------------------------------------------------------

CREATE OR REPLACE VIEW order_summary AS

WITH agg AS (
    SELECT
        order_id,
        MAX(customer_id) AS customer_id,
        MAX(restaurant_id) AS restaurant_id,
        MAX(restaurant_name) AS restaurant_name,
        MAX(area) AS area,
        MAX(cuisine) AS cuisine,
        MAX(order_value) AS order_value,
        MAX(payment_method) AS payment_method,
        MAX(estimated_delivery_minutes) AS promised_minutes,

        MIN(event_timestamp)
            FILTER (WHERE event_type = 'ORDER_PLACED')
            AS placed_time,

        MAX(event_timestamp)
            FILTER (WHERE event_type = 'DELIVERED')
            AS delivered_time

    FROM order_events
    GROUP BY order_id
),

latest AS (
    SELECT DISTINCT ON (order_id)
        order_id,
        event_type AS latest_event_type,
        order_status AS latest_status,
        event_timestamp AS latest_event_time

    FROM order_events
    ORDER BY order_id, event_timestamp DESC
)

SELECT
    agg.*,
    latest.latest_event_type,
    latest.latest_status,
    latest.latest_event_time,

    CASE
        WHEN agg.delivered_time IS NOT NULL
        THEN ROUND(
            (
                EXTRACT(
                    EPOCH FROM
                    (agg.delivered_time - agg.placed_time)
                ) / 60
            )::numeric,
            2
        )
    END AS actual_delivery_minutes,

    CASE
        WHEN agg.delivered_time IS NOT NULL
        THEN (
            EXTRACT(
                EPOCH FROM
                (agg.delivered_time - agg.placed_time)
            ) / 60
            > agg.promised_minutes
        )
        ELSE FALSE
    END AS sla_breach

FROM agg
JOIN latest USING (order_id);
