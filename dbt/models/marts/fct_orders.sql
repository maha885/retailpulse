-- fct_orders.sql
-- The core fact table: one row per order, joined to the customer dimension
-- USING A POINT-IN-TIME JOIN rather than just joining to the "current" row.
--
-- Why this matters: if a customer moved from one city to another last month,
-- naively joining to "the current customer row" would make an order from
-- BEFORE the move look like it happened in the NEW city. A point-in-time join
-- instead matches each order to whichever version of the customer was active
-- at the moment the order was placed — this is the entire reason SCD Type 2
-- exists, and it's the detail most portfolio projects skip.

with orders as (
    select * from {{ ref('stg_orders') }}
),

customer_history as (
    select * from {{ ref('stg_dim_customer') }}
)

select
    o.order_id,
    o.customer_id,
    c.customer_sk,               -- the specific dimension version active at order time
    c.customer_name,
    c.customer_city   as customer_city_at_order_time,
    c.customer_state  as customer_state_at_order_time,
    o.product_category,
    o.product_name,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.order_status,
    o.order_date,
    o.order_timestamp
from orders o
left join customer_history c
    on o.customer_id = c.customer_id
    and o.order_timestamp >= c.valid_from
    and (c.valid_to is null or o.order_timestamp < c.valid_to)
