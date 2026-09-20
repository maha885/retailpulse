-- daily_sales_summary.sql
-- A simple pre-aggregated mart, the kind of thing a BI tool (Metabase) would
-- query directly rather than hitting the fact table with an aggregation
-- every time someone opens a dashboard.

select
    order_date,
    product_category,
    count(distinct order_id)                         as total_orders,
    count(distinct customer_id)                       as unique_customers,
    sum(total_amount)                                  as total_revenue,
    round(avg(total_amount)::numeric, 2)               as avg_order_value,
    sum(case when order_status = 'cancelled' then 1 else 0 end) as cancelled_orders,
    sum(case when order_status = 'returned' then 1 else 0 end)  as returned_orders
from {{ ref('fct_orders') }}
group by order_date, product_category
