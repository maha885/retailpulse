-- stg_orders.sql
-- Light cleanup/renaming pass over the raw orders table. Staging models should
-- do as little transformation as possible — just make column names/types
-- consistent for the mart layer to build on.

with source as (
    select * from {{ source('staging_raw', 'raw_orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        customer_name,
        customer_email,
        customer_city,
        customer_state,
        product_category,
        product_name,
        quantity,
        unit_price,
        total_amount,
        order_status,
        order_date,
        order_timestamp,
        ingested_at,
        processed_at
    from source
)

select * from renamed
