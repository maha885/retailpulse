-- stg_dim_customer.sql
-- Passes the SCD Type 2 dimension through with consistent naming. Kept as its
-- own staging model (rather than skipped) so the mart layer never queries the
-- raw table directly — a standard dbt convention that keeps a single point of
-- change if the source structure ever shifts.

with source as (
    select * from {{ source('staging_raw', 'raw_dim_customer') }}
),

renamed as (
    select
        customer_sk,
        customer_id,
        customer_name,
        customer_email,
        customer_city,
        customer_state,
        valid_from,
        valid_to,
        is_current
    from source
)

select * from renamed
