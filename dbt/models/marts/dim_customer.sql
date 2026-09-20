-- dim_customer.sql
-- The gold-layer customer dimension. Just re-exposes the staging model as a
-- table (rather than a view) since dimension tables are typically materialized
-- for query performance in a real warehouse.

select * from {{ ref('stg_dim_customer') }}
