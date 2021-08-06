-- To extract retail data from PostgresDB and load it to /raw folder
COPY (
    SELECT invoice_number
        , stock_code
        , detail
        , quantity
        , invoice_date
        , unit_price
        , customer_id
        , country
    FROM ecommerce.retail_profiling
)
TO '{{ params.to_raw }}'
WITH (FORMAT CSV, HEADER)