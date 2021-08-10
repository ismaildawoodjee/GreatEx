-- final transformation before loading into production data warehouse
INSERT INTO public.retail_profiling
(
    customer_id
    , stock_code
    , invoice_date
    , quantity
    , unit_price
    , country
)
SELECT customer_id
    , stock_code
    , invoice_date
    , quantity
    , unit_price
    , country
FROM stage.retail_profiling;