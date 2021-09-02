-- Script to produce the final transformation before loading into the public/production data warehouse
-- The INSERT operation must be idempotent, which means:
-- the same INSERT operation must produce the same outcome, when running the same pipeline repeatedly
-- To achieve this, the table must be dropped and recreated before inserting anything into it
DROP TABLE IF EXISTS public.retail_profiling;

CREATE TABLE public.retail_profiling (
  customer_id INT,
  stock_code VARCHAR(32),
  invoice_date TIMESTAMP,
  quantity INT,
  unit_price NUMERIC(8, 3),
  country VARCHAR(32)
);

INSERT INTO
  public.retail_profiling (
    customer_id,
    stock_code,
    invoice_date,
    quantity,
    unit_price,
    country
  )
SELECT
  customer_id,
  stock_code,
  invoice_date,
  quantity,
  unit_price,
  country
FROM
  stage.retail_profiling;