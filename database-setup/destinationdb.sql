-- example final table where I only want 5 columns
DROP TABLE IF EXISTS public.retail_profiling;

CREATE TABLE public.retail_profiling (
  customer_id INT,
  stock_code VARCHAR(32),
  invoice_date TIMESTAMP,
  quantity INT,
  unit_price NUMERIC(8, 3),
  country VARCHAR(32)
);