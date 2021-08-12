-- Unfortunately Postgres cannot copy Parquet data, so
CREATE SCHEMA IF NOT EXISTS stage;
DROP TABLE IF EXISTS stage.retail_profiling;
CREATE TABLE stage.retail_profiling (
  invoice_number VARCHAR(16),
  stock_code VARCHAR(32),
  detail VARCHAR(1024),
  quantity INT,
  invoice_date TIMESTAMP,
  unit_price NUMERIC(8, 3),
  customer_id INT,
  country VARCHAR(32)
);
COPY stage.retail_profiling (
  invoice_number,
  stock_code,
  detail,
  quantity,
  invoice_date,
  unit_price,
  customer_id,
  country
)
FROM
  '{{ params.from_stage }}' WITH DELIMITER ',' CSV HEADER;