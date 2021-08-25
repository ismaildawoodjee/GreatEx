-- This is the setup script for the postgres-source database.
CREATE SCHEMA IF NOT EXISTS ecommerce;

CREATE TABLE ecommerce.retail_profiling (
  invoice_number VARCHAR(16),
  stock_code VARCHAR(32),
  detail VARCHAR(1024),
  quantity INT,
  invoice_date TIMESTAMP,
  unit_price NUMERIC(8, 3),
  customer_id INT,
  country VARCHAR(32)
);

COPY ecommerce.retail_profiling (
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
  '/source-data/retail_profiling.csv' WITH DELIMITER ',' CSV HEADER;