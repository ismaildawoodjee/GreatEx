CREATE SCHEMA IF NOT EXISTS validation;
DROP TABLE IF EXISTS validation.validations_store;
CREATE TABLE validations_store (
  expectation_suite TEXT,
  run_name TEXT,
  run_time TIMESTAMP,
  batch_identifier TEXT,
  value JSONB
);