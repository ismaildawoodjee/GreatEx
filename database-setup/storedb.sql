CREATE SCHEMA IF NOT EXISTS logging;

DROP TABLE IF EXISTS logging.great_expectations;

CREATE TABLE logging.great_expectations (
  expectation_suite_name TEXT,
  run_name TEXT,
  run_time TIMESTAMP,
  batch_identifier TEXT,
  value JSONB
);