-- This is the setup script for the postgres-store database.
-- The trigger on INSERT cannot be created without an existing table, so I had to
-- preemptively create the systems.ge_validations_store table even before any validations are run
CREATE SCHEMA IF NOT EXISTS systems;
CREATE SCHEMA IF NOT EXISTS logging;

DROP TABLE IF EXISTS systems.ge_validations_store;
DROP TABLE IF EXISTS logging.great_expectations;

CREATE TABLE systems.ge_validations_store (
  expectation_suite_name TEXT,
  run_name TEXT,
  run_time TEXT,
  batch_identifier TEXT,
  value TEXT
);
CREATE TABLE logging.great_expectations (
  expectation_suite_name TEXT,
  run_name TEXT,
  batch_identifier TEXT,
  run_time TIMESTAMP,
  end_time TIMESTAMP,
  duration DECIMAL,
  success BOOLEAN,
  successful_expectations INTEGER,
  evaluated_expectations INTEGER
);

-- A function has to be defined first before a trigger can be created
-- The `run_time` and `value` columns have to be casted to TIMESTAMP and JSONB
DROP FUNCTION IF EXISTS logging.log_ge_validation;
CREATE FUNCTION logging.log_ge_validation() RETURNS TRIGGER AS $trig_ge_validation$ BEGIN
INSERT INTO
  logging.great_expectations (
    expectation_suite_name,
    run_name,
    batch_identifier,
    run_time,
    end_time,
    duration,
    success,
    successful_expectations,
    evaluated_expectations
  )
VALUES
  (
    NEW.expectation_suite_name,
    NEW.run_name,
    NEW.batch_identifier,
    NEW.run_time :: TIMESTAMP AT TIME ZONE 'UTC',
    CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
    ROUND(
      EXTRACT(
        SECOND
        FROM
          (
            (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - (NEW.run_time :: TIMESTAMP AT TIME ZONE 'UTC')
          )
      ) :: NUMERIC, 2),
    (NEW.value :: JSONB ->> 'success') :: BOOLEAN,
    ROUND((NEW.value :: JSONB -> 'statistics' ->> 'success_percent') :: NUMERIC, 2),
    (NEW.value :: JSONB -> 'statistics' ->> 'evaluated_expectations') :: INTEGER,
    (NEW.value :: JSONB -> 'meta' -> 'batch_spec' ->> 'path')
  );
RETURN NULL;
END;
$trig_ge_validation$ LANGUAGE plpgsql;

-- Create trigger as below once the function has been defined
CREATE TRIGGER trig_ge_validation
AFTER
INSERT
  ON systems.ge_validations_store FOR EACH ROW EXECUTE PROCEDURE logging.log_ge_validation();