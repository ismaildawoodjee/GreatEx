-- This is the setup script for the postgres-store database.
-- The trigger cannot be defined without an existing table, so I had to 
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
  -- duration NUMERIC,
  value JSONB
);

-- A function has to be created first before a trigger can be defined
-- The `run_time` and `value` columns have to be casted to TIMESTAMP and JSONB
CREATE FUNCTION logging.log_ge_validation() 
RETURNS TRIGGER AS $trig_ge_validation$
BEGIN
    INSERT INTO logging.great_expectations VALUES 
    (
      NEW.expectation_suite_name,
      NEW.run_name,
      NEW.batch_identifier,
      NEW.run_time :: TIMESTAMP AT TIME ZONE 'UTC',
      CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
      -- (duration - NEW.run_time) NUMERIC,
      NEW.value :: JSONB
    );
    RETURN NULL;
END;
$trig_ge_validation$ LANGUAGE plpgsql;

CREATE TRIGGER trig_ge_validation
AFTER
INSERT
  ON systems.ge_validations_store FOR EACH ROW EXECUTE PROCEDURE logging.log_ge_validation();