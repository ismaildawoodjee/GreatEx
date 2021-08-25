/* Query to see the overall result of validations stored in Postgres
 Logging table: When it starts and when it ends, how long it runs, and outcome?
 Postgres functionality to query JSON data, flatten some parts of the `value` column
 Stored procedures / triggers? To create a new table from original table with added features */
SELECT
  *,
  (
    CASE
      WHEN value LIKE '%"success_percent": 100%' THEN 1
      ELSE 0
    END
  ) AS success
FROM
  systems.ge_validations_store;

INSERT INTO
  logging.great_expectations (
    SELECT
      expectation_suite_name,
      run_name,
      run_time :: TIMESTAMP AT TIME ZONE 'UTC',
      batch_identifier,
      value :: JSONB
    FROM
      systems.ge_validations_store
  );

CREATE FUNCTION log_ge_validation() RETURNS TRIGGER AS $ trig_ge_validation $ BEGIN return NULL;
END;
$ trig_ge_validation $ LANGUAGE plpgsql;
CREATE TRIGGER trig_ge_validation
AFTER
INSERT
  ON systems.stable FOR EACH ROW EXECUTE PROCEDURE log_ge_validation();

SELECT
  *
FROM
  logging.great_expectations;

/* Output validations table as CSV file (just to check out what's inside the JSON column) */
COPY (
  SELECT
    jsonb_agg(subtable)
  FROM
    (
      SELECT
        value
      FROM
        logging.great_expectations
    ) AS subtable
) TO '/great_expectations/uncommitted/myfile.json'
/* SELECT
 value
 FROM
 validation.validations_store
 LIMIT
 1; */

 