/* Output validations table as JSON file (just to check out what's inside the JSON column) */
/* This script is not a part of the Airflow DAG */
COPY (
  SELECT
    jsonb_agg(subtable)
  FROM
    (
      SELECT
        value :: JSONB
      FROM
        systems.ge_validations_store
    ) AS subtable
) TO '/great_expectations/uncommitted/json_column.json'