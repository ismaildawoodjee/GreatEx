-- Query to see the overall result of validations stored in Postgres
-- Logging table: When it starts and when it ends, how long it runs, and outcome?
-- Postgres functionality to query JSON data, flatten some parts of the `value` column
-- Stored procedures / triggers? To create a new table from original table with added features
SELECT
  *,
  (
    CASE
      WHEN value LIKE '%"success_percent": 100%' THEN 1
      ELSE 0
    END
  ) AS success
FROM
  validation.ge_validations_store;