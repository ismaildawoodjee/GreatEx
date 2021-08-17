-- Query to see the overall result of validations stored in Postgres
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