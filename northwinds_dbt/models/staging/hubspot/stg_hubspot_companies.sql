WITH formatted AS (
  SELECT
    'hubspot-' || business_name AS name,
    business_name
  FROM
    g_leal.northwinds_hubspot
)

SELECT
  *
FROM
  formatted