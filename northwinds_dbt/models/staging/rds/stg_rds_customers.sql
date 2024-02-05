WITH customers AS (
  SELECT * FROM {{ source('rds', 'customers') }}
), companies AS (
    SELECT * FROM {{ ref('stg_rds_companies') }}
),
renamed AS (
    SELECT 
    CONCAT('rds-', customer_id) AS customer_id, 
    CASE
        WHEN LENGTH(REPLACE(phone, '-', '')) = 10 THEN
            '(' || SUBSTRING(REPLACE(phone, '-', '') FROM 1 FOR 3) || ') ' || 
            SUBSTRING(REPLACE(phone, '-', '') FROM 4 FOR 3) || '-' ||
            SUBSTRING(REPLACE(phone, '-', '') FROM 7 FOR 4)
        WHEN LENGTH(REPLACE(phone, '-', '')) = 11 THEN
            '(' || SUBSTRING(REPLACE(phone, '-', '') FROM 2 FOR 3) || ') ' || 
            SUBSTRING(REPLACE(phone, '-', '') FROM 5 FOR 3) || '-' ||
            SUBSTRING(REPLACE(phone, '-', '') FROM 8 FOR 4)
        ELSE REPLACE(phone, '-', ' ')
    END AS phone,
    CONCAT('rds-', REPLACE(LOWER(companies.name), ' ', '-')) AS company_id
    FROM customers 
    JOIN companies ON customers.company_name = companies.name
)
SELECT * FROM renamed
