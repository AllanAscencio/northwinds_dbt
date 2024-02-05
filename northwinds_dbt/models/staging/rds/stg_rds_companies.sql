WITH source as (
  SELECT * FROM {{ source('rds', 'customers') }}
),
renamed as (
    SELECT 
    concat('rds-',replace(lower(company_name), ' ', '-')) as name,
    company_name,
    max(address) as address,
    max(city) as city,
    max(postal_code) as postal_code,
    max(country) as country
    FROM source
    group by company_name
)
SELECT * FROM renamed


