WITH source as (
  SELECT * FROM {{ source('rds', 'customers') }}
),
renamed as (
    SELECT 
    concat('rds-',replace(lower(company_name), ' ', '-')) as name,
    company_name as business_name,
    max(address) as address,
    max(city) as city,
    max(postal_code) as postal_code,
    max(country) as country
    FROM source
    group by business_name
)
SELECT * FROM renamed


