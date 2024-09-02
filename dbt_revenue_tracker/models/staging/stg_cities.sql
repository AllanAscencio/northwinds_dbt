WITH cities as (
  SELECT * FROM {{ source('postgres', 'cities') }} 
)

SELECT * FROM cities