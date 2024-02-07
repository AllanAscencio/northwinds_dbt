WITH customers as (
  SELECT * FROM {{ source('rds', 'customers')}} 
), 
companies as (
  SELECT * FROM {{ ref('stg_rds_companies') }}
),
renamed as (
    SELECT 
    concat('rds-', customer_id) as contact_id, 
    SPLIT_PART(contact_name, ' ', 1) as first_name,
    SPLIT_PART(contact_name, ' ', -1) as last_name,
    REPLACE(TRANSLATE(phone, '(,),-,.', ''), ' ', '') as updated_phone,
    business_name
    FROM customers 
    JOIN companies ON companies.name = customers.company_name 
), final as 
 (SELECT
   contact_id,
   first_name,
   last_name,
   CASE WHEN LENGTH(updated_phone) = 10 THEN
       '(' || SUBSTRING(updated_phone, 1, 3) || ') ' || 
       SUBSTRING(updated_phone, 4, 3) || '-' ||
       SUBSTRING(updated_phone, 7, 4) 
       END as phone,
   business_name
  FROM renamed
)
SELECT * FROM final