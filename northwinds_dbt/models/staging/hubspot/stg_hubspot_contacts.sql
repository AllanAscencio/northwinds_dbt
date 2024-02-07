WITH formatted AS (
    SELECT
        'hubspot-' || hubspot_id AS contact_id,
        first_name,
        last_name,
        CASE
            WHEN LENGTH(REPLACE(phone, '-', '')) = 10 THEN
                '(' || SUBSTRING(REPLACE(phone, '-', '') FROM 1 FOR 3) || ') ' || 
                SUBSTRING(REPLACE(phone, '-', '') FROM 4 FOR 3) || '-' ||
                SUBSTRING(REPLACE(phone, '-', '') FROM 7 FOR 4)
            WHEN LENGTH(REPLACE(phone, '-', '')) = 11 THEN
                '(' || SUBSTRING(REPLACE(phone, '-', '') FROM 2 FOR 3) || ') ' || 
                SUBSTRING(REPLACE(phone, '-', '') FROM 5 FOR 3) || '-' ||
                SUBSTRING(REPLACE(phone, '-', '') FROM 8 FOR 4)
            ELSE phone
        END AS phone,
        'hubspot-' || REPLACE(LOWER(business_name), ' ', '-') AS company_id
    FROM
        {{ source('hubspot', 'northwinds_hubspot')}} 
)

SELECT * FROM formatted
