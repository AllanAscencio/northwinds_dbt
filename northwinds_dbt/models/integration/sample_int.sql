{% set sources = ["g_leal.stg_hubspot_companies", "g_leal.stg_rds_companies"] %}
WITH merged_companies AS (
    {% for source in sources %}
    SELECT name FROM {{ source }}{% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)

SELECT name FROM merged_companies GROUP BY name