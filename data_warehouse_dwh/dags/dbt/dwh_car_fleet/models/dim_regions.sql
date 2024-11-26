-- dim_regions

WITH regions_temp AS (
    SELECT DISTINCT
        denominazione_provincia as province_id,
        denominazione_regione as region,
        ripartizione_geografica as territory
    FROM {{ source('dwh_car_fleet', 'raw_regions') }}
    WHERE denominazione_provincia IS NOT NULL
)
SELECT
    province_id,
    region,
    territory
FROM regions_temp