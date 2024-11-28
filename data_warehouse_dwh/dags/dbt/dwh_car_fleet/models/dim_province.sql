WITH province_temp AS (
    SELECT DISTINCT
        --{{ dbt_utils.generate_surrogate_key(['denominazione_provincia']) }} as province_id,
        denominazione_provincia as province_id,
        denominazione_regione as region,
        ripartizione_geografica as territory
    FROM {{ source('dwh_car_fleet', 'raw_regions') }}
    WHERE denominazione_provincia IS NOT NULL
)
SELECT
    province_id::TEXT,
    region::TEXT,
    territory::TEXT
FROM province_temp