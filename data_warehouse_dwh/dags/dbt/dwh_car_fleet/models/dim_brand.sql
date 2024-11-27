WITH brand_temp AS (
    SELECT DISTINCT
        dati ->> 'Brand' AS brand,
        dati ->> 'Model' AS model
    FROM {{ source('dwh_car_fleet', 'raw_car_spec') }}
    WHERE dati->>'Brand' IS NOT NULL AND dati->>'Model' IS NOT NULL
)
SELECT
    brand::TEXT,
    model:: TEXT
FROM brand_temp

