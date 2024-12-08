WITH parsed_data AS (
    SELECT
        dati ->> 'Brand' AS brand,
        dati ->> 'Model' AS model
    FROM {{ source('dwh_car_fleet', 'raw_car_spec') }}
    WHERE dati->>'Brand' IS NOT NULL AND dati->>'Model' IS NOT NULL
),
temp_brands AS (
    SELECT DISTINCT
        UPPER(brand) AS brand_id,
        model AS model
    FROM parsed_data
)
SELECT
    brand_id::TEXT,
    model::TEXT
FROM temp_brands
--3234 rows