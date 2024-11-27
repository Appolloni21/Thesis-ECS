WITH parsed_data AS (
    SELECT
        dati ->> 'Brand' AS brand,
        dati ->> 'Model' AS model
    FROM {{ source('dwh_car_fleet', 'raw_car_spec') }}
    WHERE dati->>'Brand' IS NOT NULL AND dati->>'Model' IS NOT NULL
),
temp_brands AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['brand']) }} AS brand_id,
        brand AS brand,
        model AS model
    FROM parsed_data
)
SELECT
    brand_id::TEXT,
    brand::TEXT,
    model::TEXT
FROM temp_brands