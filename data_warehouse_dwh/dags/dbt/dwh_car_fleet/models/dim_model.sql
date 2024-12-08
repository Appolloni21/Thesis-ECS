WITH parsed_data AS (
    SELECT
        UPPER(dati ->> 'Brand') AS brand,
        dati ->> 'Model' AS model
    FROM {{ source('dwh_car_fleet', 'raw_car_spec') }}
    WHERE dati->>'Brand' IS NOT NULL AND dati->>'Model' IS NOT NULL
)
SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['brand', 'model']) }} as model_id,
    brand AS brand,
    model AS model
FROM parsed_data
--3234 rows