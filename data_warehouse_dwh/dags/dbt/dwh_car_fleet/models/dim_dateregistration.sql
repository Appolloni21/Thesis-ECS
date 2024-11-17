-- dim_dateregistration

WITH dateregistration_temp AS (
    SELECT DISTINCT
        immatricolazione AS dateregistration_id,
        CASE
            WHEN LENGTH(immatricolazione) = 10 THEN
            -- Date format: "DD/MM/YYYY"
                 TO_TIMESTAMP(immatricolazione, 'DD/MM/YYYY')
            ELSE
                NULL
        END AS date_part
    FROM {{ source('dwh_car_fleet', 'car_valle_aosta') }}
    WHERE immatricolazione IS NOT NULL
)
SELECT
  dateregistration_id,
  EXTRACT(YEAR FROM date_part) AS year,
  EXTRACT(MONTH FROM date_part) AS month,
  EXTRACT(DAY FROM date_part) AS day
FROM dateregistration_temp


