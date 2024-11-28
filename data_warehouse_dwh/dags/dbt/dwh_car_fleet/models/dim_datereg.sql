WITH dateregistration_temp AS (
    SELECT DISTINCT
        immatricolazione AS datereg_id,
        CASE
            WHEN LENGTH(immatricolazione) = 10 THEN
            -- Date format: "DD/MM/YYYY"
                 TO_DATE(immatricolazione, 'DD/MM/YYYY')
            ELSE
                NULL
        END AS date_part
    FROM {{ source('dwh_car_fleet', 'raw_car_circulating') }}
    WHERE immatricolazione IS NOT NULL
)
SELECT
  TO_DATE(datereg_id, 'DD/MM/YYYY') AS datereg_id,
  EXTRACT(YEAR FROM date_part) AS year_reg,
  EXTRACT(MONTH FROM date_part) AS month_reg,
  EXTRACT(DAY FROM date_part) AS day_reg
FROM dateregistration_temp


