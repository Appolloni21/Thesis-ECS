SELECT DISTINCT ON (alimentazione)
        alimentazione AS fuel_id
FROM {{ source('dwh_car_fleet', 'raw_car_circulating') }}
WHERE alimentazione IS NOT NULL
--13 rows