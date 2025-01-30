SELECT DISTINCT ON (immatricolazione)
    TO_DATE(immatricolazione, 'DD/MM/YYYY') AS datereg_id,
    EXTRACT(YEAR FROM TO_DATE(immatricolazione, 'DD/MM/YYYY')) AS year_reg,
    EXTRACT(MONTH FROM TO_DATE(immatricolazione, 'DD/MM/YYYY')) AS month_reg,
    EXTRACT(DAY FROM TO_DATE(immatricolazione, 'DD/MM/YYYY')) AS day_reg
FROM {{ source('dwh_car_fleet', 'raw_car_circulating') }}
WHERE immatricolazione IS NOT NULL AND LENGTH(immatricolazione) = 10
--16610 rows

