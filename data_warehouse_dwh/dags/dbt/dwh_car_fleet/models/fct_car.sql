WITH fct_car_temp AS(
    SELECT
        TO_DATE(immatricolazione, 'DD/MM/YYYY') as datereg_id,
        CASE 
            WHEN make='FIAT - INNOCENTI' THEN 'FIAT'
            WHEN make='LANCIA - AUTOBIANCHI' THEN 'LANCIA'
            ELSE make
        END AS brand_id,
        provincia as province_id,
        engine_power as engine_power,
        displacement as displacement,
        fuel as fuel_type,
        emissioni as emissions,
        peso as weight_mass
    FROM {{ source('dwh_car_fleet', 'raw_car_circulating') }}
    WHERE dest='AUTOVETTURA PER TRASPORTO DI PERSONE' AND make IS NOT NULL AND immatricolazione IS NOT NULL
)
SELECT
--    dtr.datereg_id,
--   dbr.brand_id,
--    dpv.province_id,
    ROW_NUMBER() OVER () AS car_id,
    datereg_id,
    province_id::TEXT,
    brand_id:TEXT,
    engine_power,
    displacement,
    fuel_type::TEXT,
    emissions,
    weight_mass
FROM fct_car_temp fc
--INNER JOIN {{ ref('dim_datereg') }} dtr ON fc.datereg_id = dtr.datereg_id
--INNER JOIN {{ ref('dim_brand')}} dbr ON fc.brand_id = dbr.brand_id
--INNER JOIN {{ ref('dim_province')}} dpv ON fc.province_id = dpv.province_id