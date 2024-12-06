WITH fct_car_temp AS(
    SELECT
        ROW_NUMBER() OVER () AS car_id,
        TO_DATE(immatricolazione, 'DD/MM/YYYY') as datereg_id,
        CASE 
            WHEN make='FIAT - INNOCENTI' THEN 'FIAT'
            WHEN make='LANCIA - AUTOBIANCHI' THEN 'LANCIA'
            WHEN make='MORGAN MOTOR' THEN 'MORGAN'
            WHEN make='ROLLS ROYCE' THEN 'ROLLS-ROYCE'
            WHEN make='ROVER CARS' THEN 'ROVER'
            WHEN make='SHUANGHUAN AUTO' THEN 'SHUANGHAUN'
            WHEN make='TESLA MOTORS' THEN 'TESLA'
            ELSE make
        END AS brand_id,
        provincia as province_id,
        engine_power as engine_power,
        CASE
            WHEN fuel='ELETTR' THEN 0
            ELSE displacement
        END AS engine_displacement,
        fuel as fuel_type,
        CASE
            WHEN fuel='ELETTR' THEN 0
            ELSE emissioni
        END AS co2_emissions,
        peso as kerb_weight
    FROM {{ source('dwh_car_fleet', 'raw_car_circulating') }}
    WHERE dest='AUTOVETTURA PER TRASPORTO DI PERSONE' AND make IS NOT NULL AND provincia IS NOT NULL 
        AND immatricolazione IS NOT NULL 
        AND engine_power IS NOT NULL 
        AND displacement IS NOT NULL /*4106846*/
        AND fuel IS NOT NULL
		--AND emissioni IS NOT NULL /*3402244*/
		--AND peso IS NOT NULL
),
stg_car_spec AS (
    SELECT
    id as vid,
    UPPER(dati ->> 'Brand') AS brand,
    dati ->> 'Model' AS model,
    CASE
        WHEN (dati ->> 'Fuel Type') = 'Diesel' THEN 'GASOL'
        WHEN (dati ->> 'Fuel Type') = 'Petrol (Gasoline)' THEN 'BENZ'
        WHEN (dati ->> 'Fuel Type') = 'Electricity' THEN 'ELETTR'
        WHEN (dati ->> 'Fuel Type') = 'Petrol / electricity' THEN 'IBRIDO BENZINA/ELETTRICO'
        WHEN (dati ->> 'Fuel Type') = 'Petrol / CNG' THEN 'B/MET'
        WHEN (dati ->> 'Fuel Type') = 'Petrol / Ethanol - E85' THEN 'B/ETA'
        WHEN (dati ->> 'Fuel Type') = 'Diesel / electricity' THEN 'IBRIDO GASOLIO/ELETTRICO'
        WHEN (dati ->> 'Fuel Type') = 'Petrol / LPG' THEN 'B/GPL'
        WHEN (dati ->> 'Fuel Type') = 'LPG' THEN 'GPL'
        WHEN (dati ->> 'Fuel Type') = 'Hydrogen' THEN 'IDROGENO'
        ELSE NULL
    END AS fuel_type,
    CASE 
        WHEN (dati->>'System power') IS NOT NULL THEN regexp_replace((dati ->> 'System power'), '[^\d].*', '', 'g')::NUMERIC 
        WHEN (dati->>'System power') IS NULL AND (dati->>'Power') IS NULL THEN regexp_replace((dati ->> 'Power (CNG)'), '[^\d].*', '', 'g')::NUMERIC 
        ELSE regexp_replace((dati->>'Power'), '[^\d].*', '', 'g')::NUMERIC 
    END AS engine_power,
    CASE
        WHEN (dati ->> 'Fuel Type') = 'Electricity' THEN 0
        WHEN (dati ->> 'Fuel Type') = 'Hydrogen' THEN 0
        ELSE regexp_replace((dati ->> 'Engine displacement'), '[^\d].*', '', 'g')::DOUBLE PRECISION
    END AS engine_displacement,        
    regexp_replace((dati ->> 'Kerb Weight'), '[^\d].*', '', 'g')::NUMERIC as kerb_weight,
    CASE
        WHEN (dati ->> 'Fuel Type') = 'Electricity' THEN 0
        WHEN (dati ->> 'Fuel Type') = 'Hydrogen' THEN 0
        WHEN (dati ->> 'CO2 emissions') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (CNG)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (CNG)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (CNG) (NEDC)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (CNG) (NEDC)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (CNG) (NEDC, WLTP equivalent)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (CNG) (NEDC, WLTP equivalent)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (CNG) (WLTP)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (CNG) (WLTP)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (EPA)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (EPA)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (Ethanol - E85)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (Ethanol - E85)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (LPG)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (LPG)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (LPG) (NEDC)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (LPG) (NEDC)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (LPG) (NEDC, WLTP equivalent)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (LPG) (NEDC, WLTP equivalent)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (LPG) (WLTP)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (LPG) (WLTP)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (NEDC)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (NEDC)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (NEDC, WLTP equivalent)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (NEDC, WLTP equivalent)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (WLTP)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (WLTP)'), '[^\d].*', '', 'g')::NUMERIC
        WHEN (dati ->> 'CO2 emissions (WLTC)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (WLTC)'), '[^\d].*', '', 'g')::NUMERIC
        ELSE NULL
    END AS co2_emissions
    FROM {{ source('dwh_car_fleet', 'raw_car_spec') }}
)
SELECT DISTINCT ON(fcc.car_id) --308393 match
    fcc.car_id,
    fcc.datereg_id,
    fcc.brand_id,
    scs.model,
    fcc.province_id,
    fcc.engine_power,
    fcc.engine_displacement,
    fcc.fuel_type,
    CASE
        WHEN fcc.co2_emissions IS NULL THEN scs.co2_emissions
        ELSE fcc.co2_emissions
    END AS co2_emissions,
    fcc.kerb_weight
FROM fct_car_temp fcc 
INNER JOIN stg_car_spec scs ON fcc.brand_id = scs.brand AND fcc.fuel_type = scs.fuel_type 
                            AND fcc.engine_power = scs.engine_power
                            AND fcc.engine_displacement = scs.engine_displacement   --1337713 match
                            --AND fcc.kerb_weight = scs.kerb_weight                   --26 match
                            --AND fcc.co2_emissions = scs.co2_emissions                --0 match
--ORDER BY fcc.car_id
--INNER JOIN {{ ref('dim_datereg') }} dtr ON fc.datereg_id = dtr.datereg_id         --con questi ci mette 29min, troppo
--INNER JOIN {{ ref('dim_brand')}} dbr ON fc.brand_id = dbr.brand_id
--INNER JOIN {{ ref('dim_province')}} dpv ON fc.province_id = dpv.province_id       















/*SELECT
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
*/