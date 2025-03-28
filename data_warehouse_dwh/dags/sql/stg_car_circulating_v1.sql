-- STAGING TABLE
WITH stg_car_temp AS(
    SELECT
        ROW_NUMBER() OVER () AS car_id,
        TO_DATE(immatricolazione, 'DD/MM/YYYY') AS datereg_id,
        CASE 
            WHEN make='FIAT - INNOCENTI' THEN 'FIAT'
            WHEN make='LANCIA - AUTOBIANCHI' THEN 'LANCIA'
            WHEN make='MORGAN MOTOR' THEN 'MORGAN'
            WHEN make='ROLLS ROYCE' THEN 'ROLLS-ROYCE'
            WHEN make='ROVER CARS' THEN 'ROVER'
            WHEN make='SHUANGHUAN AUTO' THEN 'SHUANGHAUN'
            WHEN make='TESLA MOTORS' THEN 'TESLA'
            ELSE make
        END AS brand,
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
SELECT DISTINCT ON(sct.car_id) --308393 match
    sct.car_id,
    sct.datereg_id,
    sct.province_id,
    --sct.brand,
    --scs.model,
    {{ dbt_utils.generate_surrogate_key(['sct.brand', 'scs.model']) }} AS model_id,
    sct.engine_power,
    sct.engine_displacement,
    sct.fuel_type,
    CASE
        WHEN sct.co2_emissions IS NULL THEN scs.co2_emissions
        ELSE sct.co2_emissions
    END AS co2_emissions,
    sct.kerb_weight
FROM stg_car_temp sct 
INNER JOIN stg_car_spec scs ON sct.brand = scs.brand AND sct.fuel_type = scs.fuel_type 
                            AND sct.engine_power = scs.engine_power
                            AND sct.engine_displacement = scs.engine_displacement   --1337713 match
                            --AND sct.kerb_weight = scs.kerb_weight                   --26 match
                            --AND sct.co2_emissions = scs.co2_emissions                --0 match
--ORDER BY fcc.car_id
--INNER JOIN {{ ref('dim_datereg') }} dtr ON sct.datereg_id = dtr.datereg_id         
--INNER JOIN {{ ref('dim_model')}} dmo ON sct.brand = dmo.brand               --29minutes query,
--INNER JOIN {{ ref('dim_province')}} dpv ON sct.province_id = dpv.province_id       
