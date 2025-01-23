-- STAGING TABLE 
WITH stg_car_temp AS(
    --4.195.279 match 
    SELECT
        ROW_NUMBER() OVER () AS car_id,
        TO_DATE(immatricolazione, 'DD/MM/YYYY') AS datereg_id,
        CASE 
            WHEN marca='FIAT - INNOCENTI' THEN 'FIAT'
            WHEN marca='LANCIA - AUTOBIANCHI' THEN 'LANCIA'
            WHEN marca='MORGAN MOTOR' THEN 'MORGAN'
            WHEN marca='ROLLS ROYCE' THEN 'ROLLS-ROYCE'
            WHEN marca='ROVER CARS' THEN 'ROVER'
            WHEN marca='SHUANGHUAN AUTO' THEN 'SHUANGHAUN'
            WHEN marca='TESLA MOTORS' THEN 'TESLA'
            ELSE marca
        END AS brand,
        provincia as province_id,
        potenza as engine_power,
        CASE
            WHEN alimentazione='ELETTR' THEN 0
            ELSE cilindrata
        END AS engine_displacement,
        alimentazione as fuel_type,
        CASE
            WHEN alimentazione='ELETTR' THEN 0
            ELSE emissioni
        END AS co2_emissions,
        peso as kerb_weight
    FROM {{ source('dwh_car_fleet', 'raw_car_circulating') }}
    WHERE destinazione='AUTOVETTURA PER TRASPORTO DI PERSONE' /*AND make IS NOT NULL AND provincia IS NOT NULL 
        AND immatricolazione IS NOT NULL 
        AND engine_power IS NOT NULL*/ 
        --AND displacement IS NOT NULL 
        --AND fuel IS NOT NULL
		--AND emissioni IS NOT NULL 
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
SELECT DISTINCT ON(sct.car_id) 
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
FULL OUTER JOIN stg_car_spec scs ON sct.brand = scs.brand AND sct.fuel_type = scs.fuel_type 
                            AND sct.engine_power = scs.engine_power
                            AND sct.engine_displacement = scs.engine_displacement  
--4.195.280 match