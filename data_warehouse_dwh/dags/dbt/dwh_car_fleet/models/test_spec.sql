WITH temp_spec AS (
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
END AS CO2_emissions
FROM {{ source('dwh_car_fleet', 'raw_car_spec') }}
)