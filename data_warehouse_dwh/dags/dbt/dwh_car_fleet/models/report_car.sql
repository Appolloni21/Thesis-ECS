WITH temp_spec AS (
SELECT
id as vid,
UPPER(dati ->> 'Brand') AS brand,
dati ->> 'Model' AS model,
CASE
    WHEN (dati ->> 'Fuel Type') = 'Diesel' THEN 'GASOL'
    WHEN (dati ->> 'Fuel Type') = 'Petrol (Gasoline)' THEN 'BENZ'
    WHEN (dati ->> 'Fuel Type') = 'Electricity' THEN 'ELETTR'
    WHEN (dati ->> 'Fuel Type') = 'Petrol / electricity' THEN 'IBRIDO GASOLIO/ELETTRICO'
    WHEN (dati ->> 'Fuel Type') = 'Petrol / CNG' THEN 'B/MET'
    WHEN (dati ->> 'Fuel Type') = 'Petrol / Ethanol - E85' THEN 'B/ETA'
    WHEN (dati ->> 'Fuel Type') = 'Diesel / electricity' THEN 'IBRIDO GASOLIO/ELETTRICO'
    WHEN (dati ->> 'Fuel Type') = 'Petrol / LPG' THEN 'B/GPL'
    WHEN (dati ->> 'Fuel Type') = 'LPG' THEN 'GPL'
    ELSE NULL
END AS fuel_type,
CASE 
     WHEN (dati->>'System power') IS NOT NULL THEN regexp_replace((dati ->> 'System power'), '[^\d].*', '', 'g')::NUMERIC 
     ELSE regexp_replace((dati ->> 'Power'), '[^\d].*', '', 'g')::NUMERIC 
END AS engine_power,
regexp_replace((dati ->> 'Engine displacement'), '[^\d].*', '', 'g')::DOUBLE PRECISION AS engine_displacement,        
regexp_replace((dati ->> 'Kerb Weight'), '[^\d].*', '', 'g')::NUMERIC as kerb_weight,
CASE 
    WHEN (dati ->> 'CO2 emissions') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions'), '[^\d].*', '', 'g')::NUMERIC
    WHEN (dati ->> 'CO2 emissions (WLTC)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions (WLTC)'), '[^\d].*', '', 'g')::NUMERIC
END AS CO2_emissions
FROM {{ source('dwh_car_fleet', 'raw_car_spec') }}
),
report AS (
SELECT 
    fcc.car_id AS car_id,
    fcc.datereg_id AS datereg_id,
    fcc.province_id AS province_id,
    fcc.brand_id AS brand_id,
    ts.model AS model_spec,
    fcc.engine_power AS engine_power_id,
    fcc.displacement AS displacement_id,
    fcc.fuel_type AS fuel_type_id,
    fcc.emissions AS CO2_emissions_id,
    fcc.weight_mass AS weight_mass_id
    --ROW_NUMBER() OVER (PARTITION BY fcc.car_id ORDER BY fcc.datereg_id) AS rn
FROM {{ ref('fct_car') }} fcc
INNER JOIN temp_spec ts ON brand_id = ts.brand 
    AND fcc.fuel_type = ts.fuel_type 
    AND fcc.engine_power = ts.engine_power 
)
SELECT 
    car_id,
    datereg_id,
    province_id,
    brand_id,
    model_spec,
    engine_power_id,
    displacement_id,
    fuel_type_id,
    CO2_emissions_id,
    weight_mass_id
FROM report
GROUP BY car_id,
    datereg_id,
    province_id,
    brand_id,
    model_spec,
    engine_power_id,
    displacement_id,
    fuel_type_id,
    CO2_emissions_id,
    weight_mass_id
--WHERE rn = 1

