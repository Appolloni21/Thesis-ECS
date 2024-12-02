/* BASE 1

SELECT
id as vid,
(dati ->> 'Brand') AS brand,
dati ->> 'Model' AS model,
regexp_replace((dati ->> 'CO2 emissions'), '[^\d].*', '', 'g')::NUMERIC AS emissions,   
dati ->> 'Fuel Type' AS fuel_type,
regexp_replace((dati ->> 'Power'), '[^\d].*', '', 'g')::NUMERIC AS engine_power,        
regexp_replace((dati ->> 'Kerb Weight'), '[^\d].*', '', 'g')::NUMERIC as kerb_weight    
FROM raw_car_spec
order by vid
*/

/* BASE 2 */

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
FROM raw_car_spec
