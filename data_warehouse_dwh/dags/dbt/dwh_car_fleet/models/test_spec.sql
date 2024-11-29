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

SELECT
id as vid,
dati ->> 'Brand' AS brand,
dati ->> 'Model' AS model,
dati ->> 'Fuel Type' AS fuel_type,
CASE 
     WHEN (dati->>'System power') IS NOT NULL THEN regexp_replace((dati ->> 'System power'), '[^\d].*', '', 'g')::NUMERIC 
     ELSE regexp_replace((dati ->> 'Power'), '[^\d].*', '', 'g')::NUMERIC 
END AS engine_power,        
regexp_replace((dati ->> 'Kerb Weight'), '[^\d].*', '', 'g')::NUMERIC as kerb_weight
CASE 
    WHEN (dati ->> 'CO2 emissions') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions'), '[^\d].*', '', 'g')::NUMERIC
    WHEN (dati ->> 'CO2 emissions (WLTC)') IS NOT NULL THEN regexp_replace((dati ->> 'CO2 emissions'), '[^\d].*', '', 'g')::NUMERIC
END AS CO2_emissions
FROM raw_car_spec
order by vid

-- CO2 emissions, CO2 emissions (WLTP), CO2 emissions (NEDC) (ce ne sono altri)
-- CO2 emissions (WLTC) compare sempre da solo

-- Power, System Power, "Power (CNG)","Power (Ethanol - E85)", "Power (LPG)",


-- mettere condizione che se CO2 emission è null è perchè è un veicolo BEV, quindi le emissioni sono 0 e non null


--Fuel Type
--Kerb Weight, forse anche 'Max. weight'