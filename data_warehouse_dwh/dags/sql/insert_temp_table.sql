INSERT INTO raw_car_circulating
SELECT  v_type, dest, utilization, provincia, make, displacement, fuel, engine_power, immatricolazione, classe, emissioni, peso
FROM raw_car_temp