
-- create car schema
CREATE TABLE IF NOT EXISTS raw_car_temp(
            carid NUMERIC,
            v_type VARCHAR,
            dest VARCHAR,
            utilization VARCHAR,
            provincia VARCHAR,
            make VARCHAR,
            displacement FLOAT, 
            fuel VARCHAR,
            engine_power NUMERIC, 
            immatricolazione VARCHAR,
            classe VARCHAR,
            emissioni NUMERIC, 
            peso NUMERIC); 