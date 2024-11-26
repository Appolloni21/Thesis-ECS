-- create circulating cars table A
CREATE TABLE IF NOT EXISTS raw_car_fleet (
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
            peso INT); 