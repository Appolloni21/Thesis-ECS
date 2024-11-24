
-- create car schema
CREATE TABLE IF NOT EXISTS raw_car_fleet_B (
            carid NUMERIC,
            v_type VARCHAR,
            dest VARCHAR,
            utilization VARCHAR,
            provincia VARCHAR,
            make VARCHAR,
            displacement VARCHAR, --FLOAT
            fuel VARCHAR,
            engine_power VARCHAR, --INT
            immatricolazione VARCHAR,
            classe VARCHAR,
            emissioni VARCHAR, --NUMERIC
            peso VARCHAR); --INT