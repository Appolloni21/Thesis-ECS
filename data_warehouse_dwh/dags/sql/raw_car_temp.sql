
-- create car schema
/*CREATE TABLE IF NOT EXISTS raw_car_temp(
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
*/
CREATE TABLE IF NOT EXISTS raw_car_temp(
            progressivo NUMERIC,
            tipo_veicolo TEXT,
            destinazione TEXT,
            uso TEXT,
            provincia TEXT,
            marca TEXT,
            cilindrata FLOAT,
            alimentazione TEXT,
            potenza NUMERIC,
            immatricolazione TEXT,
            classe TEXT,
            emissioni NUMERIC,
            peso NUMERIC);   