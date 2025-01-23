-- create circulating cars table A
/*CREATE TABLE IF NOT EXISTS raw_car_circulating (
            v_type TEXT,
            dest TEXT,
            utilization TEXT,
            provincia TEXT,
            make TEXT,
            displacement FLOAT,
            fuel TEXT,
            engine_power NUMERIC,
            immatricolazione TEXT,
            classe TEXT,
            emissioni NUMERIC,
            peso NUMERIC);      --before was int 
*/
CREATE TABLE IF NOT EXISTS raw_car_circulating (
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