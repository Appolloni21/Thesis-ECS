DROP TABLE IF EXISTS raw_car_spec;
CREATE TABLE IF NOT EXISTS raw_car_spec (
    id SERIAL PRIMARY KEY,
    dati JSONB
);