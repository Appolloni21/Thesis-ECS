DROP TABLE IF EXISTS raw_car_spec;
CREATE TABLE IF NOT EXISTS raw_car_spec (
    id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    dati JSONB
);