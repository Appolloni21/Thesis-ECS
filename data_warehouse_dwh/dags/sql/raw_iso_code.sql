DROP TABLE IF EXISTS raw_iso_code;
CREATE TABLE IF NOT EXISTS raw_iso_code (
    country_name TEXT,
    country_short_code TEXT,
    country_long_code TEXT,
    country_number_code NUMERIC,
    region_name TEXT,
    region_type TEXT,
    regional_code TEXT,
    regional_number_code NUMERIC
);