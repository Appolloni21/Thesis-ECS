-- create regions table
CREATE TABLE IF NOT EXISTS raw_regions (
            istat_code NUMERIC,
            denominazione_ita_altra VARCHAR,
            denominazione_ita VARCHAR,
            denominazione_altra VARCHAR, 
            cap NUMERIC,
            sigla_provincia VARCHAR,
            denominazione_provincia VARCHAR,
            tipologia_provincia VARCHAR,
            codice_regione NUMERIC,
            denominazione_regione VARCHAR,
            tipologia_regione VARCHAR,
            ripartizione_geografica VARCHAR,
            flag_capoluogo VARCHAR,
            codice_belfiore VARCHAR,
            lat VARCHAR,
            lon VARCHAR,
            superficie_kmq VARCHAR ); 