-- create regions table
CREATE TABLE IF NOT EXISTS raw_province (
            istat_code NUMERIC,
            denominazione_ita_altra TEXT,
            denominazione_ita TEXT,
            denominazione_altra TEXT, 
            cap NUMERIC,
            sigla_provincia TEXT,
            denominazione_provincia TEXT,
            tipologia_provincia TEXT,
            codice_regione NUMERIC,
            denominazione_regione TEXT,
            tipologia_regione TEXT,
            ripartizione_geografica TEXT,
            flag_capoluogo TEXT,
            codice_belfiore TEXT,
            lat TEXT,
            lon TEXT,
            superficie_kmq TEXT ); 