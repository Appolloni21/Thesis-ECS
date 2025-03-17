WITH stg_province AS (
    SELECT DISTINCT
        CASE
			WHEN denominazione_provincia = 'Valle d''Aosta/Vallée d''Aoste' THEN 'AOSTA'
			WHEN denominazione_provincia = 'Bolzano/Bozen' THEN 'BOLZANO'
			WHEN denominazione_provincia = 'Monza e della Brianza' THEN 'MONZA E BRIANZA'
			WHEN denominazione_provincia = 'Reggio nell''Emilia' THEN 'REGGIO EMILIA'
			WHEN denominazione_provincia = 'Sud Sardegna' THEN 'CARBONIA-IGLESIAS'
			ELSE UPPER(denominazione_provincia) 
		END AS province_id,
        denominazione_regione AS region,
        ripartizione_geografica AS territory
    FROM {{ source('dwh_car_fleet', 'raw_province') }}
    WHERE denominazione_provincia IS NOT NULL
),
stg_iso_province AS(
	SELECT
		CASE
			WHEN region_name = 'Forli-Cesena' THEN 'FORLÌ-CESENA'
			ELSE UPPER(region_name)
		END AS province,
		CASE
            WHEN region_name = 'Pesaro e Urbino' THEN 'IT-PU'
			ELSE (country_short_code || '-' || regional_code) 
		END AS province_iso_code
	FROM {{ source('dwh_car_fleet', 'raw_iso_code') }}
	WHERE region_type='Province'
),
stg_iso_region AS(
	SELECT
		CASE
			WHEN region_name='Trentino-Alto Adige' THEN 'Trentino-Alto Adige/Südtirol'
			WHEN region_name='Valle d''Aosta' THEN 'Valle d''Aosta/Vallée d''Aoste'
			ELSE region_name
		END AS region,
		(country_short_code || '-' || regional_code) AS region_iso_code
	FROM {{ source('dwh_car_fleet', 'raw_iso_code') }}
	WHERE region_type='region'
)
SELECT DISTINCT ON (sp.province_id)
	sp.province_id,
	sp.region,
	sp.territory,
	sip.province_iso_code,
	sir.region_iso_code
FROM stg_province sp
INNER JOIN stg_iso_province sip ON sp.province_id = sip.province
INNER JOIN stg_iso_region sir ON sp.region = sir.region
--107 rows