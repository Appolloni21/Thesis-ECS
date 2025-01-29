WITH stg_province AS (
    SELECT DISTINCT
        UPPER(denominazione_provincia) AS province_id,
        denominazione_regione AS region,
        ripartizione_geografica AS territory
    FROM {{ source('dwh_car_fleet', 'raw_province') }}
    WHERE denominazione_provincia IS NOT NULL
),
stg_iso_province AS(
	SELECT
		CASE
			WHEN region_name = 'Forli-Cesena' THEN 'Forlì-Cesena'
			WHEN region_name = 'Carbonia-Iglesias' THEN 'Sud Sardegna'
			WHEN region_name = 'Reggio Emilia' THEN 'Reggio Nell''Emilia'
			WHEN region_name = 'Monza e Brianza' THEN 'Monza e della Brianza'
			WHEN region_name = 'Bolzano' THEN 'Bolzano/Bozen'
			WHEN region_name = 'Aosta' THEN 'VALLE D''AOSTA/VALLÉE D''AOSTE'
			ELSE region_name
		END AS province,
		CASE
			WHEN region_name = 'Carbonia-Iglesias' THEN 'IT-SU'
            WHEN region_name = 'Pesaro e Urbino' THEN 'IT-PU'
			ELSE (country_short_code || '-' || regional_code) 
		END AS province_iso_code
	FROM {{ source('dwh_car_fleet', 'raw_iso_code') }}
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
INNER JOIN stg_iso_province sip ON sp.province_id = UPPER(sip.province)
INNER JOIN stg_iso_region sir ON sp.region = sir.region
--107 rows