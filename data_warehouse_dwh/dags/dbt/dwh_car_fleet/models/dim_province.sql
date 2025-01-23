WITH province_temp AS (
    SELECT DISTINCT
        --{{ dbt_utils.generate_surrogate_key(['denominazione_provincia']) }} as province_id,
        UPPER(denominazione_provincia) AS province_id,
        denominazione_regione AS region,
        ripartizione_geografica AS territory
    FROM {{ source('dwh_car_fleet', 'raw_province') }}
    WHERE denominazione_provincia IS NOT NULL
),
iso_province_temp AS(
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
iso_region_temp AS(
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
SELECT DISTINCT ON (pt.province_id)
	pt.province_id,
	pt.region,
	pt.territory,
	ipt.province_iso_code,
	irt.region_iso_code
FROM province_temp pt
INNER JOIN iso_province_temp ipt ON pt.province_id = UPPER(ipt.province)
INNER JOIN iso_region_temp irt ON pt.region = irt.region
--107 rows