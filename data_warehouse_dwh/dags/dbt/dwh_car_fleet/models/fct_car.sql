WITH fct_car_temp AS(
    SELECT
        immatricolazione as dateregistration_id,
        {{ dbt_utils.generate_surrogate_key(['make']) }} AS brand_id,
        {{ dbt_utils.generate_surrogate_key(['provincia']) }} as province_id,
        engine_power as engine_power,
        displacement as displacement,
        fuel as fuel_type,
        emissioni as emissions,
        peso as weight_mass
    FROM {{ source('dwh_car_fleet', 'raw_car_circulating') }}
)
SELECT
    dt.dateregistration_id,
    dbr.brand_id,
    dp.province_id,
    engine_power,
    displacement,
    fuel_type,
    emissions,
    weight_mass
FROM fct_car_temp fc
INNER JOIN {{ ref('dim_dateregistration') }} dt ON fc.dateregistration_id = dt.dateregistration_id
INNER JOIN {{ ref('dim_brand')}} dbr ON fc.brand_id = dbr.brand_id
INNER JOIN {{ ref('dim_province')}} dp ON fc.province_id = dp.province_id