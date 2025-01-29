SELECT
    fct.car_id,
    fct.datereg_id,
    fct.province_id,
    dp.province_iso_code,
    dp.region_iso_code,
    dp.region,
    dm.model,
    dm.brand,
    fct.engine_power,
    fct.engine_displacement,
    fct.fuel_type,
    fct.co2_emissions,
    fct.max_weight
FROM {{ ref('fct_car') }} fct
INNER JOIN {{ ref('dim_province') }} dp ON fct.province_id = dp.province_id
INNER JOIN {{ ref('dim_model' )}} dm ON fct.model_id = dm.model_id
INNER JOIN {{ ref('dim_datereg' )}} dtr ON fct.datereg_id = dtr.datereg_id