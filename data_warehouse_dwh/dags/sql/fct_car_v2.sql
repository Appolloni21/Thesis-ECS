SELECT
	scc.car_id,
    dtr.datereg_id,
    dpv.province_id,
    dmo.model_id,
    scc.engine_power,
    scc.engine_displacement,
    scc.fuel_type,
    scc.co2_emissions,
    scc.kerb_weight
FROM {{ ref('stg_car_circulating') }} scc
FULL OUTER JOIN {{ ref('dim_datereg') }} dtr ON scc.datereg_id = dtr.datereg_id
FULL OUTER JOIN {{ ref('dim_model')}} dmo ON scc.model_id = dmo.model_id
FULL OUTER JOIN {{ ref('dim_province')}} dpv ON scc.province_id = dpv.province_id
WHERE scc.car_id IS NOT NULL --4.195.279 match, otherwise without this WHERE clause there would be 4.200.554 match
