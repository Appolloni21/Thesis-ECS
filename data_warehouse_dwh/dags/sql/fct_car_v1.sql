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
INNER JOIN {{ ref('dim_datereg') }} dtr ON scc.datereg_id = dtr.datereg_id
INNER JOIN {{ ref('dim_model')}} dmo ON scc.model_id = dmo.model_id
INNER JOIN {{ ref('dim_province')}} dpv ON scc.province_id = dpv.province_id
--308393 match