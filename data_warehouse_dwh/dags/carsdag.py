from airflow import DAG

from airflow.decorators import (
    dag,
    task,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#from airflow.models.baseoperator import chain 
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

import os


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "DWH_ECS_dag"
@dag(
    dag_id=DAG_ID,
    start_date=datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
)
def cars_dag():
    create_car_table = SQLExecuteQueryOperator(
        task_id="create_car_table",
        conn_id="dwh_pgres",
        sql="sql/car_valle_aosta_schema.sql"
    )
    
    @task
    def load_data():
        data_path = "include/dataset/Circolante_Valle_Aosta.csv"

        postgres_hook = PostgresHook(postgres_conn_id="dwh_pgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY car_valle_aosta FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()
    
    create_car_table >> load_data()

dag = cars_dag()