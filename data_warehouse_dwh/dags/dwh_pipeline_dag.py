from airflow import DAG

from airflow.decorators import (
    dag,
    task,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

# from airflow.models.baseoperator import chain
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

from include.utilities import get_data, DATA_PATHS, DATA_PATHS_2
import os, json


ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "DWH_pipeline_dag"


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2021, 2, 2),
    schedule="@once",
    catchup=False,
)
def dwh_pipeline_dag():

    # TASK 1: extract raw data from sources
    extract_1 = PythonOperator(
        task_id="extract_1",
        python_callable=get_data,
    )

    # TASK 2: create main raw table
    create_raw_table_1 = SQLExecuteQueryOperator(
        task_id="create_raw_table_1",
        conn_id="dwh_pgres",
        sql="sql/raw_circolante_2019.sql",
    )

    # TASK 3: load data
    @task
    def load_data_1():
        # data_path = "resources/parco_circolante_Abruzzo.csv"
        postgres_hook = PostgresHook(postgres_conn_id="dwh_pgres")
        conn = postgres_hook.get_conn()
        for data_path in DATA_PATHS:
            print(data_path)
            cur = conn.cursor()
            with open(data_path, "r") as file:
                cur.copy_expert(
                    # "COPY raw_car_fleet FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE E'\b' ",
                    "COPY raw_car_fleet FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"' ",
                    file,
                )
            conn.commit()

    # TASK 4: create main raw table
    create_raw_table_2 = SQLExecuteQueryOperator(
        task_id="create_raw_table_2",
        conn_id="dwh_pgres",
        sql="sql/raw_car_fleet_B.sql",
    )

    # TASK 4: load data
    @task
    def load_data_2():
        # data_path = "resources/parco_circolante_Abruzzo.csv"
        postgres_hook = PostgresHook(postgres_conn_id="dwh_pgres")
        conn = postgres_hook.get_conn()
        for data_path in DATA_PATHS_2:
            print(data_path)
            cur = conn.cursor()
            with open(data_path, "r") as file:
                cur.copy_expert(
                    "COPY raw_car_fleet_B FROM STDIN WITH ( FORMAT CSV, HEADER, DELIMITER ',', QUOTE '\"') ",
                    file,
                )
            conn.commit()

    # TASK 5: create table for regions and provinces
    create_raw_table_3 = SQLExecuteQueryOperator(
        task_id="create_raw_table_3",
        conn_id="dwh_pgres",
        sql="sql/raw_regions.sql",
    )

    # TASK 6: load regions and provinces data
    @task
    def load_data_3():
        data_path = "include/gi_comuni_cap.csv"
        postgres_hook = PostgresHook(postgres_conn_id="dwh_pgres")
        conn = postgres_hook.get_conn()

        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY raw_regions FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"' ",
                file,
            )
        conn.commit()

    # TASK 7
    create_car_spec_table = SQLExecuteQueryOperator(
        task_id="create_car_spec_table",
        conn_id="dwh_pgres",
        sql="sql/raw_car_spec_1.sql"
    )

    # TASK 8
    @task
    def load_car_spec():
        data_path = "include/datasets_scraping/cars_test_2.json"
        postgres_hook = PostgresHook(postgres_conn_id="dwh_pgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            data = json.load(file)

        query = """ INSERT INTO raw_car_spec (dati) VALUES (%s); """

        # Inserisce i dati in un unica riga e unica colonna
        # cur.execute(query, [json.dumps(data)])

        # Inserisce i dati in un unica colonna ma in più righe
        for row in data.values():
            cur.execute(query, [json.dumps(row)])

        conn.commit()

    # extract_1
    # create_raw_table_1 >> load_data_1() >>
    create_raw_table_2 >> load_data_2() 
    # create_raw_table_3 >> load_data_3()
    # create_car_spec_table >> load_car_spec()

dag = dwh_pipeline_dag()
