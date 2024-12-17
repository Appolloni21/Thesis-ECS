import os, json
from airflow import DAG

from airflow.decorators import (
    dag,
    task,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain

# from airflow.models.baseoperator import chain
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

from include.utilities import *
from pathlib import Path 

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode
from cosmos.profiles import PostgresUserPasswordProfileMapping 
  
DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt" 
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

project_config = ProjectConfig(
    dbt_project_path='/usr/local/airflow/dags/dbt/dwh_car_fleet'
)
  
profile_config = ProfileConfig( 
     profile_name="default", 
     target_name="dev", 
     profile_mapping=PostgresUserPasswordProfileMapping( 
         conn_id="dwh_pgres", 
         profile_args={"schema": "public",
                       "port": 7432,
                        "dbname": "dwh_warehouse_ecs" }, 
     ), 
) 

execution_config = ExecutionConfig(
        execution_mode=ExecutionMode.VIRTUALENV,
        dbt_executable_path = DBT_EXECUTABLE_PATH 
     )

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
    #extract_1 = PythonOperator(
    #    task_id="extract_1",
    #    python_callable=get_data,
    #)

    # TASK 2: create circulating car table 1
    create_raw_table_1 = SQLExecuteQueryOperator(
        task_id="create_raw_table_1",
        conn_id="dwh_pgres",
        sql="sql/raw_car_circulating.sql",
    )

   
    # TASK 2b: create circulating car temp table
    create_raw_table_2 = SQLExecuteQueryOperator(
        task_id="create_raw_table_2",
        conn_id="dwh_pgres",
        sql="sql/raw_car_temp.sql",
    )


    # TASK 3: load data
    @task
    def load_data_car_circulating():
        postgres_hook = PostgresHook(postgres_conn_id="dwh_pgres")
        conn = postgres_hook.get_conn()

        for file_name in os.listdir(DATASETS_2019_DIR):
            # Controlla se il file ha estensione .csv
            data_path = os.path.join(DATASETS_2019_DIR, file_name)
            if file_name.endswith("Friuli.csv"):
                query = "COPY raw_car_temp FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '\"') "
                postgreSQL_importing(query, conn, data_path)
            #elif(get_region_name(file_name) in REGIONS_A):
            elif(file_name.endswith("Abruzzo.csv")):
                query = "COPY raw_car_circulating FROM STDIN WITH CSV DELIMITER AS ',' QUOTE '\"' "
                postgreSQL_importing(query,conn,data_path)
            #else:
            elif(file_name.endswith("Puglia.csv")):
                query = "COPY raw_car_temp FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '\"') "
                postgreSQL_importing(query,conn,data_path)

    # TASK 4: create table for regions and provinces
    create_raw_province = SQLExecuteQueryOperator(
        task_id="create_raw_province",
        conn_id="dwh_pgres",
        sql="sql/raw_province.sql",
    )

    # TASK 5: load regions and provinces data
    @task
    def load_data_province():
        data_path = "include/gi_comuni_cap.csv"
        postgres_hook = PostgresHook(postgres_conn_id="dwh_pgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY raw_province FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"' ",
                file,
            )
        conn.commit()

    # TASK 6: create car spec table
    create_raw_car_spec = SQLExecuteQueryOperator(
        task_id="create_raw_car_spec",
        conn_id="dwh_pgres",
        sql="sql/raw_car_spec.sql"
    )

    # TASK 7: load car spec table
    @task
    def load_car_spec():
        data_path = "include/datasets_scraping/car_spec.json"
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

    # TASK 8: cleaning
    cleaning_temp_table = SQLExecuteQueryOperator(
        task_id="cleaning_temp_table",
        conn_id="dwh_pgres",
        sql="sql/cleaning_temp_table.sql",
    )

    # TASK -: create iso code table
    create_raw_iso_code = SQLExecuteQueryOperator(
        task_id="create_raw_iso_code",
        conn_id="dwh_pgres",
        sql="sql/raw_iso_code.sql"
    )

    # TASK -: load iso code table
    @task
    def load_iso_code():
        data_path = "include/ISO-3166-2-IT.csv"
        postgres_hook = PostgresHook(postgres_conn_id="dwh_pgres")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY raw_iso_code FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"' ",
                file,
            )
        conn.commit()

    transform = DbtTaskGroup(
        group_id='transform',
        # dbt/cosmos-specific parameters 
        project_config=project_config, 
        profile_config=profile_config,
        execution_config = execution_config, 
        operator_args={ 
            "install_deps": True,  # install any necessary dependencies before running any dbt command 
        }, 
    )

    chain(  #extract_1,
            create_raw_table_1,
            create_raw_table_2,
            load_data_car_circulating(),
            create_raw_province,
            load_data_province(),
            create_raw_car_spec,
            load_car_spec(),
            cleaning_temp_table,
            create_raw_iso_code,
            load_iso_code(),
            transform
    )

dag = dwh_pipeline_dag()
