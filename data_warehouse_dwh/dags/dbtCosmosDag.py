""" 
 An example DAG that uses Cosmos to render a dbt project. 

 TESTING ONLY: DO NOT USE
""" 
  
import os 
from datetime import datetime 
from pathlib import Path 
from airflow.decorators import dag
from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode
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


@dag( 
     # normal dag parameters 
     schedule_interval=None, 
     start_date=datetime(2023, 1, 1), 
     catchup=False, 
     dag_id="dbt_AcosmosDag", 
) 
def dbtCosm_dag():
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

    transform 


dbtCosm_dag()
