# Investigating a Data Warehouse Realization through Open Source Tools. The Case Study of the Italian Circulating Car Fleet

This repository contains all the code I developed for my Master's thesis in Engineering in Computer Science.

This project demonstrates how to implement a Data Warehouse for the analysis of data regarding the italian circulating vehicle fleet in 2019. The Datasets are downloaded manually and the data pipeline takes care of loading and transforming the data into the Data Warehouse.

All the code and tools have been tested on a Virtual Machine with Ubuntu 22.04.5.

## Architecture
![arch_pipeline_white](https://github.com/user-attachments/assets/f3d28c03-0aa5-4a0c-85d0-8c197356083e)

- Data Warehouse: PostgreSQL
- Orchestration: Apache Airflow
- ELT: dbt
- Visualization: Apache Superset

## Prerequisites
- [Docker](https://docs.docker.com/get-started/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)

## Setup
1. Clone this repository locally
    ```
    git clone https://github.com/Appolloni21/Thesis-ECS.git
    ```

2. Open a terminal and go to the project folder 
    ```
    cd data_warehouse_dwh
    ```

3. Type the following command to build all the docker containers

    ```
    astro dev start
    ```

## Getting started
1. Inside `/include` create two folders, `/datasets_2019` and `/datasets_scraping`
2. Go to the [Ministry of Infrastructure and Transport website](https://dati.mit.gov.it/catalog/dataset/dataset-parco-circolante-dei-veicoli), download all the twenty `.zip` files and extract the content into `/datasets_2019`.
The directory should appear like this:

    ```
    ├── data_warehouse_dwh
    │   ├── include
    │   │   ├── datasets_2019
    │   │   │    ├── Circolante_Abruzzo.csv
    │   │   │    ├── Circolante_Basailicata.csv
    │   │   │    ├── Circolante_Calabria.csv
    │   │   │    ├── ...
    │   │   │    ├── ...
    │   │   │    └── Circolante_Veneto.csv
    ```
3. The dataset obtained through web scraping, `car_spec.json`, it is available
in `web-scraper/` folder. Place it in `data_warehouse_dwh/include/datasets_scraping` folder. 
4. Open in a browser `https://localhost:8081/`
5. Login with `User: admin` and `Password: admin`
6. In the homepage click on `DWH_pipeline_dag` 

    ![](/images/Airflow_UI_3.png)

7. Then click on `Trigger DAG` button to start the pipeline

    ![](/images/Airflow_UI_5.png)
   
## Visualization and Data Analysis
1. Open the Superset UI in a web browser at `https://localhost:8088/` and
login with `User: admin` and `Password: admin`
2. Once signed in, click on _Settings_ -> _Database Connections_
3. Click on the blue button `+ Database` on the top right, then select _PostgreSQL_ as database to connect
4. Then click on `Connect this database with a SQLAlchemy URI string instead`
5. In `SQLAlchemy URI` field put the following string

    ```
    postgresql+psycopg2://dwh_warehouse_ecs:postgres-password@172.17.0.1:7432/dwh_warehouse_ecs
    ```
6. Click on _Test Connection_. It should tell connection works, next click on
_Connect_ button.

Apache Superset is configured properly and it is ready to see the data inside the Data Warehouse.
## Snapshot of the dashboard
![](/images/dashboard.png)