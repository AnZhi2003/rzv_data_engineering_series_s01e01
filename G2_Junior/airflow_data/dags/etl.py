"""Сбор данных из таблиц БД источников (shop-1, shop-2) -> инкрементальное обновление данных в DWH"""

import pendulum
import json
import logging
import os
import pandas as pd

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from func_etl import checking_schema_tables_columns_in_DB, get_max_created_at, get_table_from_DB, get_sqlalchemy_engine

"""Variables"""
var_airflow_etl_str = Variable.get("var_airflow_etl")
var_airflow_etl_dict = json.loads(var_airflow_etl_str)
var_DWH_tables = var_airflow_etl_dict["tables"]
var_source_conn = var_airflow_etl_dict["source_conn"]

"""Connection to DWH"""
DWH_engine = get_sqlalchemy_engine("pg-dwh")

default_args = {"owner": "anzhi", 
                "depends_on_past": False,
                "start_date": datetime(2024, 7, 2, 12, 0, 0, tzinfo=pendulum.timezone("UTC")),
                "retry_delay": timedelta(minutes=5),
                "retries": 1,
                }

@dag(default_args=default_args, catchup=False, tags=["etl"], schedule_interval='*/2 * * * *')
def etl():

    @task()
    def checking_tables_in_DWH():
        return checking_schema_tables_columns_in_DB(DWH_engine, "dds", var_DWH_tables)
    
    @task()
    def extract_transform(**context) -> list:
        """
        В каждой БД-источнике, в каждой таблице:
        1. Получить таблицу, в которой created_at > max_created_at_target
        2. Если max_created_at_target == None, то из source_table получить все данные, что в ней есть. 
        3. Добавить столбцы 'igested_at', 'city_name', 'id_source' (доп ключ по {table}_id и src_id)
        4. Сохранить в csv с именем файла 'execution_date_dag_id' в '/tmp/airflow_staging'
        5. Вернуть полный путь к файлу
        """
        context = get_current_context()
        dag_run_id = context["dag_run"].run_id
        dag_run_datetime = context["dag_run"].execution_date
        dag_run_date = context["dag_run"].execution_date.date()
    
        list_of_paths = []

        for source_key, source_param in var_source_conn.items():
            source_name = source_key
            source_engine = get_sqlalchemy_engine(source_param["hook"])
            city_name = source_param["city_name"]
            
            for table_name in var_DWH_tables.keys():
                # Максимальная дата в DWH:
                max_created_at_DWH = get_max_created_at(DWH_engine, "dds", table_name) 
                # Dataframe после max_created_at_DWH:
                df_source = get_table_from_DB(source_engine, "public", table_name, max_created_at_DWH) 
                
                # Если вернется пустой df:
                if df_source.empty:
                    logging.info(f"No new data found for table {table_name}")
                    continue
                
                # Добавляем дополнительные столбцы:
                df_source["igested_at"] = dag_run_datetime
                df_source["city_name"] = city_name
                df_source["id_source"] = df_source[df_source.columns[0]].apply(lambda x: f"{source_name}_{x}")

                # Добавить создание файла и записать путь в виде строки в список list_of_paths
                file_name = f"{dag_run_id}_{dag_run_date}_{table_name}.csv"
                file_path = os.path.join('/tmp/airflow_staging', file_name)
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                df_source.to_csv(file_path, sep = ',', index=False, header=True, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S', float_format='%.2f')

                list_of_paths.append(file_path)
        
        return list_of_paths

    @task()
    def load(list_of_paths, DWH_engine: Engine):
        """
        1. Получить датафрейм из файла, полученного на предыдущем шаге.
        2. df.to_sql()
        """
        schema_name = "dds"

        for file_path in list_of_paths:
            df = pd.read_csv(file_path)
            table_name = file_path.split('/')[-1].split('_')[-1].split('.')[0]

            try:
                df.to_sql(table_name, DWH_engine, schema=schema_name,if_exists='append', index=False)
                logging.info(f"Data loaded successfully into {schema_name}.{table_name}")
            except Exception as e:
                logging.error(f"Error loading data into {schema_name}.{table_name}: {e}")



    checking_tables_in_DWH_task = checking_tables_in_DWH()
    extract_transform_task = extract_transform()
    load_task = load(extract_transform_task, DWH_engine)

    checking_tables_in_DWH_task >> extract_transform_task >> load_task

etl()