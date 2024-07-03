"""Сбор данных из таблиц БД источников (shop-1, shop-2) -> инкрементальное обновление данных в DWH"""

"""ВОПРОСЫ по https://teletype.in/@razvodov_alexey/incremental_loading_scd2:
1. stg_extract = max (created_at) - окно возможного обновления данных. Что это за окно? Как его определить?
2. stg_transform и stg_load: ...и загрузить в стейджинг слой, таблица в котором предварительно очищается. 
Зачем очищается стейджинг слой? Мы считаем, что эти данные уже загружены в финальный слой хранилища?
3. oda_extract и oda_transform_i: откуда появились данные, которые надо обновить?
4. oda_transform_u
5. i, u, d - insert, update, delete?
"""

import pendulum
import datetime as dt
import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
#import logging

"""Variables"""
var_airflow_etl_str = Variable.get("var_airflow_etl")
var_airflow_etl_dict = json.loads(var_airflow_etl_str)
var_DWH_tables = var_airflow_etl_dict["tables"]

"""Connections:"""
DWH_pg_hook = PostgresHook(postgres_conn_id="pg-dwh")
DWH_conn = DWH_pg_hook.get_conn()
DWH_cursor = DWH_conn.cursor()

shop_1_pg_hook = PostgresHook(postgres_conn_id="pg-shop-1")
shop_1_conn = shop_1_pg_hook.get_conn()
shop_1_cursor = shop_1_conn.cursor()

shop_2_pg_hook = PostgresHook(postgress_conn_id="pg-shop-2")
shop_2_conn = shop_2_pg_hook.get_conn()
shop_2_cursor = shop_2_conn.cursor()

def check_schema_in_DWH():
    """Проверка наличия таблиц в DWH. Если таблицы нет, создать таблицу. 
    Если в источниках появятся новые таблицы, можно их добавить в переменную, и данные из них также будут перекладываться в хранилище.
    Можно будет вынести в отдельный файл, так как будет использоваться и для stg, и для dds"""
    pass

def get_max_created_at():
    """"Получение максимальной даты по столбцу "created_at" из любой таблицы, как источника, так и таргета.
    SELECT MAX(created_at) AS max_created_at FROM table_name;
    Можно будет вынести в отдельный файл"""
    pass

default_args = {"owner": "anzhi", 
                "depends_on_past": False,
                "start_date": dt.datetime(2024, 7, 2, 12, 0, 0, tzinfo=pendulum.timezone("UTC")),
                "retry_delay": timedelta(minutes=5),
                "retries": 1,
                }

@dag(default_args=default_args, catchup=False, tags=["etl"])
def etl():
    
    @task()
    def ectract_transform():
        """
        1. check_schema_in_DWH()
        2. get_max_created_at(target_table) as max_created_at_target
        3. Если max_created_at_target == None, то из source_table получить все данные, что в ней есть. 
        Если max_created_at_target != None, то данные после max_created_at_target.
        4. Добавить столбцы 'igested_at', 'city-name', 'id_source' (доп ключ по {table}_id и src_id)
        5. Сохранить в csv с именем файла 'execution_date_dag_id' в '/tmp/airflow_staging/load_staging_data'
        6. Вернуть полный путь к файлу
         load_dds_data
        """
        pass

    @task()
    def load():
        """
        1. Получить датафрейм из файла, полученного на предыдущем шаге.
        2. df.to_sql()
        """
        pass



etl()


DWH_conn.close()
DWH_cursor.close()
shop_1_conn.close()
shop_1_cursor.close()
shop_2_conn.close()
shop_2_cursor.close()
