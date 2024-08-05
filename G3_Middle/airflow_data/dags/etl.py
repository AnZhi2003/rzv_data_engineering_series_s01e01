import json
import pendulum
import logging
import os
import pandas as pd
import hashlib

from datetime import timedelta, datetime
from sqlalchemy.engine import Engine
from sqlalchemy import text
from typing import Dict, List

from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import get_current_context
from airflow.models import Variable

from func_etl import (get_date_of_update, 
                      get_max_created_at, 
                      get_sqlalchemy_engine, 
                      get_table_from_DB, 
                      checking_columns_in_DB, 
                      checking_schema_tables_columns_in_DB, 
                      generate_file_path, 
                      save_df_to_csv,
                      add_metadata_to_df)

"""Variables"""
var_airflow_etl_str = Variable.get("var_airflow_etl")
var_airflow_etl_dict = json.loads(var_airflow_etl_str)

var_tables_list = var_airflow_etl_dict["tables_list"]
var_tables_columns_from_source = var_airflow_etl_dict["tables_columns"]
var_tech_columns = var_airflow_etl_dict["tech_columns"]
var_scd2_columns = var_airflow_etl_dict["scd2_columns"]
var_temp_columns = var_airflow_etl_dict["temporary_columns"]

var_source_conn = var_airflow_etl_dict["source_conn"]

"""Connection to DWH"""
DWH_engine = get_sqlalchemy_engine("pg-dwh")

default_args = {"owner": "anzhi",
                "depends_on_past": False,
                "start_date": datetime(2024, 7, 20, 12, 0, 0, tzinfo=pendulum.timezone("UTC")),
                "retries": 1, 
                "retry_delay": timedelta(minutes=5)}

@dag(default_args=default_args, catchup=False, tags=["etl"], schedule_interval='*/5 * * * *', max_active_runs=1)
def etl():
    
    @task()
    def extract_i() -> List:
        context = get_current_context()
        dag_run_id = context["dag_run"].run_id
        dag_run_dttm = context["dag_run"].execution_date
        dag_run_date = context["dag_run"].execution_date.date()

        schema_name_source = "public"
        schema_name_DWH = "dds"

        new_data_paths_i = []

        # Данные из источников сохраняем в csv
        for source_key, source_param in var_source_conn.items():
            source_name = source_key
            source_city_name = source_param.get("city_name")
            source_conn_id = source_param.get("conn_id")
            source_engine = get_sqlalchemy_engine(source_conn_id)
            source_update_months_ago = source_param.get("update_months_ago", 3)


            for table_name in var_tables_list:
                max_created_at = get_max_created_at(DWH_engine, schema_name_DWH, table_name, source_name)
                filters = {"max_created_at": max_created_at} if max_created_at is not None else {}
                extracted_data_i = get_table_from_DB(source_engine, schema_name_source, table_name, filters=filters)
                if not extracted_data_i.empty:
                    file_path_i = generate_file_path(dag_run_id, dag_run_date, source_name, source_city_name, table_name, "source-insert", "stg")
                    save_df_to_csv(extracted_data_i, file_path_i)
                    new_data_paths_i.append(file_path_i)
                    logging.info(f"Data extracted and saved for insert: {file_path_i}")
                else:
                    logging.warning(f"No data found for insert for table {table_name} from source {source_name}")
        return new_data_paths_i

    @task()
    def extract_u() -> Dict:
        context = get_current_context()
        dag_run_id = context["dag_run"].run_id
        dag_run_dttm = context["dag_run"].execution_date
        dag_run_date = context["dag_run"].execution_date.date()

        schema_name_source = "public"
        schema_name_DWH = "dds"

        new_data_paths_u = []
        target_data_paths_u = []

        # Данные из источников сохраняем в csv
        for source_key, source_param in var_source_conn.items():
            source_name = source_key
            source_city_name = source_param.get("city_name")
            source_conn_id = source_param.get("conn_id")
            source_engine = get_sqlalchemy_engine(source_conn_id)
            source_update_months_ago = source_param.get("update_months_ago", 3)


            for table_name in var_tables_list:
                max_created_at = get_max_created_at(DWH_engine, schema_name_DWH, table_name, source_name)
                date_of_update = get_date_of_update(max_created_at, source_update_months_ago)
                logging.info(f"Date of update for dds.{table_name} of {source_name} is {date_of_update}")

                if max_created_at is None:
                    filters = {"date_of_update": date_of_update}
                else:
                    filters = {"date_range": (date_of_update, max_created_at)}

                extracted_data_u = get_table_from_DB(source_engine, schema_name_source, table_name, filters=filters)
                if not extracted_data_u.empty:
                    file_path_extracted_data_u = generate_file_path(dag_run_id, dag_run_date, source_name, source_city_name, table_name, "source-update", "stg")
                    save_df_to_csv(extracted_data_u, file_path_extracted_data_u)
                    new_data_paths_u.append(file_path_extracted_data_u)
                else:
                    logging.warning(f"No data found for update for table {table_name} from source {source_name}")

        # Данные из таргета для сравнения сохраняем в csv
        for table_name in var_tables_list:        
            for_update_data_u = get_table_from_DB(DWH_engine, schema_name_DWH, table_name, filters={"date_range": (date_of_update, max_created_at)})
            if not for_update_data_u.empty:
                file_path_for_update_data_u = generate_file_path(dag_run_id, dag_run_date, source_name, "any_city_name", table_name, "target-update", "stg")
                save_df_to_csv(for_update_data_u, file_path_for_update_data_u)
                target_data_paths_u.append(file_path_for_update_data_u)
            else:
                logging.warning(f"No data found for update for table {table_name} from source {source_name}")
        
        return {"new_data_paths_u": new_data_paths_u, "target_data_paths_u": target_data_paths_u}

    @task()
    def transform_i(new_data_paths_i: List) -> Dict:

        context = get_current_context()
        dag_run_id = context["dag_run"].run_id
        dag_run_dttm = context["dag_run"].execution_date
        dag_run_date = context["dag_run"].execution_date.date()

        transformed_new_data_i =[]

        for file_path in new_data_paths_i:
            table_name = file_path.split("/")[-1].split("_")[-1].split(".")[0]
            source_name = file_path.split("/")[-1].split("_")[-2]
            source_city_name = file_path.split("/")[-1].split("_")[-3]

            df = pd.read_csv(file_path)
            df = add_metadata_to_df(df, dag_run_dttm, source_name, source_city_name, "insert_new_rows")
            
            file_name = file_path.split("/")[-1]
            file_path = os.path.join("/tmp/airflow_stg", file_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            save_df_to_csv(df, file_path)

            transformed_new_data_i.append(file_path)

        return {"transformed_new_data_i": transformed_new_data_i}


    @task()
    def transform_u(extract_u_result: Dict) -> Dict:

        new_data_paths_u = extract_u_result["new_data_paths_u"]
        target_data_paths_u = extract_u_result["target_data_paths_u"]

        context = get_current_context()
        dag_run_id = context["dag_run"].run_id
        dag_run_dttm = context["dag_run"].execution_date
        dag_run_date = context["dag_run"].execution_date.date()

        df_stg = pd.DataFrame()
        transformed_data_u = []

        for table_name in var_tables_list:
            for source_name in var_source_conn.keys():

                new_file_path = next((fp for fp in new_data_paths_u if table_name in fp and source_name in fp), None)
                target_file_path = next((fp for fp in target_data_paths_u  if table_name in fp and source_name in fp), None)

                if new_file_path:
                    new_df = pd.read_csv(new_file_path)
                    source_city_name_new_df = new_file_path.split("/")[-1].split("_")[-3]

                if target_file_path:
                    target_df = pd.read_csv(target_file_path)

                if new_file_path and target_file_path:
                    id_column = new_df.columns[0]

                    #новые строки, которых ранее не было в DWH dds:
                    new_id_rows = new_df[~new_df[id_column].isin(target_df[id_column])]
                    new_id_rows = add_metadata_to_df(new_id_rows, dag_run_dttm, source_name, source_city_name_new_df, "insert_new_rows")
                    
                    #Строки, которые были ранее загружены в DWH dds, но их уже нет на источнике:
                    del_id_rows = target_df[~target_df[id_column].isin(new_df[id_column])]
                    if not del_id_rows.empty:
                        del_id_rows["eff_to_dttm"] = dag_run_dttm - timedelta(seconds=1)
                        del_id_rows["do_with_record"] = "delete"

                    # Удаляем строки из new_df и target_df, которые уже были обработаны как new_id_rows и del_id_rows.
                    # Останутся строки, где одинаковые id есть как в источнике, так и таргете
                    new_df = new_df[new_df[id_column].isin(target_df[id_column])]
                    target_df = target_df[target_df[id_column].isin(new_df[id_column])]

                    if table_name in var_tables_columns_from_source:
                        columns = list(var_tables_columns_from_source[table_name].keys())
                    else:
                        logging.error(f"Columns for table {table_name} not found in var_tables_columns_from_source")
                        continue

                    new_df["hash"] = new_df[columns].apply(lambda row: hashlib.md5(row.astype(str).str.cat(sep="|").encode()).hexdigest(), axis=1)
                    target_df["hash"] = target_df[columns].apply(lambda row: hashlib.md5(row.astype(str).str.cat(sep="|").encode()).hexdigest(), axis=1)
                    
                    #отбираем строки у которых изменилась бизнесс-часть:
                    merged_df = pd.merge(new_df, target_df, on=[id_column, "hash"], how="outer", indicator=True)
                    changed_df = merged_df[merged_df["_merge"] != "both"]

                    new_entries = changed_df[changed_df["_merge"] == "left_only"].drop(columns=["_merge"])
                    if not new_entries.empty:
                        new_entries = add_metadata_to_df(new_entries, dag_run_dttm, source_name, source_city_name_new_df, "add_changed_row")
                    
                    old_entries = changed_df[changed_df["_merge"] == "right_only"].drop(columns=["_merge"])
                    old_entries["eff_to_dttm"] = dag_run_dttm - timedelta(seconds=1)
                    old_entries["do_with_record"] = "close_changed_row"

                    df_stg = pd.concat([df_stg, new_id_rows, del_id_rows, new_entries, old_entries], axis=0)

                elif new_file_path and not target_file_path:
                    new_df = add_metadata_to_df(new_df, dag_run_dttm, source_name, source_city_name_new_df, "insert_new_rows")
                    df_stg = pd.concat([df_stg, new_df], axis=0)
                    
                elif not new_file_path and target_file_path:
                    target_df = pd.read_csv(target_file_path)
                    target_df["eff_to_dttm"] = dag_run_dttm - timedelta(seconds=1)
                    target_df["do_with_record"] = "delete"
                    df_stg = pd.concat([df_stg, target_df], axis=0)

        if not df_stg.empty:
            transformed_data_path_u = generate_file_path(dag_run_id, dag_run_date, source_name, "any_city_name", table_name, "to_DB", "dds")
            save_df_to_csv(df_stg, transformed_data_path_u)
            transformed_data_u.append(transformed_data_path_u)
                        

        return {"transformed_data_u": transformed_data_u}
    
    
    @task()
    def checking_tables_in_DWH_stg() -> bool:
        created_schema_tables_columns_in_DWH_stg = checking_schema_tables_columns_in_DB(DWH_engine, "stg", var_tables_columns_from_source)
        created_tech_columns_in_DWH_stg = checking_columns_in_DB(DWH_engine, "stg", var_tables_list, var_tech_columns)
        created_scd2_columns_in_DWH_stg = checking_columns_in_DB(DWH_engine, "stg", var_tables_list, var_scd2_columns)
        created_temp_columns_in_DWH_stg = checking_columns_in_DB(DWH_engine, "stg", var_tables_list, var_temp_columns)
        return (created_schema_tables_columns_in_DWH_stg and 
                created_tech_columns_in_DWH_stg and 
                created_scd2_columns_in_DWH_stg and
                created_temp_columns_in_DWH_stg)
    
    
    @task()
    def load_to_stg(transformed_data_u: Dict, transformed_new_data_i: Dict) -> bool:

        transformed_data_u = transformed_data_u.get("transformed_data_u", [])
        transformed_new_data_i = transformed_new_data_i.get("transformed_new_data_i", [])
        transformed_data = transformed_data_u + transformed_new_data_i

        schema_name = "stg"
        data_loaded = True

        if not transformed_data:
            logging.infp("No data to load.")

        with DWH_engine.connect() as conn:
            for file_path in transformed_data:
                if not file_path:
                    logging.warning("Empty file path encountered in transformed_data.")
                    continue 

                table_name = file_path.split("/")[-1].split("_")[-1].split(".")[0]
                source_name = file_path.split("/")[-1].split("_")[-2]

                try:
                    conn.execute(text(f"TRUNCATE TABLE {schema_name}.{table_name};"))
                    logging.info(f"Truncated {schema_name}.{table_name} befor loading")

                    df = pd.read_csv(file_path)

                    try:
                        df.to_sql(table_name, DWH_engine, schema=schema_name, if_exists="append", index=False)
                        logging.info(f"Data loaded successfully into {schema_name}.{table_name} for source={source_name}. DF length is {len(df)}")
                    except Exception as e:
                        data_loaded = False
                        logging.error(f"Error loading data into {schema_name}.{table_name}: {e}")
                        raise e
                
                except Exception as e:
                    logging.error(f"Error truncating table {schema_name}.{table_name}: {e}")
                    data_loaded = False

        return data_loaded
    
    @task()
    def checking_tables_in_DWH_dds():
        created_schema_tables_columns_in_DWH_dds = checking_schema_tables_columns_in_DB(DWH_engine, "dds", var_tables_columns_from_source)
        created_tech_columns_in_DWH_dds = checking_columns_in_DB(DWH_engine, "dds", var_tables_list, var_tech_columns)
        created_scd2_columns_in_DWH_dds =checking_columns_in_DB(DWH_engine, "dds", var_tables_list, var_scd2_columns)
        return created_schema_tables_columns_in_DWH_dds and created_tech_columns_in_DWH_dds and created_scd2_columns_in_DWH_dds

    @task()
    def load_to_dds() -> bool:
        schema_stg = "stg"
        schema_dds = "dds"

        success_flag = True

        with DWH_engine.connect() as conn:
            for table_name in var_tables_list:
                stg_table = f"{schema_stg}.{table_name}"
                dds_table = f"{schema_dds}.{table_name}"
                
                try:
                    record_types = ["insert_new_rows", "add_changed_row", "close_changed_row", "delete"]

                    stg_query = f"SELECT * from {stg_table}"
                    stg_df = pd.read_sql(stg_query, conn)

                    id_column = stg_df.columns[0]

                    for record_type in record_types:
                        if record_type in stg_df["do_with_record"].values:
                            record_df = stg_df[stg_df["do_with_record"] == record_type]

                            if record_type in ["insert_new_rows", "add_changed_row"]:
                                try:
                                    # Добавляем новые строки в dds таблицу
                                    record_df.drop(columns=["do_with_record"], inplace=True)
                                    record_df.to_sql(table_name, conn, schema=schema_dds, if_exists="append", index=False)
                                    logging.info(f"Inserted new rows into {dds_table} from {stg_table}")
                                except Exception as e:
                                    success_flag = False
                                    logging.error(f"Error inserting new rows into {dds_table} from {stg_table}: {e}")
                            
                            elif record_type in ["close_changed_row", "delete"]:
                                try:
                                    # Обновляем eff_to_dttm в dds таблице
                                    for _, row in record_df.iterrows():
                                        update_query = text(f"""
                                                            UPDATE {dds_table} 
                                                            SET eff_to_dttm = :eff_to_dttm
                                                            WHERE id = :id 
                                                            AND eff_to_dttm =:max_date
                                                        """)
                                        conn.execute(update_query, {
                                            "eff_to_dttm": row["eff_to_dttm"],
                                            "id": row[id_column],
                                            "max_date": datetime(2999, 12, 31, 23, 59, 59, 999999)
                                        })
                                    logging.info(f"Updated rows in {dds_table} from {stg_table}")
                                except Exception as e:
                                    success_flag = False
                                    logging.error(f"Error updating rows in {dds_table} from {stg_table}: {e}")
                except Exception as e:
                    success_flag = False
                    logging.error(f"Error processing table {table_name}: {e}")

        return success_flag
    
    # Определение задач и их аргументов
    checking_tables_in_DWH_stg_result = checking_tables_in_DWH_stg()
    checking_tables_in_DWH_dds_result = checking_tables_in_DWH_dds()

    tables_in_DWH_checked = DummyOperator(task_id="tables_in_DWH_checked")

    extract_i_result = extract_i()
    extract_u_result = extract_u()

    data_extracted = DummyOperator(task_id="data_extracted")

    transform_i_result = transform_i(extract_i_result)
    transform_u_result = transform_u(extract_u_result)
    
    load_to_stg_result = load_to_stg(transform_u_result, transform_i_result)
    load_to_dds_result = load_to_dds()

    # Установка зависимостей
    [checking_tables_in_DWH_stg_result, checking_tables_in_DWH_dds_result] >> tables_in_DWH_checked >> [extract_i_result, extract_u_result]
    [extract_i_result, extract_u_result] >> data_extracted >> [transform_i_result, transform_u_result]
    [transform_i_result, transform_u_result] >> load_to_stg_result
    load_to_stg_result >> load_to_dds_result

etl()
