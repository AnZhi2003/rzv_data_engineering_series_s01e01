from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Dict
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import logging
import pandas as pd
from pandas import DataFrame
import os

def get_sqlalchemy_engine(conn_id: str) -> Engine:
    hook = PostgresHook(postgres_conn_id=conn_id)
    return create_engine(hook.get_uri())

def checking_schema_tables_columns_in_DB(engine: Engine, schema_name: str, table_dict: Dict[str, Dict[str, str]]) -> bool:
    
    all_tables_created = True

    with engine.connect() as conn:
        try:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))

            for table_name, columns in table_dict.items():
                result = conn.execute(text(f"""
                                        SELECT EXISTS(
                                        SELECT 1
                                        FROM information_schema.tables
                                        WHERE table_schema = :schema_name AND table_name = :table_name);"""), 
                                        {"schema_name": schema_name, "table_name": table_name})
                table_exists = result.scalar()

                if not table_exists:
                    columns_list = ", ".join([f"{col} {typ}" for col, typ in columns.items()])
                    create_table_query = text(f"CREATE TABLE {schema_name}.{table_name} ({columns_list})")
                    try:
                        conn.execute(create_table_query)
                        logging.info(f"Table {schema_name}.{table_name} created successfully")
                    except Exception as e:
                        logging.error(f"Error creating table {schema_name}.{table_name}: {e}")
                        all_tables_created = False
                else: 
                    result = conn.execute(text(f"""
                                            SELECT column_name, data_type
                                            FROM information_schema.columns
                                            WHERE table_schema = :schema_name AND table_name = :table_name;"""), 
                                            {"schema_name": schema_name, "table_name": table_name})
                    columns_exists = {row["column_name"]: row["data_type"] for row in result}

                    for col, typ in columns.items():
                        if col not in columns_exists:
                            alter_table_query = text(f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {col} {typ};")
                            
                            try:
                                conn.execute(alter_table_query)
                                logging.info(f"Column added {col} to table {schema_name}.{table_name}")
                            except Exception as e:
                                logging.error(f"Error adding column {col} to table {schema_name}.{table_name}: {e}")
                                all_tables_created = False
        except Exception as e:
            logging.error(f"Error during schema and table checking: {e}")
            all_tables_created = False
        finally:
            conn.close()

    return all_tables_created


def checking_columns_in_DB(engine: Engine, schema_name: str, table_list: list, columns_dict: Dict[str, str]) -> bool:
    
    all_tables_created = True

    with engine.connect() as conn:
        try:
            for table_name in table_list:
                result = conn.execute(text(f"""
                                        SELECT column_name, data_type
                                        FROM information_schema.columns
                                        WHERE table_schema = :schema_name AND table_name = :table_name;"""), 
                                        {"schema_name": schema_name, "table_name": table_name})
                columns_exists = {row["column_name"]: row["data_type"] for row in result}
                
                for col, typ in columns_dict.items():
                    if col not in columns_exists:
                        alter_table_query = text(f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {col} {typ};")
                    
                        try:
                            conn.execute(alter_table_query)
                            logging.info(f"Column added {col} to table {schema_name}.{table_name}")
                        except Exception as e:
                            logging.error(f"Error adding column {col} to table {schema_name}.{table_name}: {e}")
                            all_tables_created = False

        except Exception as e:
            logging.error(f"Error during column checking: {e}")
            all_tables_created = False
        finally:
            conn.close()

    return all_tables_created

def get_table_from_DB(engine: Engine, schema_name: str, table_name: str, filters: dict | None) -> pd.DataFrame:
    with engine.connect() as conn:
        try:
            query = f"SELECT * FROM {schema_name}.{table_name} WHERE 1=1"

            filter_conditions = []
            filter_params = {}

            if filters:
                if "date_of_update" in filters:
                    filter_conditions.append("created_at >= :date_of_update")
                    filter_params["date_of_update"] = filters["date_of_update"]
                if "max_created_at" in filters:
                    filter_conditions.append("created_at > :max_created_at")
                    filter_params["max_created_at"] = filters["max_created_at"]
                if "source_name" in filters:
                    filter_conditions.append("source_name = :source_name")
                    filter_params["source_name"] = filters["source_name"]
                if "date_range" in filters:
                    filter_conditions.append("created_at BETWEEN :start_date AND :end_date")
                    filter_params["start_date"] = filters["date_range"][0]
                    filter_params["end_date"] = filters["date_range"][1]
                    
            if filter_conditions:
                query += " AND " + " AND ".join(filter_conditions)
                logging.info(f"Query is {query} with params={filter_params}")

            
            df = pd.read_sql_query(text(query), conn, params=filter_params)
            logging.info(f"Fetched {len(df)} records from {schema_name}.{table_name}")
        except Exception as e:
            logging.error(f"Error fetching data from {schema_name}.{table_name}: {e}")
            df = pd.DataFrame()
    
    return df


def get_max_created_at(engine: Engine, schema_name: str, table_name: str, source_name: str) -> datetime | None:
    with engine.connect() as conn:
        try:
            result = conn.execute(text(f"""
                                       SELECT MAX(created_at) as max_created_at
                                       FROM {schema_name}.{table_name}
                                       WHERE source_name = :source_name;"""),
                                       {"source_name": source_name})
            
            max_created_at = result.scalar()
            logging.info(f"Max_created_at for {schema_name}.{table_name} for source={source_name}: {max_created_at}")
        except Exception as e:
            logging.error(f"Error fetching max_created_at from {schema_name}.{table_name} for source={source_name}: {e}")
            
    return max_created_at

def get_date_of_update(max_created_at: datetime | None, update_months_ago: int) -> datetime:
    """Функция для установления глубины обновления в прошлом"""
    if max_created_at is None:
        current_dttm = datetime.now()
        start_of_current_dttm = current_dttm.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        date_of_update = start_of_current_dttm - relativedelta(months=update_months_ago)
    else:
        current_dttm = max_created_at
        start_of_current_dttm = current_dttm.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        date_of_update = start_of_current_dttm - relativedelta(months=update_months_ago)
    
    return date_of_update


def generate_file_path(dag_run_id: str, dag_run_date: date, source_name: str, source_city_name: str, table_name: str, action: str, folder_name: str):
    file_name = f"{action}_{dag_run_id}_{dag_run_date}_{source_city_name}_{source_name}_{table_name}.csv"
    return os.path.join(f"/tmp/airflow_{folder_name}", file_name)

def save_df_to_csv(df: DataFrame, file_path: str):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(file_path, sep=',', index=False, header=True, encoding="utf-8", date_format='%Y-%m-%d %H:%M:%S', float_format='%.2f')

def add_metadata_to_df(df, dag_run_dttm, source_name, source_city_name, action):
    if not df.empty:
        df["eff_from_dttm"] = dag_run_dttm
        df["eff_to_dttm"] = datetime(2999, 12, 31, 23, 59, 59, 999999)
        df["do_with_record"] = action
        df["ingested_at"] = dag_run_dttm
        df["source_id"] = df[df.columns[0]].apply(lambda x: f"{source_name}_{x}")
        df["source_name"] = source_name
        df["city_name"] = source_city_name
    return df