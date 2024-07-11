from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Dict
from datetime import datetime
import logging
import pandas as pd


def get_sqlalchemy_engine(conn_id) -> Engine:
    hook = PostgresHook(postgres_conn_id=conn_id)
    return create_engine(hook.get_uri())

def checking_schema_tables_columns_in_DB(engine: Engine, schema_name: str, table_dict: Dict[str, Dict[str, str]]) -> bool:
    # Creating schema:
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))

        all_tables_created = True

        # Checking and crating tables and columns:
        for table_name, columns in table_dict.items():
            # Create tables if not exists
            result = conn.execute(text(f"""SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = :schema_name
                        AND table_name = :table_name);
                        """), {"schema_name": schema_name, "table_name": table_name})
            table_exists = result.scalar()

            if not table_exists:
                columns_list = ", ".join([f"{col} {typ}" for col, typ in columns.items()])
                create_table_query = text(f"""CREATE TABLE {schema_name}.{table_name} ({columns_list});""")
                try:
                    conn.execute(create_table_query)
                    logging.info(f"Table {schema_name}.{table_name} created successfully.")
                except Exception as e:
                    logging.error(f"Error creating table {schema_name}.{table_name}: {e}.")
                    all_tables_created = False

            # Adding lacking columns    
            result = conn.execute(text(f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = :schema_name
                        AND table_name = :table_name;
                        """), {"schema_name": schema_name, "table_name": table_name})
            columns_exists = {row["column_name"]: row["data_type"] for row in result}

            for col, typ in columns.items():
                if col not in columns_exists:
                    alter_table_query = text(f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {col} {typ};")

                    try:
                        conn.execute(alter_table_query)
                        logging.info(f"Column added {col} to table {schema_name}.{table_name}.")
                    except Exception as e:
                        logging.error(f"Error adding column {col} to table {schema_name}.{table_name}: {e}.")
                        all_tables_created = False
        
    return all_tables_created
            
def get_max_created_at(engine: Engine, schema_name: str, table_name: str, source_name: str) -> datetime | None:
    """"Получение максимальной даты по столбцу "created_at" из любой таблицы, как источника, так и таргета."""
    with engine.connect() as conn:
        try:
            result = conn.execute(text(f"""
                        SELECT MAX(created_at) AS max_created_at 
                        FROM {schema_name}.{table_name}
                        WHERE source_name = :source_name;
                        """), {"source_name": source_name})
            max_created_at = result.scalar()
            logging.info(f"Max created_at for {schema_name}.{table_name} for source={source_name}: {max_created_at}")
        except Exception as e:
            logging.error(f"Error fetching max created_at from {schema_name}.{table_name} for source={source_name}: {e}.")
            max_created_at = None

    return max_created_at

def get_table_from_DB(engine: Engine, schema_name: str, table_name: str, max_created_at: datetime | None, source_name: str) -> pd.DataFrame:
    with engine.connect() as conn:
        try:
            if max_created_at is None:
                query= text(f"""
                        SELECT * 
                        FROM {schema_name}.{table_name};
                        """)
            else:
                query = text(f"""
                SELECT * 
                FROM {schema_name}.{table_name}
                WHERE created_at > :max_created_at;
                """)
            df = pd.read_sql_query(query, conn, params={"max_created_at": max_created_at})
            logging.info(f"Fetched {len(df)} records from {schema_name}.{table_name} for source={source_name} since {max_created_at}")
        except Exception as e:
            logging.error(f"Error fetching data from {schema_name}.{table_name} for source={source_name}: {e}.")
            df = pd.DataFrame()
        
    return df