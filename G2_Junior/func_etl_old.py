from airflow.hooks.postgres_hook import PostgresHook
from typing import Dict
from datetime import datetime
import logging
import pandas as pd

def checking_schema_tables_columns_in_DB(hook: PostgresHook, schema_name: str, table_dict: Dict[str, Dict[str, str]]) -> bool:
    
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Creating schema:
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    all_tables_created = True

    # Checking and crating tables and columns:
    for table_name, columns in table_dict.items():
        # Create tables if not exists
        cursor.execute(f"""SELECT EXISTS (
                       SELECT 1
                       FROM information_schema.tables
                       WHERE table_schema = '{schema_name}'
                       AND table_name = '{table_name}');
                       """)
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            columns_list = ", ".join([f"{col} {typ}" for col, typ in columns.items()])
            create_table_query = f"""CREATE TABLE {schema_name}.{table_name} ({columns_list});"""
            try:
                cursor.execute(create_table_query)
                conn.commit()
                logging.info(f"Table {schema_name}.{table_name} created successfully.")
            except Exception as e:
                logging.error(f"Error creating table {schema_name}.{table_name}: {e}.")
                all_tables_created = False

        # Adding lacking columns    
        cursor.execute(f"""
                       SELECT column_name, data_type
                       FROM information_schema.columns
                       WHERE table_schema = '{schema_name}'
                       AND table_name = '{table_name}';
                       """)
        columns_exists = {row[0]: row[1] for row in cursor.fetchall()}

        for col, typ in columns.items():
            if col not in columns_exists:
                alter_table_query = f"ALTER TABLE {schema_name}.{table_name} ADD COLUMN {col} {typ};"

                try:
                    cursor.execute(alter_table_query)
                    conn.commit()
                    logging.info(f"Column added {col} to table {schema_name}.{table_name}.")
                except Exception as e:
                    logging.error(f"Error adding column {col} to table {schema_name}.{table_name}: {e}.")
                    all_tables_created = False
    
    cursor.close()
    conn.close()

    return all_tables_created
        
def get_max_created_at(hook: PostgresHook, schema_name: str, table_name: str) -> datetime | None:
    """"Получение максимальной даты по столбцу "created_at" из любой таблицы, как источника, так и таргета."""
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
                       SELECT MAX(created_at) AS max_created_at 
                       FROM {schema_name}.{table_name};
                       """)
        max_created_at = cursor.fetchone()[0]
    except Exception as e:
        logging.error(f"Error fetching max created_at from {schema_name}.{table_name}: {e}.")
        max_created_at = None
    finally:
        cursor.close()
        conn.close()

    return max_created_at

def get_table_from_DB(hook: PostgresHook, schema_name: str, table_name: str, max_created_at: datetime | None) -> pd.DataFrame:
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        if max_created_at is None:
            query= f"""
                    SELECT * 
                    FROM {schema_name}.{table_name};
                    """
        else:
            query =f"""
            SELECT * 
            FROM {schema_name}.{table_name}
            WHERE created_at > '{max_created_at}';
            """
        df = pd.read_sql_query(query, conn)

    except Exception as e:
        logging.error(f"Error fetching data from {schema_name}.{table_name}: {e}.")
        df = pd.DataFrame()
    finally:
        cursor.close()
        conn.close()
    
    return df