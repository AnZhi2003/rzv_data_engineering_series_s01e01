"""ПЛАН

@dag()
def etl_dds():

@task()
def extract_i(extracted_data_params): !!! аргументы из дага def etl_stg()
    1. Проходимся 2 циклами с одним вложенным по extracted_data_params, получаем max_created_at и date_from_source по каждому source_name и table_name
    2. extracted_data = sql-запрос: select * stg.table_name where source_name = source_name and  created_at > max_created_at -> сохраняем в csv в папку '/tmp/airflow_dds', путь сохраняем в список new_data_i
    3. return new_data_i


    
    6. Фильтруем полученную таблицу extracted_data: created_at <= max_created_at -> сохраняем в csv в папку '/tmp/airflow_extract', путь сохраняем в список data_u


@task()
def transform_i():
1. Проходимся циклом по путям в списке new_data_i 
2. Распаковываем данные из путей в pd.Dataframe 
3. Добавляем столбцы: ingested_at, eff_from_dttm, eff_to_dttm, city_name, source_name, source_id
4. Cохраняем в csv в папку '/tmp/airflow_staging', путь сохраняем в список transformed_new_data_i

@task()
def transform_u():
1. 


@task()
def load_i():
1. Проходимся циклом по путям в списке transformed_new_data_i 
2. Распаковываем данные из путей в pd.Dataframe 
3. df.to_sql() в stg-layer
"""