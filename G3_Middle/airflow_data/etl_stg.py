"""ПЛАН

0. Функция для установления глубины обновления в прошлом в разрезе источников: get_date_from_source, datetime  (на вход var_source_conn). Указать кол-во месяцев в variables.
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    # Текущая дата
    current_date = datetime.now()

    # Установка первого дня текущего месяца с нулевым временем
    first_day_of_current_month = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # Получение даты три месяца назад
    three_months_ago = first_day_of_current_month - relativedelta(months=3)

    print(f"Current date: {current_date}")
    print(f"First day of current month: {first_day_of_current_month}")
    print(f"Date three months ago: {three_months_ago}")


@dag()
def etl_stg():

@task()
def extract():
    1. Проходимся циклом по источникам (на вход var_source_conn)
    2. Проходимся внутренним циклом по таблицам в источниках (на вход var_DWH_tables)
    3. Получем max_created_at из target (в разрезе источников и таблиц)
    4. extract_dttm = datetime.now()
    5. Получаем дату, с которой забираем данные из источника  -- date_from_source с помощью функции get_date_from_source
    6. Получаем данные из источника > date_from_source = extracted_data
    7. Сохраняем extracted_data в csv в папку '/tmp/airflow_extract', путь сохраняем в список extracted_data
    8. Сохраняем в словарь extracted_data_params = {"source_name": {"table_name": {"max_created_at": max_created_at, "date_from_source": date_from_source}}}
    8. return extract_dttm, extracted_data_params, extracted_data


@task()
def transform(extract_dttm, extracted_data_params, extracted_data):
    1. Проходимся циклом по путям в списке extracted_data 
    2. Распаковываем данные из путей в pd.Dataframe 
    3. Добавляем столбцы: ingested_at = extract_dttm; source_id, source_name, city_name -- из наименований файлов
    4. Сохраняем в csv в папку '/tmp/airflow_staging', путь сохраняем в список transformed_new_data. В наименовании файла должно быть название таблицы
    5. return transformed_new_data

@task()
def load(transformed_new_data):
    1. Проходимся циклом по путям в списке transformed_new_data
    2. Получаем название таблицы из названия файла
    3. Truncate stg.table_name
    4. Распаковываем данные из путей в pd.Dataframe 
    5. df.to_sql() в stg-layer
"""