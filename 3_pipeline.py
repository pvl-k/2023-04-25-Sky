####################################################################################
# Тестовое задание на позицию Data Engineer. Выполнил Павел Коротков, tlg: @pvl_ko #
####################################################################################


# импортируем необходимые библиотки
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.base import BaseHook
from datetime import date, datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import clickhouse_connect
import pendulum


# установливаем местное время
local_tz = pendulum.timezone("Europe/Moscow")

# устанавливаем параметры DAG'а
default_args = {
    'owner': 'DE',
    'start_date': datetime(2023, 1, 1, tzinfo=local_tz),
    'email': ['airflow@sky.pro', 'DE@sky.pro'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retrydelay': timedelta(minutes=10)
}

# функция выгрузки данных из созданного VIEW в базе Postgres
def postgres_select(connection, yesterday):
    # т.к. SELECT выполняем из специально подготовленного для этих целей VIEW, то не указываем столбцы поименно, чтобы не загромождать код ДАГа
    sql_query = f'SELECT * FROM Lessons_View WHERE lesson_start_at = "{yesterday}";'
    # сохраняем полученную выгрузку в датафрейм
    df = pd.read_sql_query(sql_query, connection)
    # Рвем подключение к БД PostgreSQL
    connection.dispose()
    return df

# функция переопределения типов данных по столбцам и записи csv-файла на диск (при отсутствии необходимости в записи, можно закомментировать)
def datatype_change(df, yesterday):
    # переопределяем типы данных по столбцам
    df.astype({'lesson_id': 'uint64', 'lesson_title': 'str', 'lesson_description': 'str', 'lesson_start_at': 'datetime64',
               'lesson_end_at': 'datetime64', 'lesson_homework_url': 'str', 'lesson_teacher': 'uint32', 'lesson_join_url': 'str',
               'lesson_rec_url': 'str', 'module_id': 'uint32', 'module_title': 'str', 'module_description': 'str', 'module_created_at': 'datetime64',
               'module_updated_at': 'datetime64', 'module_order_in_stream': 'uint8', 'stream_id': 'uint32', 'stream_name': 'str', 'stream_description': 'str',
               'stream_start_at': 'datetime64', 'stream_end_at': 'datetime64', 'stream_created_at': 'datetime64', 'stream_updated_at': 'datetime64',
               'course_id': 'uint16', 'course_title': 'str', 'course_description': 'str', 'course_created_at': 'datetime64', 'course_updated_at': 'datetime64'})
    # сохраняем датафрейм в CSV-файл, если такая необходимость есть
    df.to_csv(f'{yesterday}.csv', index=False)
    return df

def click_insert(client, df):
    # загружаем датафрейм в таблицу "lessons" ClickHouse
    client.insert_df('dbname.lessons', df)


# определяем общую функцию, которая будет выполнять ETL
def pipeline():

    # получаем вчершнюю дату для sql-запроса и правильного именования CSV-файла, в который будем сохранять датафрейм
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    # создаем подключение к БД PostgreSQL
    postgres_conn = BaseHook.get_connection('postgres')
    connection = create_engine(f'postgresql://{postgres_conn.login}:{postgres_conn.password}@{postgres_conn.host}/{postgres_conn.schema}')

    # запускаем SELECT из posgres
    df = postgres_select(connection, yesterday)

    # переопределяем типы данных в датафрейме и записываем csv-файл на диск
    datatype_change(df, yesterday)
    
    # создаем подключение к БД ClickHouse
    clickhouse_conn = BaseHook.get_connection('clickhouse')
    client = clickhouse_connect.get_client(host=clickhouse_conn.host, port=clickhouse_conn.port, 
                                    username=clickhouse_conn.login, password=clickhouse_conn.password, 
                                    secure=True)
    
    # записываеv датафрейм в ClickHouse
    click_insert(client, df)

# устанавливаем параметры DAG'a, время запуска в формате крона - ежедневно в 3 утра
with DAG(
    dag_id='daily_etl', 
    default_args=default_args, 
    catchup=False, 
    schedule_interval='0 3 * * *'
    ) as dag:

    # вызываем функцию для ETL    
    etl_pipeline = PythonOperator(
        task_id='etl_pipeline',
        python_callable=pipeline
    )

    # уведомление о завершении ETL-процесса
    email_notification = EmailOperator(
        task_id='send_email',
        to='airflow@sky.pro',
        subject='ClickHouse Daily Incremental Load Done',
        html_content='<p>Ежедневная загрузка в ClickHouse выполнена.</p>'
    )
    
    # устанавливаем порядок выполнения task'ов
    etl_pipeline >> email_notification