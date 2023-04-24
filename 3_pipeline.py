####################################################################################
# Тестовое задание на позицию Data Engineer. Выполнил Павел Коротков, tlg: @pvl_ko #
####################################################################################


# импортируем необходимые библиотки
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import date, datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import clickhouse_connect

# устанавливаем параметры по умолчанию
defaultargs = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'email': ['airflow@sky.pro', 'DE@sky.pro'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retrydelay': timedelta(minutes=10)
}

# для того, чтобы не хранить логин, пароль, hostname в коде, записываем их в txt-файлы и добавляем эти файлы в gitignore

# для подключения к PostgreSQL
with open('pg_user.txt', 'r', encoding='utf-8') as fp:
    pg_user = fp.read().rstrip()
with open('pg_user_password.txt', 'r', encoding='utf-8') as fp:
    pg_password = fp.read().rstrip()
with open('pg_hostname.txt', 'r', encoding='utf-8') as fp:
    pg_hostname = fp.read().rstrip()
# для подключения к ClickHouse
with open('ch_user.txt', 'r', encoding='utf-8') as fp:
    ch_user = fp.read().rstrip()
with open('ch_user_password.txt', 'r', encoding='utf-8') as fp:
    ch_password = fp.read().rstrip()
with open('ch_hostname.txt', 'r', encoding='utf-8') as fp:
    ch_hostname = fp.read().rstrip()

# определяем функцию, которая будет выполнять ETL
def pipeline():
    # получаем вчершнюю дату для sql-запроса и правильного именования CSV-файла, в который будем сохранять датафрейм
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    # создаем подключение к БД PostgreSQL
    conn = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_hostname}/db_name')
    # т.к. SELECT выполняем из специально подготовленного для этих целей VIEW, то не указываем столбцы поименно, чтобы не загромождать код
    sql_query = f'SELECT * FROM Lessons_View WHERE lesson_start_at = "{yesterday}";'
    # сохраняем полученную выгрузку в датафрейм
    df = pd.read_sql_query(sql_query, conn)
    # Рвем подключение к БД PostgreSQL
    conn.dispose()
    




    # сохраняем датафрейм в CSV-файл
    df.to_csv(f'{yesterday}.csv', index=False)

    # создаем подключение к БД ClickHouse
    client = clickhouse_connect.get_client(host=ch_hostname, port=8443, 
                                    username=ch_user, password=ch_password, 
                                    secure=True)
    
    # загружаем датафрейм в ClickHouse
    client.insert_df('dbname.lessons', df)


# устанавливаем параметры DAG'a, время запуска в формате крона - ежедневно в 3 утра
with DAG(
    dag_id='incremental_daily_load', 
    defaultargs=defaultargs, 
    catchup=False, 
    schedule_interval='0 3 * * *'
    ) as dag:

    # вызываем Python-функцию для ETL    
    etl_pipeline = PythonOperator(
        task_id='etl_pipeline',
        python_callable=pipeline
    )

    # уведомление о завершении ETL-процесса
    email_notification = EmailOperator(
        task_id='send_email',
        to='airflow@sky.pro',
        subject='ClickHouse Daily Incremental Load Done',
        html_content='<p>Ежедневная загрузка в ClickHouse выполнена.</p>',
    )
    
    # устанавливаем порядок выполнения task'ов
    etl_pipeline >> email_notification