from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta, timezone
# from plugins import slack

import requests
import logging
import psycopg2


def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def extract(**context):
    lat = 37.470159
    lon = 126.897394
    key = context["params"]["key"]
    link = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={key}&units=metric"
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)
    f = requests.get(link)
    return (f.json())


def transform(**context):
    
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    return text


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    KST = timezone(timedelta(hours=9))
    
    cur = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    sql = "BEGIN; TRUNCATE TABLE {schema}.{table};".format(schema=schema, table=table)
    for line in lines["daily"]:
        (day, temp, min, max) = (datetime.fromtimestamp(line["dt"]).strftime('%Y-%m-%d'), line["temp"]["day"], line["temp"]["min"], line["temp"]["max"])
        sql += f"""INSERT INTO {schema}.{table} VALUES ('{day}', '{temp}', '{min}', '{max}', '{datetime.now(KST)}'); END;"""
    logging.info(sql)
    cur.execute(sql)


dag_second_assignment = DAG(
    dag_id = 'weatherDataToRedshift',
    start_date = datetime(2023,4,6), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 2 * * 3',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
)


extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        'key':  Variable.get("open_weather_api_key")
    },
    dag = dag_second_assignment)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = { 
    },  
    dag = dag_second_assignment)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'jewoos15',   ## 자신의 스키마로 변경
        'table': 'weather_forecast'
    },
    dag = dag_second_assignment)

extract >> transform >> load
