# Airflow importing
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

# Selenium importing
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.select import Select
from selenium.webdriver.edge.options import Options
from bs4 import BeautifulSoup
import time

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

driver = webdriver.Remote('http://127.0.0.1:4444/wd/hub', DesiredCapabilities.FIREFOX)



def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def init_setting(**context):
    driver.get(url='https://www.airport.kr/ap/ko/dep/depPasSchList.do')
    driver.maximize_window()

    # 시간 선택
    from_time = driver.find_element(By.XPATH, '//*[@id="FROM_TIME"]/option[1]').click()  # 00:00 선택
    to_time = driver.find_element(By.XPATH, '//*[@id="TO_TIME"]/option[24]').click()  # 23:59 선택

    # Target searching
    driver.find_element(By.XPATH, '//*[@id="searchBtn"]').click()

    # Page loading
    time.sleep(5)

    # 항공 데이터 contact
    flight_elements = driver.find_elements(By.CLASS_NAME, 'flight-info-basic-link ')

    result = set(driver, from_time, to_time, flight_elements)

    return result


def extract_and_load(**context):
    # Organize pulled date
    init_result = context["task_instance"].xcom_pull(key="return_value", task_ids="init_setting")
    driver = init_result[0]
    from_time = init_result[0]
    to_time = init_result[0]
    flight_elements = init_result[0]

    # Redshift cursor
    cur = get_Redshift_connection()

    # Extract and Load
    column_number = 0
    sql = ""
    for flight_element in flight_elements:
        destination = flight_element.find_element(By.XPATH, f'//*[@id="column2-{column_number}"]').text
        departure_time = flight_element.find_element(By.XPATH, f'//*[@id="column1-{column_number}"]').text + ':00'
        flight_number = flight_element.find_element(By.XPATH, f'//*[@id="column3-{column_number}"]').text
        airline = flight_element.find_element(By.XPATH, f'//*[@id="column4-{column_number}"]').text
        terminal = flight_element.find_element(By.XPATH, f'//*[@id="column5-{column_number}"]').text
        gate = flight_element.find_element(By.XPATH, f'//*[@id="column7-{column_number}"]').text
        # print(flight_number)
        sql += f"""BEGIN;INSERT INTO airportdb.airportdata(destination, departure_time, flight_number, airline, terminal, gate) 
                    VALUES ('{destination}', '{departure_time}',
                            '{flight_number}', '{airline}', '{terminal}', '{gate}');END;"""

    cur.execute(sql)

    driver.quit()






##############
# DAG Setting
##############

dag = DAG(
    dag_id = 'flight_data',
    start_date = datetime(2023,4,22), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 11 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 2,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
)






####################
# DAG Task Setting
####################

init_setting = PythonOperator(
    task_id = 'init_setting',
    python_callable = init_setting,
    params = {
    },
    dag = dag)

extract_and_load = PythonOperator(
    task_id = 'extract_and_load',
    python_callable = extract_and_load,
    params = {
        'schema' : 'jewoos15',
        'table' : 'airportdb'
    },  
    dag = dag)



init_setting >> extract_and_load()
