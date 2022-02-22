import os
import datetime as dt

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
        dag_id='titanic_dag',
        start_date=dt.datetime(2021, 3, 1),
        schedule_interval='@once'
) as dag:
   check_if_file_exists = SimpleHttpOperator(
      method='HEAD',
      task_id='check_file_existence',
      http_conn_id='web_stanford_http_id',
      endpoint='/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv',
   )

   @task
   def download_titanic_dataset():
      url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
      response = requests.get(url, stream=True)
      response.raise_for_status()
      filepath = os.path.join(os.path.expanduser('~'), 'titanic.csv')
      with open(filepath, 'w', encoding='utf-8') as f:
         for chunk in response.iter_lines():
            f.write('{}\n'.format(chunk.decode('utf-8')))
      return filepath

   @task()
   def load_titanic_hive():
         task_id='load_data_to_hive',
         hql='''LOAD DATA INPATH '{{ task_instance.xcom_pull(task_ids='download_titanic_dataset', key='return_value') }}'
         INTO TABLE titanic;''',

   @task()
   def show_avg_fare():
      task_id='show_avg_fare',
      hql='''SELECT Pclass, avg(Fare) FROM titanic GROUP BY Pclass;''',

   @task()
   def send_result_telegram():
      task_id='send_success_message_telegram',
      telegram_conn_id='telegram_conn_id',
      chat_id='1234567',
      text='''Pipeline {{ execution_date.int_timestamp }} is done''',

   download_titanic_dataset >> load_titanic_hive >> show_avg_fare >> send_result_telegram