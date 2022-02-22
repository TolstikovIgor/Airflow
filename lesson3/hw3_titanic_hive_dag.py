import datetime as dt
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.task_group import TaskGroup

args = {
   'owner': 'airflow',
   'start_date': dt.datetime(2020, 12, 23),
   'retries': 1,
   'retry_delay': dt.timedelta(minutes=1),
   'depends_on_past': False,
}

with DAG(
        dag_id='titanic_hive',
        schedule_interval=None,
        default_args=args,
) as dag:
   create_titanic_dataset = BashOperator(
      task_id='download_titanic_dataset',
      bash_command='''TITANIC_FILE="titanic-{{ execution_date.int_timestamp }}.csv" && \
      wget -q https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv -O $TITANIC_FILE && \
      hdfs dfs -mkdir -p /datasets/ && \
      hdfs dfs -put $TITANIC_FILE /datasets/ && \
      rm $TITANIC_FILE && \
      echo "/datasets/$TITANIC_FILE" ''',
   )
   with TaskGroup("prepare_table") as prepare_table:
      drop_hive_table = HiveOperator(
         task_id='drop_hive_table',
         hql='DROP TABLE IF EXISTS titanic;',
      )

      create_hive_table = HiveOperator(
         task_id='create_hive_table',
         hql='''CREATE TABLE IF NOT EXISTS titanic ( Survived INT, Pclass INT,
          Name STRING, Sex STRING, Age INT, Sibsp INT, Parch INT, Fare DOUBLE)
          ROW FORMAT DELIMITED
          FIELDS TERMINATED BY ','
          STORED AS TEXTFILE
          TBLPROPERTIES('skip.header.line.count'='1');''',
      )

      drop_hive_table >> create_hive_table


   load_titanic_hive = HiveOperator(
      task_id='load_data_to_hive',
      hql='''LOAD DATA INPATH '{{ task_instance.xcom_pull(task_ids='download_titanic_dataset', key='return_value') }}'
      INTO TABLE titanic;''',
   )

   show_avg_fare = HiveOperator(
      task_id='show_avg_fare',
      hql='''SELECT Pclass, avg(Fare) FROM titanic GROUP BY Pclass;''',
   )

   send_result_telegram = TelegramOperator(
      task_id='send_success_message_telegram',
      telegram_conn_id='telegram_conn_id',
      chat_id='1234567',
      text='''Pipeline {{ execution_date.int_timestamp }} is done''',
   )

   create_titanic_dataset >> prepare_table >> load_titanic_hive
   load_titanic_hive >> show_avg_fare
   show_avg_fare >> send_result_telegram

