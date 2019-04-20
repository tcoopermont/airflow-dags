import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
	'owner': 'airflow',
	'start_date': dt.datetime(2018, 4, 20, 00, 00, 00),
	'concurrency': 1,
	'retries': 0
}

with DAG('simple_dag_backfill',
	default_args=default_args,
	schedule_interval='22 30 * * *') as dag:
	date = {{execution_date}}
	task_hello = BashOperator(task_id='hello', bash_command='echo "hello!"')
	task_dl_apples = BashOperator(task_id='download_apples', bash_command="wget http://fruit.tec8.net/apples/%s" % (date))
	task_hello >> task_dl_apples 
