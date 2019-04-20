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

with DAG('simple_http_test',
	default_args=default_args,
	schedule_interval='30 22 * * *') as dag:
	date = "{{ ds }}"
	task_hello = BashOperator(task_id='hello', bash_command='echo "hello!"')
	task_dl_apples = BashOperator(task_id='download_apples', 
		bash_command="curl http://maps.tec8.net/apples/%s.txt -o /tmp/apples/%s.txt" % (date,date))
	task_dl_figs = BashOperator(task_id='download_figs', 
		bash_command="curl http://maps.tec8.net/figs/%s.txt -o /tmp/figs/%s.txt" % (date,date))
	task_hello << task_dl_apples 
	task_hello << task_dl_figs 
