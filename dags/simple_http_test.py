import datetime as dt
import pandas

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook 

import count_rows

# folder structure
#mkdir -p /tmp/fruit/dl/apples
#mkdir -p /tmp/fruit/dl/figs
#mkdir -p /tmp/fruit/combined

default_args = {
	'owner': 'airflow',
	'start_date': dt.datetime(2018, 4, 20, 00, 00, 00),
	'concurrency': 1,
	'retries': 0
}
date = "{{ ds }}"

def insert_counts(date):
	fruitRecs = pandas.read_csv("/tmp/fruit/combined/%s.txt" % (date),sep=" ",names=['type','date','time'])
	fruitCounts = fruitRecs.groupby('type').count()
	mysql_hook = MySqlHook(mysql_conn_id="mysql_fruit", schema="fruit") # This connection must be set from the Connection view in Airflow UI
	connection = mysql_hook.get_conn() # Gets the connection from PostgreHook
	#connection.insert_rows("insert into fruit_count (apples) values (11)")
	mysql_hook.run("insert into fruit_counts (rec_date,apples,figs) values ('%s',%s,%s)" 
		% (date,fruitCounts.date.apples,fruitCounts.date.figs))

with DAG('simple_http_test',
	default_args=default_args,
	schedule_interval='30 22 * * *') as dag:
	task_hello = BashOperator(task_id='hello', bash_command='echo "hello!"')
	task_dl_apples = BashOperator(task_id='download_apples', 
		bash_command="curl http://maps.tec8.net/apples/%s.txt -o /tmp/fruit/dl/apples/%s.txt" % (date,date))
	task_dl_figs = BashOperator(task_id='download_figs', 
		bash_command="curl http://maps.tec8.net/figs/%s.txt -o /tmp/fruit/dl/figs/%s.txt" % (date,date))
	task_combine = BashOperator(task_id='combine', 
		bash_command="cat /tmp/fruit/dl/*/%s.txt > /tmp/fruit/combined/%s.txt" % (date,date))

	#count_rows = PythonOperator(task_id="count_rows", python_callable=count_rows.main)
	count_rows = PythonOperator(task_id='count_rows', python_callable=insert_counts, op_args=[date])
	task_combine << task_dl_apples 
	task_combine << task_dl_figs 
	task_combine >> count_rows >> task_hello
