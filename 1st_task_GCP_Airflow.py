import requests, json
import airflow
import csv
import json
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'Top-10_hottest_cities',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20))
t1 = BashOperator(
    task_id='echo',
    bash_command='echo test',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31-1)

key = "cba863c7770e4f9f92c93340220305"
base_url = "http://api.weatherapi.com/v1/current.json?key="
cities = {
 'Tokyo' : ''
,'Delhi' : ''
,'Shanghai' : ''
,'Dhaka' : ''
,'Sao Paulo' : ''
,'Mexico City' : ''
,'Cairo' : ''
,'Beijing' : ''
,'Mumbai' : ''
,'Osaka' : ''
,'Chongqing' : ''
,'Karachi' : ''
,'Istanbul' : ''
,'Kinshasa' : ''
,'Lagos' : ''
,'Buenos Aires' : ''
,'Kolkata' : ''
,'Manila' : ''
,'Tianjin' : ''
,'Guangzhou' : ''
,'Rio De Janeiro' : ''
,'Lahore' : ''
,'Bangalore' : ''
,'Shenzhen' : ''
,'Moscow' : ''
,'Chennai' : ''
,'Bogota' : ''
,'Paris' : ''
,'Jakarta' : ''
,'Lima' : ''
,'Bangkok' : ''
,'Hyderabad' : ''
,'Seoul' : ''
,'Nagoya' : ''
,'London' : ''
,'Chengdu' : ''
,'Nanjing' : ''
,'Tehran' : ''
,'Ho Chi Minh City' : ''
,'Luanda' : ''
,'Wuhan' : ''
,'Xi An Shaanxi' : ''
,'Ahmedabad' : ''
,'Kuala Lumpur' : ''
,'New York City' : ''
,'Hangzhou' : ''
,'Surat' : ''
,'Suzhou' : ''
,'Hong Kong' : ''
,'Riyadh' : ''}
for city in cities:
	url = base_url + "&q=" + city + "&aqi=no"
	response = requests.get(url)
	if response.status_code == 200:
		data = response.json()
		main = data('main')
		temp = main('temp_c')
		cities[city] = temp
result = cities
with open("/home/airflow/gcs/data/cities.csv", "w") as fp:
    json.dump(result,fp) 
