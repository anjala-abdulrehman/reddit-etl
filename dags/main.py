from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

from airflow.operators.empty import EmptyOperator

import os
import pandas as pd


def load_to_mongodb():
    folder_path = '/path/to/your/folder'
    mongo_conn_id = 'reddit_mongo_conn'
    database_name = 'reddit_data'
    collection_name = 'your_collection'

    mongo_hook = MongoHook(mongo_conn_id)

    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            data = pd.read_csv(file_path)
            records = data.to_dict('records')
            mongo_hook.insert_many(mongo_conn_id, database_name, collection_name, records)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='copy_to_mongodb',
    default_args=default_args,
    description='DAG to copy data to MongoDB and Init Project',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')

    load_to_mongo = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=load_to_mongodb,
    )

    end = EmptyOperator(task_id='end')


start >> load_to_mongo >> end
