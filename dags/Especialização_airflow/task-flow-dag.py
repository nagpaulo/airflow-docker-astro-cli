"""
"""

# TODO import libraries
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime

@dag(
    dag_id="task-flow-dag",
    start_date=datetime(2024,10,29),
    catchup=False,
    is_paused_upon_creation=False,
    tags=['especialização'],
)
def init():
    # TODO tasks 1
    @task(task_id="extract_data")
    def extract():
        return {"data","extract"}
    
    # TODO tasks 2
    @task(task_id="transform_data")
    def transform(data: dict):
        return {"data","transform"}
    
    transform(extract())
    
dag = init()