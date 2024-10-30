"""_summary_
"""
# TODO 1: import libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# TODO 2: declare DAG structure
with DAG(
    dag_id="traditional-dag",
    start_date=datetime(2024,10,29),
    catchup=False,
    tags=['especialização'],
) as dag:    
    # TODO 3: declare tasks
    # TODO function 1: extract data
    def extract():
        return {"data": "extract"}
    
    # TODO function 2: extract data
    def transform(ti):
        data = ti.xcom_pull(task_ids="extract")
        return  {"data": "transform"}
    
    # TODO declare tasks [1]
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract
    )

    # TODO declare tasks [2]
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform
    )
    
    # TODO 4: set task deoendencies
    extract_data >> transform_data