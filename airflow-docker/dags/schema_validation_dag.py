from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from src.generate_schema import generate_schema
from src.validate_data import validate_data

default_args = {
    'owner': 'user',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('schema_validation_dag', default_args=default_args, schedule_interval='@daily') as dag:
    
    generate_schema_task = PythonOperator(
        task_id='generate_schema',
        python_callable=generate_schema,
    )
    
    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    generate_schema_task >> validate_data_task