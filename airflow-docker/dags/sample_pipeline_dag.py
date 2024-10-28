# dags/pipeline_dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from src.data_loader import load_data
from src.preprocessing import preprocess_data
from src.model import train_model

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'telecom_pipeline',  # Name of the DAG
    default_args=default_args,
    schedule_interval='@daily',  # Run daily
)

# Task 1: Load Data
def load_data_task():
    """
    Airflow task to load the raw dataset using the data_loader module.
    """
    data = load_data()
    return data

# Task 2: Preprocess Data
def preprocess_data_task():
    """
    Airflow task to preprocess the loaded dataset using the preprocessing module.
    """
    from src.data_loader import load_data
    data = load_data()
    preprocessed_data = preprocess_data(data)
    return preprocessed_data

def feature_engineering_task():
    """
    Airflow task to preprocess the loaded dataset using the preprocessing module.
    """
    from src.data_loader import load_data
    data = load_data()
    preprocessed_data = preprocess_data(data)
    best_features_churn = select_best_k_features(preprocessed_data, target_column)
    best_features_device_upgrade = create_device_upgrade_subset(processed_data)
    return best_features_churn, best_features_device_upgrade



# Define tasks in the DAG
load_data_task = PythonOperator(task_id='load_data', python_callable=load_data_task, dag=dag)
preprocess_data_task = PythonOperator(task_id='preprocess_data', python_callable=preprocess_data_task, dag=dag)
feature_engineering_task = PythonOperator(task_id='feature_engineering_task', python_callable=feature_engineering_task, dag=dag)

# Set task dependencies
load_data_task >> preprocess_data_task >> train_model_task
