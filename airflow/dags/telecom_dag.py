import sys
import os
import pandas as pd

current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_path = os.path.join(current_dir, 'src')
sys.path.append(src_path)

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from config import RAW_DATA_PATH, PROCESSED_DATA_PATH
from config import CHURN_FEATURES_PATH, DEVICE_UPGRADE_FEATURES_PATH
from data_loader import load_data
from preprocessing import preprocess_data
from feature_engineering import find_optimal_k, select_best_k_features, create_device_upgrade_subset

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'telecom_dag',  # Name of the DAG
    default_args=default_args,
    description='This is a DAG for the Telecom project',
    schedule_interval='@daily',  # Run daily
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
)

# Task 1: Load Data
def load_data_task():
    """
    Airflow task to load the raw dataset using the data_loader module.
    """
    data = load_data()
    print(data.head())
    if data is not None:
        print("Data loaded to airflow local")
    else:
        print("Data loading failed")
    return True

# Task 2: Preprocess Data
def preprocess_data_task():
    """
    Airflow task to preprocess the loaded dataset using the preprocessing module.
    """
    data = pd.read_csv(RAW_DATA_PATH)
    preprocessed_data = preprocess_data(data)
    preprocessed_data.to_csv(PROCESSED_DATA_PATH, index=False)
    print(preprocessed_data.head())
    print("Data preprocessed and saved for further tasks")
    return True

def feature_engineering_task():
    """
    Airflow task to preprocess the loaded dataset using the preprocessing module.
    """
    processed_data = pd.read_csv(PROCESSED_DATA_PATH)

    # Defining target column for feature selection
    target_column = 'Churn'

    # Performing feature engineering
    find_optimal_k(processed_data, target_column, k_range=range(25, 31))
    best_features_churn = select_best_k_features(processed_data, target_column)
    best_features_device_upgrade = create_device_upgrade_subset(processed_data)

    # Saving the engineered feature sets to separate files
    best_features_churn.to_csv(CHURN_FEATURES_PATH, index=False)
    best_features_device_upgrade.to_csv(DEVICE_UPGRADE_FEATURES_PATH, index=False)
    print("Feature engineering completed and files saved")
    return True

# Define tasks in the DAG
load_data_task = PythonOperator(task_id='load_data', python_callable=load_data_task, dag=dag)
preprocess_data_task = PythonOperator(task_id='preprocess_data', python_callable=preprocess_data_task, dag=dag)
feature_engineering_task = PythonOperator(task_id='feature_engineering_task', python_callable=feature_engineering_task, dag=dag)

# Set task dependencies
load_data_task >> preprocess_data_task >> feature_engineering_task