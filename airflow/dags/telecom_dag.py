import sys
import os
import pandas as pd

current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_path = os.path.join(current_dir, 'src')
sys.path.append(src_path)

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from config import RAW_DATA_PATH, PROCESSED_DATA_PATH, TEST_DATA_PATH, PROCESSED_TEST_PATH
from config import CHURN_FEATURES_PATH, CHURN_TEST_PATH
from data_loader import load_data
from preprocessing import preprocess_data
from feature_engineering import find_optimal_k, select_best_k_features, transform_and_save_test_features
from google.cloud import storage

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
    # Saving the engineered feature sets to csv
    best_features_churn.to_csv(CHURN_FEATURES_PATH, index=False)
    # Uploading churn_features to gcs
    upload_to_gcs(CHURN_FEATURES_PATH, 'data-source-telecom-customers', 'data/clean_data/train_clean/best_features_churn.csv')
    print("Feature engineering completed and files saved for training")
    return True

def test_data_processing_and_feature_selection_task():
    test_data = pd.read_csv(TEST_DATA_PATH)
    test_processed = preprocess_data(test_data)
    test_processed.to_csv(PROCESSED_TEST_PATH, index=False)
    # selecting the same features for test data as the best features obtained for train data 
    best_features_test = transform_and_save_test_features(CHURN_FEATURES_PATH, PROCESSED_TEST_PATH, target_column)
    # converting to csv
    best_features_test.to_csv(CHURN_TEST_PATH, index=False)
    # uploading to gcs
    upload_to_gcs(CHURN_TEST_PATH, 'data-source-telecom-customers', 'data/clean_data/test_clean/best_features_test.csv')
    print('Test data processed and features selected')

# Define tasks in the DAG
load_data_task = PythonOperator(task_id='load_data', python_callable=load_data_task, dag=dag)
preprocess_data_task = PythonOperator(task_id='preprocess_data', python_callable=preprocess_data_task, dag=dag)
feature_engineering_task = PythonOperator(task_id='feature_engineering_task', python_callable=feature_engineering_task, dag=dag)
test_data_processing_and_feature_selection_task = PythonOperator(task_id='test_data_processing_and_feature_selection_task', python_callable=test_data_processing_and_feature_selection_task, dag=dag)
# Set task dependencies
load_data_task >> preprocess_data_task >> feature_engineering_task >> test_data_processing_and_feature_selection_task