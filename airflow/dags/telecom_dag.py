import sys
import os

##Get the project root directory dynamically
#current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#src_path = os.path.join(current_dir, 'src')
# Add src directory to sys.path if not already included
#if src_path not in sys.path:
#    sys.path.append(src_path)
sys.path.append('/Users/svs/Desktop/Projects/Telecom-Device-Upgrade-Prediction/src')

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import
from datetime import datetime, timedelta
from config import RAW_DATA_PATH, PROCESSED_DATA_PATH, TEST_DATA_PATH, PROCESSED_TEST_PATH
from config import CHURN_FEATURES_PATH, CHURN_TEST_PATH
from data_loader import load_data
from preprocessing import preprocess_data
from feature_engineering import find_optimal_k, select_best_k_features, transform_and_save_test_features
from upload_to_gcs import upload_to_gcs

# Debugging paths
print("DAG File Location:", __file__)
print("Current Working Directory:", os.getcwd())
print("Initial sys.path:", sys.path)


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),  # Fixed start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Add retry delay
}

# Initialize the DAG
dag = DAG(
    'telecom_dag',
    default_args=default_args,
    description='This is a DAG for the Telecom project',
    schedule_interval='@daily',
    catchup=False,
)

# Task 1: Load Data
def load_data_task():
    try:
        data = load_data()
        print(data.head())
        if data is not None:
            print("Data loaded to Airflow local")
        else:
            raise ValueError("Data loading failed")
        return True
    except Exception as e:
        print(f"Error in load_data_task: {e}")
        raise

# Task 2: Preprocess Data
def preprocess_data_task():
    try:
        data = pd.read_csv(RAW_DATA_PATH)
        preprocessed_data = preprocess_data(data)
        preprocessed_data.to_csv(PROCESSED_DATA_PATH, index=False)
        print(preprocessed_data.head())
        print("Data preprocessed and saved for further tasks")
        return True
    except Exception as e:
        print(f"Error in preprocess_data_task: {e}")
        raise

# Task 3: Feature Engineering
def feature_engineering_task():
    try:
        processed_data = pd.read_csv(PROCESSED_DATA_PATH)
        target_column = 'Churn'
        find_optimal_k(processed_data, target_column, k_range=range(25, 31))
        best_features_for_churn = select_best_k_features(processed_data, target_column)
        best_features_for_churn.to_csv(CHURN_FEATURES_PATH, index=False)
        upload_to_gcs(CHURN_FEATURES_PATH, 'data-source-telecom-customers', 'data/clean_data/train_clean/best_features_for_churn.csv')
        print("Feature engineering completed and files saved for training")
        return True
    except Exception as e:
        print(f"Error in feature_engineering_task: {e}")
        raise

# Task 4: Test Data Processing and Feature Selection
def test_data_processing_and_feature_selection_task():
    try:
        test_data = pd.read_csv(TEST_DATA_PATH)
        test_processed = preprocess_data(test_data)
        test_processed.to_csv(PROCESSED_TEST_PATH, index=False)
        target_column = 'Churn'
        best_features_test = transform_and_save_test_features(CHURN_FEATURES_PATH, PROCESSED_TEST_PATH, target_column)
        best_features_test.to_csv(CHURN_TEST_PATH, index=False)
        upload_to_gcs(CHURN_TEST_PATH, 'data-source-telecom-customers', 'data/clean_data/test_clean/best_features_test.csv')
        print('Test data processed and features selected')
        return True
    except Exception as e:
        print(f"Error in test_data_processing_and_feature_selection_task: {e}")
        raise

# Define tasks in the DAG
load_data_operator = PythonOperator(
    task_id='load_data',
    python_callable=load_data_task,
    dag=dag
)

preprocess_data_operator = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data_task,
    dag=dag
)

feature_engineering_operator = PythonOperator(
    task_id='feature_engineering',
    python_callable=feature_engineering_task,
    dag=dag
)

test_data_operator = PythonOperator(
    task_id='test_data_processing',
    python_callable=test_data_processing_and_feature_selection_task,
    dag=dag
)

# Set task dependencies
load_data_operator >> preprocess_data_operator >> feature_engineering_operator >> test_data_operator
