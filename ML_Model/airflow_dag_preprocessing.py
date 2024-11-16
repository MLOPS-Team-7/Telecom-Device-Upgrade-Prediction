from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sys
import os
import importlib.util
from google.cloud import storage
 
RAW_DATA_FILE = 'gs://data-source-telecom-customers/data/raw_data/train/train.csv'
PROCESSED_DATA_FILE = 'gs://data-source-telecom-customers/data/processed_data/preprocessed_data.csv'
BEST_FEATURES_FILE = 'gs://data-source-telecom-customers/data/clean_data/train_clean/best_features_for_churn.csv'
TEST_DATA_FILE = 'gs://data-source-telecom-customers/data/raw_data/test/test.csv'
BEST_FEATURES_TEST_FILE = 'gs://data-source-telecom-customers/data/clean_data/test_clean/best_features_test.csv'
TEST_PROCESSED_FILE = 'gs://data-source-telecom-customers/data/processed_data/test_processed.csv'
 
GCS_BUCKET = 'data-source-telecom-customers'
MODULES_FOLDER = 'python_modules'
 
# Custom function to upload files to GCS (using full gs:// file path)
def custom_upload_to_gcs(gs_file_path):
    try:
        # Initialize the GCS client
        client = storage.Client()
 
        # Extract bucket name and file path from gs:// path
        bucket_name, file_path = gs_file_path.replace("gs://", "").split("/", 1)
 
        # Get the GCS bucket
        bucket = client.bucket(bucket_name)
 
        # Define the blob (file) name in GCS
        blob = bucket.blob(file_path)
 
        # Upload the file to GCS
        blob.upload_from_filename(file_path)
        print(f"File {file_path} uploaded successfully to GCS bucket {bucket_name}")
        return True
    except Exception as e:
        print(f"Error uploading file {file_path} to GCS: {e}")
        raise
 
# Helper function to load a module dynamically from GCS
def load_module_from_gcs(module_name):
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        module_path = f"{MODULES_FOLDER}/{module_name}.py"
       
        # Temporary location to load the module
        local_module_path = f"/tmp/{module_name}.py"
       
        # Download the module from GCS
        blob = bucket.blob(module_path)
        blob.download_to_filename(local_module_path)
       
        # Load the module dynamically
        spec = importlib.util.spec_from_file_location(module_name, local_module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
       
        print(f"Module {module_name} loaded successfully from GCS")
        return module
    except Exception as e:
        print(f"Error loading module {module_name} from GCS: {e}")
        raise
 
# Preload necessary modules
preprocessing = load_module_from_gcs('preprocessing')
feature_engineering = load_module_from_gcs('feature_engineering')
 
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
 
# Initialize the DAG
dag = DAG(
    'telecom_dag',
    default_args=default_args,
    description='DAG for Telecom project with modules directly loaded from GCS',
    schedule_interval='@daily',
    catchup=False,
)
 
# Task 1: Load Data
def load_data_task():
    try:
        print(f"Raw data is available in GCS at {RAW_DATA_FILE}")
        return True
    except Exception as e:
        print(f"Error in load_data_task: {e}")
        raise
 
# Task 2: Preprocess Data
def preprocess_data_task():
    try:
        data = pd.read_csv(RAW_DATA_FILE)
        preprocessed_data = preprocessing.preprocess_data(data)
        preprocessed_data.to_csv(PROCESSED_DATA_FILE, index=False)
        #custom_upload_to_gcs(PROCESSED_DATA_FILE)
        print("Data preprocessed and uploaded to GCS")
        return True
    except Exception as e:
        print(f"Error in preprocess_data_task: {e}")
        raise
 
# Task 3: Feature Engineering
def feature_engineering_task():
    try:
        processed_data = pd.read_csv(PROCESSED_DATA_FILE)
        target_column = 'Churn'
        feature_engineering.find_optimal_k(processed_data, target_column, k_range=range(25, 31))
        best_features_for_churn = feature_engineering.select_best_k_features(processed_data, target_column)
        best_features_for_churn.to_csv(BEST_FEATURES_FILE, index=False)
        #custom_upload_to_gcs(BEST_FEATURES_FILE)
        print("Feature engineering completed and files uploaded to GCS")
        return True
    except Exception as e:
        print(f"Error in feature_engineering_task: {e}")
        raise
 
# Task 4: Test Data Processing and Feature Selection
def test_data_processing_and_feature_selection_task():
    try:
        test_data = pd.read_csv(TEST_DATA_FILE)
        test_processed = preprocessing.preprocess_data(test_data)
        test_processed.to_csv(TEST_PROCESSED_FILE, index=False)
        #custom_upload_to_gcs(TEST_PROCESSED)
       
        best_features_test = feature_engineering.transform_and_save_test_features(
            BEST_FEATURES_FILE,
            TEST_PROCESSED_FILE,            
            'Churn'
        )
        best_features_test.to_csv(BEST_FEATURES_TEST_FILE, index=False)
        #custom_upload_to_gcs(BEST_FEATURES_TEST_FILE)
        print("Test data processed and uploaded to GCS")
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
 