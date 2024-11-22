from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import sys
import os
import importlib.util
from google.cloud import storage

# File paths
RAW_DATA_FILE = 'gs://data-source-telecom-customers/data/raw_data/train/train.csv'
PROCESSED_DATA_FILE = 'gs://data-source-telecom-customers/data/processed_data/preprocessed_data.csv'
BEST_FEATURES_FILE = 'gs://data-source-telecom-customers/data/clean_data/train_clean/best_features_for_churn.csv'
SELECTED_FEATURES_JSON = 'gs://data-source-telecom-customers/data/clean_data/selected_features.json'
BEST_FEATURES_TEST_FILE = 'gs://data-source-telecom-customers/data/clean_data/test_clean/best_features_test.csv'

TRAINING_BUCKET = 'training_bucket'
NEW_DATA_BUCKET = 'new_data'

MODULES_FOLDER = 'python_modules'

# Helper function to upload files to GCS
def custom_upload_to_gcs(gs_file_path):
    try:
        client = storage.Client()
        bucket_name, file_path = gs_file_path.replace("gs://", "").split("/", 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
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
        bucket = client.bucket(TRAINING_BUCKET)
        module_path = f"{MODULES_FOLDER}/{module_name}.py"
        local_module_path = f"/tmp/{module_name}.py"
        blob = bucket.blob(module_path)
        blob.download_to_filename(local_module_path)
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

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'telecom_dag_with_sensors',
    default_args=default_args,
    description='DAG for Telecom project with GCS sensors',
    schedule_interval=None,  # Triggered by sensors
    catchup=False,
)

# Sensor for new training data
training_data_sensor = GCSObjectExistenceSensor(
    task_id='training_data_sensor',
    bucket=TRAINING_BUCKET,
    object='train.csv',
    timeout=600,
    poke_interval=30,
    mode='poke',
    dag=dag
)

# Sensor for new prediction data
new_data_sensor = GCSObjectExistenceSensor(
    task_id='new_data_sensor',
    bucket=NEW_DATA_BUCKET,
    object='*.csv',  # Wildcard to capture any new CSV file
    timeout=600,
    poke_interval=30,
    mode='poke',
    dag=dag
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
        print("Data preprocessed and saved locally.")
        custom_upload_to_gcs(PROCESSED_DATA_FILE)
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
        selected_features = best_features_for_churn.columns.drop(target_column).tolist()
        
        # Save selected features as JSON
        with open('selected_features.json', 'w') as f:
            json.dump(selected_features, f)
        custom_upload_to_gcs(SELECTED_FEATURES_JSON)

        best_features_for_churn.to_csv(BEST_FEATURES_FILE, index=False)
        print("Feature engineering completed and files uploaded to GCS.")
        return True
    except Exception as e:
        print(f"Error in feature_engineering_task: {e}")
        raise

# Task 4: Preprocess New Data for Prediction
def preprocess_new_data_task():
    try:
        client = storage.Client()
        bucket = client.bucket(NEW_DATA_BUCKET)
        blobs = bucket.list_blobs()
        latest_file = max(blobs, key=lambda blob: blob.updated)  # Select the latest file
        test_data = pd.read_csv(f"gs://{NEW_DATA_BUCKET}/{latest_file.name}")
        
        preprocessed_test_data = preprocessing.preprocess_data(test_data)
        
        # Load selected features from GCS
        bucket = client.bucket(TRAINING_BUCKET)
        blob = bucket.blob('data/clean_data/selected_features.json')
        selected_features = json.loads(blob.download_as_text())
        
        # Select only the saved features
        feature_data = preprocessed_test_data[selected_features]
        feature_data.to_csv(BEST_FEATURES_TEST_FILE, index=False)
        print("New data preprocessed and saved with selected features.")
        custom_upload_to_gcs(BEST_FEATURES_TEST_FILE)
        return True
    except Exception as e:
        print(f"Error in preprocess_new_data_task: {e}")
        raise

# Define Python tasks
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

preprocess_new_data_operator = PythonOperator(
    task_id='preprocess_new_data',
    python_callable=preprocess_new_data_task,
    dag=dag
)

# Set task dependencies
training_data_sensor >> load_data_operator >> preprocess_data_operator >> feature_engineering_operator
new_data_sensor >> preprocess_new_data_operator
