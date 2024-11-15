from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from datetime import datetime, timedelta
import pandas as pd
import io
 
# GCS Bucket and file paths
RAW_BUCKET_NAME = 'data-source-telecom-customers'
RAW_DATA_FILE = '/data/raw_data/train/train.csv'
PROCESSED_BUCKET_NAME = 'data-source-telecom-customers'
PROCESSED_DATA_FILE = '/data/preprocessed_data/preprocessed_data.csv'
BEST_FEATURES_FILE = 'clean_data/best_features_for_churn.csv'
TEST_DATA_FILE = 'raw_data/test_data.csv'
BEST_FEATURES_TEST_FILE = 'clean_data/best_features_test.csv'
CLEAN_DATA_PATH = 'data-source-telecom-customers/data/clean_data/train_clean/'
CLEAN_DATA_TEST = 'data-source-telecom-customers/data/clean_data/test_clean/'
 
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
 
# Initialize the DAG
with DAG(
    dag_id='telecom_dag_gcs_direct',
    default_args=default_args,
    description='Telecom DAG to process data directly from GCS and store cleaned data back in GCS',
    schedule_interval=None,
    start_date=datetime(2024, 11, 1),
    catchup=False,
) as dag:
    # Helper function to read CSV from GCS
    def read_csv_from_gcs(bucket_name, file_name):
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        data = blob.download_as_string()
        return pd.read_csv(io.BytesIO(data))
 
    # Helper function to write CSV to GCS
    def write_csv_to_gcs(bucket_name, file_name, dataframe):
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(dataframe.to_csv(index=False), content_type='text/csv')
        print(f"File written to gs://{bucket_name}/{file_name}")
 
    # Task 1: Preprocess Data
    def preprocess_data_task():
        raw_data = read_csv_from_gcs(RAW_BUCKET_NAME, RAW_DATA_FILE)
        # Example preprocessing logic: fill missing values
        raw_data.fillna(0, inplace=True)
        write_csv_to_gcs(PROCESSED_BUCKET_NAME, PROCESSED_DATA_FILE, raw_data)
        print("Data preprocessed and saved to GCS")
        return True
 
    preprocess_data = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data_task,
    )
 
    # Task 2: Feature Engineering
    def feature_engineering_task():
        processed_data = read_csv_from_gcs(PROCESSED_BUCKET_NAME, PROCESSED_DATA_FILE)
        target_column = 'Churn'
        # Example feature engineering: select numeric columns
        numeric_columns = processed_data.select_dtypes(include='number').columns
        best_features = processed_data[numeric_columns]
        write_csv_to_gcs(CLEAN_DATA_PATH, BEST_FEATURES_FILE, best_features)
        print("Feature engineering completed and saved to GCS")
        return True
 
    feature_engineering = PythonOperator(
        task_id='feature_engineering',
        python_callable=feature_engineering_task,
    )
 
    # Task 3: Test Data Processing
    def test_data_processing_task():
        test_data = read_csv_from_gcs(RAW_BUCKET_NAME, TEST_DATA_FILE)
        # Example preprocessing for test data
        test_data.fillna(0, inplace=True)
        # Example feature selection for test data
        best_features = read_csv_from_gcs(PROCESSED_BUCKET_NAME, BEST_FEATURES_FILE)
        selected_features_test = test_data[best_features.columns]
        write_csv_to_gcs(CLEAN_DATA_TEST, BEST_FEATURES_TEST_FILE, selected_features_test)
        print("Test data processed and saved to GCS")
        return True
 
    test_data_processing = PythonOperator(
        task_id='test_data_processing',
        python_callable=test_data_processing_task,
    )
 
    # Set task dependencies
    preprocess_data >> feature_engineering >> test_data_processing