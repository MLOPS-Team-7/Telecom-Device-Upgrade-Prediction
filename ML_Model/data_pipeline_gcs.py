import os
import sys
import json
import io
import importlib.util
from datetime import datetime, timedelta


import pandas as pd
import numpy as np
from scipy.stats import zscore
from google.cloud import storage
import logging

from pyspark.sql import SparkSession
from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.analyzers import Size, Mean
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook


 
 
# Define bucket names
GCS_BUCKET = 'data-source-telecom-customers'

# Define path prefixes
TRAINING_DATA_PREFIX = 'data/raw_data/train/training_data_'
NEW_DATA_PREFIX = 'data/raw_data/new_data/new_data_'
PROCESSED_DATA_PREFIX = 'data/processed_data/processed_train/'
CLEAN_DATA_PREFIX = 'data/clean_data/train_clean/'
NEW_DATA_PROCESSED_PREFIX = 'data/processed_data/processed_new_data/'
NEW_DATA_CLEAN_BUCKET = 'holdout_batches'
SCHEMA_TRAIN_PREFIX = 'schema_files/train/'
SCHEMA_NEW_PREFIX = 'schema_files/new/'
MODULES_FOLDER = 'python_modules'

# Set up global logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Function to get a logger for each task dynamically
#def get_task_logger(task_name):
    #return logging.getLogger(task_name)

# Helper function to load a module dynamically from GCS
def load_module_from_gcs(module_name):
    log = logging.getLogger("load_module_from_gcs")  # Get a logger for this task
    try:
        log.info(f"Starting to load module '{module_name}' from GCS...")
        
        # Initialize the GCS client
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        module_path = f"{MODULES_FOLDER}/{module_name}.py"
        
        # Temporary location to load the module
        local_module_path = f"/tmp/{module_name}.py"
        
        # Download the module from GCS
        blob = bucket.blob(module_path)
        blob.download_to_filename(local_module_path)
        log.info(f"Downloaded module '{module_name}' from GCS to {local_module_path}")
        
        # Load the module dynamically
        spec = importlib.util.spec_from_file_location(module_name, local_module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        
        log.info(f"Module '{module_name}' loaded successfully from GCS")
        return module

    except Exception as e:
        log.error(f"Error loading module '{module_name}' from GCS: {e}", exc_info=True)
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
    'telecom_dag_new',
    default_args=default_args,
    description='Datapipeline DAG',
    schedule_interval=None,  # Event-driven, no fixed schedule
    catchup=False,
)

def log_training_sensor_location():
    print(f"Training Data Sensor is monitoring: {GCS_BUCKET}/{TRAINING_DATA_PREFIX}")

training_data_sensor_logger = PythonOperator(
    task_id='log_training_data_sensor_location',
    python_callable=log_training_sensor_location,
    dag=dag,
)

# Task: GCSObjectExistenceSensor for training data
training_data_sensor = GCSObjectsWithPrefixExistenceSensor(
    task_id='training_data_sensor',
    bucket=GCS_BUCKET,
    prefix=TRAINING_DATA_PREFIX,
    timeout=600,
    poke_interval=30,
    mode='poke',
    dag=dag,
)

# Task: Log new data sensor location
def log_new_data_sensor_location():
    print(f"New Data Sensor is monitoring: {GCS_BUCKET}/{NEW_DATA_PREFIX}")

new_data_sensor_logger = PythonOperator(
    task_id='log_new_data_sensor_location',
    python_callable=log_new_data_sensor_location,
    dag=dag,
)

# Task: GCSObjectExistenceSensor for new data
new_data_sensor = GCSObjectsWithPrefixExistenceSensor(
    task_id='new_data_sensor',
    bucket=GCS_BUCKET,
    prefix=NEW_DATA_PREFIX,
    timeout=600,
    poke_interval=30,
    mode='poke',
    dag=dag,
)
 
def load_data_task():
    try:
        print(f"New data is available")
        return True
    except Exception as e:
        print(f"Error in load_data_task: {e}")
        raise
 
def preprocess_data_task():
    log = logging.getLogger("preprocess_data_task")  # Get a logger for this task
    try:
        log.info("Initializing GCS client...")
        
        # Initialize the GCS client
        client = storage.Client()

        # Get the bucket and list files in the training bucket
        log.info(f"Fetching files from bucket: {GCS_BUCKET} with prefix: {TRAINING_DATA_PREFIX}")
        bucket = client.bucket(GCS_BUCKET)
        blobs = list(bucket.list_blobs(prefix=TRAINING_DATA_PREFIX))
        
        if not blobs:
            error_message = f"No files found in bucket: {GCS_BUCKET} with prefix: {TRAINING_DATA_PREFIX}"
            log.error(error_message)
            raise ValueError(error_message)

        # Identify the newest file (if there are multiple, based on time created)
        newest_blob = max(blobs, key=lambda blob: blob.updated)  # Use 'updated' for modification time
        log.info(f"Processing file: {newest_blob.name}")

        # Read the file content as a string
        file_content = newest_blob.download_as_text()
        log.info("File content loaded successfully.")

        # Load content into a Pandas DataFrame using StringIO
        data = pd.read_csv(io.StringIO(file_content))
        log.info("Data loaded into a DataFrame.")

        # Preprocess the data
        log.info("Starting data preprocessing...")
        preprocessed_data = preprocessing.preprocess_data(data)
        log.info("Data preprocessing complete.")

        # Save the preprocessed data to GCS directly
        base_file_name = newest_blob.name.split('/')[-1]  # Extract file name
        processed_blob_name = f"{PROCESSED_DATA_PREFIX}processed_{base_file_name}"  # Use prefix
        processed_blob = bucket.blob(processed_blob_name)

        # Upload the preprocessed data
        processed_blob.upload_from_string(preprocessed_data.to_csv(index=False), content_type="text/csv")
        log.info(f"Processed data uploaded to GCS: gs://{GCS_BUCKET}/{processed_blob_name}")

        return True

    
    except Exception as e:
        log.error(f"Error in preprocess_data_task: {e}", exc_info=True)
        raise


 
def feature_engineering_task():
    log = logging.getLogger("feature_engineering_task")  # Initialize a logger for the task
    try:
        log.info("Initializing GCS client...")

        # Initialize GCS Client
        client = storage.Client()

        # Ensure bucket and prefix are correctly used
        log.info(f"Fetching processed files from bucket: {GCS_BUCKET} with prefix: {PROCESSED_DATA_PREFIX}")
        processed_bucket = client.bucket(GCS_BUCKET)
        processed_blobs = list(processed_bucket.list_blobs(prefix=PROCESSED_DATA_PREFIX))

        if not processed_blobs:
            error_message = f"No files found in bucket {GCS_BUCKET} with prefix {PROCESSED_DATA_PREFIX}"
            log.error(error_message)
            raise ValueError(error_message)

        # Get the most recent file
        latest_blob = max(processed_blobs, key=lambda blob: blob.updated)
        processed_data_path = f"gs://{GCS_BUCKET}/{latest_blob.name}"
        log.info(f"Reading processed data from: {processed_data_path}")

        # Read the data
        processed_data = pd.read_csv(processed_data_path)
        log.info("Processed data successfully read into a DataFrame.")

        # Perform feature engineering
        target_column = "Churn"
        log.info("Finding optimal k for feature engineering...")
        feature_engineering.find_optimal_k(processed_data, target_column, k_range=range(25, 31))
        log.info("Optimal k identified. Selecting best k features...")
        best_features_for_churn = feature_engineering.select_best_k_features(processed_data, target_column)
        log.info("Feature selection completed.")

        # Save the engineered features
        clean_blob_name = CLEAN_DATA_PREFIX + "best_features_for_churn.csv"
        log.info(f"Saving feature-engineered data to: gs://{GCS_BUCKET}/{clean_blob_name}")
        clean_bucket = client.bucket(GCS_BUCKET)
        clean_blob = clean_bucket.blob(clean_blob_name)
        clean_blob.upload_from_string(best_features_for_churn.to_csv(index=False), content_type="text/csv")
        log.info(f"Feature-engineered data successfully uploaded to: gs://{GCS_BUCKET}/{clean_blob_name}")

        return True

    except Exception as e:
        log.error(f"Error in feature_engineering_task: {e}", exc_info=True)
        raise


 
def generate_schema_task():
    log = logging.getLogger("generate_schema_task")  # Initialize logger
    try:
        log.info("Initializing GCS client...")

        # Initialize GCS Client
        client = storage.Client()

        # Extract bucket name and prefix from CLEAN_DATA_PREFIX
        clean_bucket_name = GCS_BUCKET
        clean_prefix = CLEAN_DATA_PREFIX
        clean_bucket = client.bucket(clean_bucket_name)
        blobs = list(clean_bucket.list_blobs(prefix=clean_prefix))

        # Identify the newest file
        if not blobs:
            error_message = f"No files found in bucket: {clean_bucket_name} with prefix: {clean_prefix}"
            log.error(error_message)
            raise ValueError(error_message)

        newest_blob = max(blobs, key=lambda blob: blob.updated)
        gcs_file_path = f"gs://{clean_bucket_name}/{newest_blob.name}"
        log.info(f"Processing file: {newest_blob.name}")

        # Download the CSV file locally
        local_file_path = "/tmp/temp_data.csv"
        newest_blob.download_to_filename(local_file_path)
        log.info(f"Downloaded file to: {local_file_path}")

        # Load data into pandas DataFrame
        df = pd.read_csv(local_file_path)
        log.info("Data successfully loaded into pandas DataFrame.")

        # Infer schema and data profiles
        schema = {"features": []}
        log.info("Generating schema for features...")
        for column in df.columns:
            if column == "Churn":  # Skip the 'Churn' column
                log.info(f"Skipping target column: {column}")
                continue

            # Generate schema for each column
            feature = {
                "name": column,
                "data_type": str(df[column].dtype),
                "null_count": int(df[column].isnull().sum()),
                "sample_values": [str(x) for x in df[column].dropna().sample(min(5, len(df[column]))).tolist()] if not df[column].isnull().all() else [],
                "isComplete": bool(df[column].isnull().sum() == 0),
                "hasMin": float(df[column].min()) if df[column].dtype in ['int64', 'float64'] else None,
                "hasMax": float(df[column].max()) if df[column].dtype in ['int64', 'float64'] else None,
                "monitorAnomalies": True,
            }
            schema["features"].append(feature)

        log.info("Schema generation completed.")

        # Save schema as JSON to GCS
        schema_bucket_name = GCS_BUCKET
        schema_prefix = SCHEMA_TRAIN_PREFIX
        schema_blob_name = f"{schema_prefix}pandas_schema.json"
        schema_bucket = client.bucket(schema_bucket_name)
        schema_blob = schema_bucket.blob(schema_blob_name)

        # Upload schema as JSON to GCS
        schema_blob.upload_from_string(json.dumps(schema, indent=4), content_type="application/json")
        log.info(f"Schema saved to GCS: gs://{schema_bucket_name}/{schema_blob_name}")

        return True

    
    except Exception as e:
        log.error(f"Error in generate_schema_task: {e}", exc_info=True)
        raise


 
def preprocess_new_data_task():
    log = logging.getLogger("preprocess_new_data_task")
    try:
        log.info("Initializing GCS client...")

        # Initialize the GCS client
        client = storage.Client()

        # List files in the new data folder
        bucket = client.bucket(GCS_BUCKET)
        blobs = list(bucket.list_blobs(prefix=NEW_DATA_PREFIX))
        
        if not blobs:
            error_message = f"No files found in bucket: {GCS_BUCKET} with prefix: {NEW_DATA_PREFIX}"
            log.error(error_message)
            raise ValueError(error_message)

        # Identify the newest file based on update time
        newest_blob = max(blobs, key=lambda blob: blob.updated)
        log.info(f"Processing file: {newest_blob.name}")

        # Read the file into a DataFrame
        file_content = newest_blob.download_as_text()
        data = pd.read_csv(io.StringIO(file_content))
        log.info("Data read directly from GCS.")

        # Preprocess the data
        preprocessed_data = preprocessing.preprocess_new_data(data)
        log.info("Data preprocessing complete.")

        # Define the upload path for the preprocessed data
        processed_blob_name = f"{NEW_DATA_PROCESSED_PREFIX}processed_{newest_blob.name.split('/')[-1]}"
        processed_blob = bucket.blob(processed_blob_name)

        # Upload the preprocessed data
        processed_blob.upload_from_string(preprocessed_data.to_csv(index=False), content_type="text/csv")
        log.info(f"Processed data uploaded to GCS: gs://{GCS_BUCKET}/{processed_blob_name}")

        return True

    
    except Exception as e:
        log.error(f"Error in preprocess_new_data_task: {e}", exc_info=True)
        raise
 
def new_data_feature_selection_task():
    log = logging.getLogger("new_data_feature_selection_task")
    try:
        log.info("Initializing GCS client...")
        # Initialize the GCS client
        client = storage.Client()

        # List processed files in the NEW_DATA_PROCESSED_PREFIX folder
        source_bucket = client.bucket(GCS_BUCKET)
        blobs = list(source_bucket.list_blobs(prefix=NEW_DATA_PROCESSED_PREFIX))
        
        if not blobs:
            error_message = f"No processed files found in bucket: {GCS_BUCKET} with prefix: {NEW_DATA_PROCESSED_PREFIX}"
            log.error(error_message)
            raise ValueError(error_message)

        # Identify the newest processed file based on update time
        newest_processed_blob = max(blobs, key=lambda blob: blob.updated)
        log.info(f"Using processed new data file: {newest_processed_blob.name}")

        # Read the processed data into a DataFrame
        file_content = newest_processed_blob.download_as_text()
        processed_data = pd.read_csv(io.StringIO(file_content))
        log.info("Processed data loaded successfully.")

        # Load the best features file from the CLEAN_DATA_PREFIX
        best_features_blob_path = f"{CLEAN_DATA_PREFIX}best_features_for_churn.csv"
        best_features_blob = source_bucket.blob(best_features_blob_path)

        if not best_features_blob.exists():
            error_message = f"Best features file not found at: {best_features_blob_path}"
            log.error(error_message)
            raise ValueError(error_message)
        
        best_features_content = best_features_blob.download_as_text()
        best_features_data = pd.read_csv(io.StringIO(best_features_content))
        log.info("Loaded training feature set.")

        # Identify common features between the datasets
        common_features = list(set(processed_data.columns) & set(best_features_data.columns))
        if not common_features:
            error_message = "No common features found between processed data and training feature set."
            log.error(error_message)
            raise ValueError(error_message)
        
        selected_data = processed_data[common_features]
        log.info(f"Selected common features: {common_features}")

        # Define the upload path for the selected features
        clean_data_bucket = client.bucket(NEW_DATA_CLEAN_BUCKET)
        selected_features_blob_name = f"selected_features_{newest_processed_blob.name.split('/')[-1]}"
        selected_features_blob = clean_data_bucket.blob(selected_features_blob_name)

        # Upload the selected features data
        selected_features_blob.upload_from_string(selected_data.to_csv(index=False), content_type="text/csv")
        log.info(f"Selected features uploaded to GCS: gs://{NEW_DATA_CLEAN_BUCKET}/{selected_features_blob_name}")

        return True

    
    except Exception as e:
        log.error(f"Error in new_data_feature_selection_task: {e}", exc_info=True)
        raise
 
def schema_validation_and_anomaly_detection_task():
    log = logging.getLogger("schema_validation_and_anomaly_detection_task")
    try:
        log.info("Initializing GCS client...")
        # Initialize GCS Client
        client = storage.Client()

        # List files in the NEW_DATA_CLEAN_BUCKET folder
        clean_bucket_name = NEW_DATA_CLEAN_BUCKET
        clean_bucket = client.bucket(clean_bucket_name)
        blobs = list(clean_bucket.list_blobs(prefix=""))

        if not blobs:
            error_message = f"No files found in bucket: {clean_bucket_name}"
            log.error(error_message)
            raise ValueError(error_message)

        # Identify the newest file
        newest_blob = max(blobs, key=lambda blob: blob.updated)
        log.info(f"Validating file: {newest_blob.name}")
        gcs_file_path = f"gs://{clean_bucket_name}/{newest_blob.name}"

        # Download the CSV file locally
        local_file_path = "/tmp/newest_file.csv"
        newest_blob.download_to_filename(local_file_path)

        # Load data into a pandas DataFrame
        df = pd.read_csv(local_file_path)
        log.info("Data loaded into pandas DataFrame.")

        # Handle empty dataset
        if df.empty:
            error_message = "Dataset is empty. Cannot perform schema validation or anomaly detection."
            log.error(error_message)
            raise ValueError(error_message)

        # Load schema configuration from GCS
        schema_bucket_name = GCS_BUCKET  # Use the correct variable for schema bucket
        schema_blob_path = f"{SCHEMA_TRAIN_PREFIX}pandas_schema.json"
        log.info(f"Attempting to load schema file from: gs://{schema_bucket_name}/{schema_blob_path}")
        schema_bucket = client.bucket(schema_bucket_name)
        schema_blob = schema_bucket.blob(schema_blob_path)

        if not schema_blob.exists():
            error_message = f"Schema configuration file not found at: {schema_blob_path}"
            log.error(error_message)
            raise ValueError(error_message)

        schema_config = json.loads(schema_blob.download_as_text())
        log.info("Schema configuration loaded from GCS.")

        # Validate schema structure
        if "features" not in schema_config or not schema_config["features"]:
            error_message = "Schema configuration is invalid or missing 'features'."
            log.error(error_message)
            raise ValueError(error_message)

        # Schema Validation
        log.info("Starting schema validation...")
        for feature in schema_config["features"]:
            column_name = feature["name"]
            if column_name not in df.columns:
                log.warning(f"Column {column_name} is missing in the dataset. Skipping validation for this column.")
                continue

            # Validate numeric columns for min and max
            if df[column_name].dtype in ["int64", "float64"]:
                if "hasMin" in feature:
                    if (df[column_name] < feature["hasMin"]).any():
                        error_message = f"Column {column_name} has values below the minimum allowed."
                        log.error(error_message)
                        raise ValueError(error_message)
                if "hasMax" in feature:
                    if (df[column_name] > feature["hasMax"]).any():
                        error_message = f"Column {column_name} has values above the maximum allowed."
                        log.error(error_message)
                        raise ValueError(error_message)

            # Check for completeness
            if feature.get("isComplete", False) and df[column_name].isnull().any():
                error_message = f"Column {column_name} contains missing values."
                log.error(error_message)
                raise ValueError(error_message)

        log.info("Schema validation passed.")

        # Anomaly Detection
        log.info("Starting anomaly detection...")
        for feature in schema_config["features"]:
            if feature.get("monitorAnomalies", False):
                column_name = feature["name"]
                if column_name not in df.columns:
                    continue
                if df[column_name].dtype in ["int64", "float64"]:
                    values = df[column_name].dropna().values
                    z_scores = zscore(values)
                    threshold = 3  # Define z-score threshold for anomalies
                    anomalies = np.where(abs(z_scores) > threshold)[0]

                    if anomalies.size > 0:
                        log.info(f"Anomalies detected for {column_name} at indices: {anomalies.tolist()}")
                    else:
                        log.info(f"No anomalies detected for {column_name}.")

        return True

    except ValueError as ve:
        log.error(f"Validation Error: {ve}")
        raise
    except Exception as e:
        log.error(f"Error in schema_validation_and_anomaly_detection_task: {e}", exc_info=True)
        raise


def convert_latest_csv_to_jsonl_task(): 
    log = logging.getLogger("convert_latest_csv_to_jsonl_task")
    try:
        log.info("Initializing GCS client...")
        # Initialize GCS Client
        client = storage.Client()

        # Access the source bucket
        clean_bucket = client.bucket(NEW_DATA_CLEAN_BUCKET)

        # List all files in the bucket
        blobs = list(client.list_blobs(clean_bucket))

        if not blobs:
            error_message = f"No files found in bucket {NEW_DATA_CLEAN_BUCKET}"
            log.error(error_message)
            raise ValueError(error_message)

        # Find the latest file by updated timestamp
        latest_blob = max(blobs, key=lambda x: x.updated)
        source_file_name = latest_blob.name
        log.info(f"Latest file selected: {source_file_name}")

        # Local file paths
        local_csv_path = "/tmp/latest_best_features_for_churn.csv"
        local_jsonl_path = "/tmp/latest_best_features_for_churn.jsonl"

        # Download the latest file locally
        latest_blob.download_to_filename(local_csv_path)
        log.info(f"Downloaded latest file to {local_csv_path}")

        # Load the CSV file into a pandas DataFrame
        df = pd.read_csv(local_csv_path)
        log.info("CSV file loaded into DataFrame.")

        # Convert DataFrame to JSON Lines format
        with open(local_jsonl_path, "w") as jsonl_file:
            for _, row in df.iterrows():
                jsonl_file.write(row.to_json() + "\n")
        log.info(f"DataFrame converted to JSONL format and saved at {local_jsonl_path}")

        # Upload the JSONL file to the destination bucket
        destination_bucket_name = "vertex_model_data"  # Use the existing model_data bucket
        destination_file_name = "latest_best_features_for_churn.jsonl"
        destination_bucket = client.bucket(destination_bucket_name)
        destination_blob = destination_bucket.blob(destination_file_name)

        destination_blob.upload_from_filename(local_jsonl_path, content_type="application/json")
        log.info(f"JSONL file uploaded to bucket {destination_bucket_name} as {destination_file_name}")

        return True

   
    except Exception as e:
        log.error(f"Error in convert_latest_csv_to_jsonl_task: {e}", exc_info=True)
        raise
   
# Define Python tasks
load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data_task,
    dag=dag,
)
 
preprocess_data = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data_task,
    dag=dag,
)
 
feature_engineering_task_op = PythonOperator(
    task_id='feature_engineering',
    python_callable=feature_engineering_task,
    dag=dag,
)
 
generate_schema = PythonOperator(
    task_id='generate_schema',
    python_callable=generate_schema_task,
    dag=dag,
)
 
preprocess_new_data = PythonOperator(
    task_id='preprocess_new_data',
    python_callable=preprocess_new_data_task,
    dag=dag,
)
 
new_data_feature_selection = PythonOperator(
    task_id='new_data_feature_selection',
    python_callable=new_data_feature_selection_task,
    dag=dag,
)
 
schema_validation = PythonOperator(
    task_id='schema_validation',
    python_callable= schema_validation_and_anomaly_detection_task,
    dag=dag,
)

convert_csv_to_jsonl = PythonOperator(
    task_id='convert_csv_to_jsonl',
    python_callable=convert_latest_csv_to_jsonl_task,
    dag=dag,
)
 
# Set task dependencies
training_data_sensor_logger >> training_data_sensor >> load_data >> preprocess_data >> feature_engineering_task_op >> generate_schema
new_data_sensor_logger >> new_data_sensor >> preprocess_new_data >> new_data_feature_selection >> schema_validation >> convert_csv_to_jsonl