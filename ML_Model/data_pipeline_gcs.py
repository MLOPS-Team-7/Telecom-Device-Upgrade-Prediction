from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import pandas as pd
import importlib.util
import sys
from google.cloud import storage
from tensorflow_data_validation.utils.schema_io import write_schema_text
import tensorflow_data_validation as tfdv
import os

TRAINING_DATA_BUCKET = 'data-source-telecom-customers/data/raw_data/train'
PROCESSED_DATA_BUCKET = 'data-source-telecom-customers/data/processed_data/processed_train'
CLEAN_DATA_BUCKET = 'data-source-telecom-customers/data/clean_data/train_clean'
SCHEMA_TRAIN = 'schema_files/train'
NEW_DATA_BUCKET = 'data-source-telecom-customers/data/raw_data/new_data'
NEW_DATA_PROCESSED = 'data-source-telecom-customers/data/processed_data/processed_new_data'
NEW_DATA_CLEAN = 'holdout_batches'
SCHEMA_NEW = 'schema_files/new'


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
    'telecom_dag_new',
    default_args=default_args,
    description='Datapipeline DAG',
    schedule_interval=None,  # Event-driven, no fixed schedule
    catchup=False,
)

# Sensor for new training data
training_data_sensor = GCSObjectExistenceSensor(
    task_id='training_data_sensor',
    bucket=TRAINING_DATA_BUCKET,
    object='*.csv',
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

def load_data_task():
    try:
        print(f"New data is available")
        return True
    except Exception as e:
        print(f"Error in load_data_task: {e}")
        raise

def preprocess_data_task():
    try:
        # Initialize the GCS client
        client = storage.Client()

        # Get the bucket and list files in the training bucket
        training_bucket = client.bucket(TRAINING_DATA_BUCKET.split('/')[0])
        training_folder = "/".join(TRAINING_DATA_BUCKET.split('/')[1:])
        blobs = list(training_bucket.list_blobs(prefix=training_folder))

        # Identify the newest file (if there are multiple, based on time created)
        newest_blob = max(blobs, key=lambda blob: blob.time_created)
        print(f"Processing file: {newest_blob.name}")

        # Read the file directly from GCS as a stream
        with newest_blob.open("r") as file_stream:
            data = pd.read_csv(file_stream)
        print("Data read directly from GCS.")

        # Preprocess the data
        preprocessed_data = preprocessing.preprocess_data(data)

        # Save the preprocessed data to GCS directly
        processed_bucket = client.bucket(PROCESSED_DATA_BUCKET.split('/')[0])
        processed_folder = "/".join(PROCESSED_DATA_BUCKET.split('/')[1:])
        processed_blob_name = f"{processed_folder}/processed_{newest_blob.name.split('/')[-1]}"
        processed_blob = processed_bucket.blob(processed_blob_name)

        # Write the preprocessed data to GCS
        processed_blob.upload_from_string(preprocessed_data.to_csv(index=False), content_type="text/csv")
        print(f"Processed data uploaded directly to GCS bucket: {processed_blob_name}")

        return True

    except Exception as e:
        print(f"Error in preprocess_data_task: {e}")
        raise

def feature_engineering_task():
    try:
        # GCS Client
        client = storage.Client()

        # Read the processed file from the processed bucket
        processed_bucket = client.bucket(PROCESSED_DATA_BUCKET)
        processed_blobs = list(processed_bucket.list_blobs(prefix=""))  # List all files in the processed bucket

        # Ensure only CSV files are processed and get the latest file
        csv_blobs = [blob for blob in processed_blobs if blob.name.endswith(".csv")]
        if not csv_blobs:
            raise ValueError("No processed CSV files found in the processed bucket.")

        # Get the most recent processed file
        latest_blob = max(csv_blobs, key=lambda blob: blob.updated)  # Sort by last modified time
        processed_data = pd.read_csv(f"gs://{processed_bucket.name}/{latest_blob.name}")
        print(f"Reading processed data from: gs://{processed_bucket.name}/{latest_blob.name}")

        # Perform feature engineering
        target_column = "Churn"
        feature_engineering.find_optimal_k(processed_data, target_column, k_range=range(25, 31))
        best_features_for_churn = feature_engineering.select_best_k_features(processed_data, target_column)

        # Save the engineered features with a fixed file name
        clean_bucket = client.bucket(CLEAN_DATA_BUCKET)
        clean_blob_name = "best_features_for_churn.csv"  # Fixed file name
        clean_blob = clean_bucket.blob(clean_blob_name)

        # Convert DataFrame to CSV and upload directly to GCS
        clean_blob.upload_from_string(best_features_for_churn.to_csv(index=False), content_type="text/csv")
        print(f"Feature-engineered data uploaded to: gs://{CLEAN_DATA_BUCKET}/{clean_blob_name}")

        return True
    except Exception as e:
        print(f"Error in feature_engineering_task: {e}")
        raise

def generate_schema_task():
    try:
        # Initialize the GCS client
        client = storage.Client()

        # Define GCS paths
        clean_bucket = client.bucket(CLEAN_DATA_BUCKET)
        schema_bucket = client.bucket(SCHEMA_BUCKET)

        # Identify the newest file in the clean data bucket
        blobs = list(clean_bucket.list_blobs())
        newest_blob = max(blobs, key=lambda blob: blob.time_created)
        print(f"Processing file: {newest_blob.name}")

        # Read the file directly from GCS as a stream
        with newest_blob.open("r") as file_stream:
            data = pd.read_csv(file_stream)
        print("Data read directly from GCS.")

        # Generate statistics using TFDV
        stats = tfdv.generate_statistics_from_dataframe(data)

        # Infer a schema from the statistics
        schema = tfdv.infer_schema(stats)

        # Convert schema to a string
        schema_str = tfdv.schema_util.schema_to_text(schema)
        print("Schema generated in memory.")

        # Upload the schema string directly to GCS
        schema_blob = schema_bucket.blob('schema.pbtxt')
        schema_blob.upload_from_string(schema_str)
        print(f"Schema file schema.pbtxt uploaded successfully to bucket {SCHEMA_BUCKET}.")

        return True

    except Exception as e:
        print(f"Error in generate_schema_task: {e}")
        raise

def preprocess_new_data_task():
    try:
        # Initialize the GCS client
        client = storage.Client()

        # Get the bucket and list files in the training bucket
        new_bucket = client.bucket(NEW_DATA_BUCKET.split('/')[0])
        new_folder = "/".join(NEW_DATA_BUCKET.split('/')[1:])
        blobs = list(new_bucket.list_blobs(prefix=new_folder))

        # Identify the newest file (if there are multiple, based on time created)
        newest_blob = max(blobs, key=lambda blob: blob.time_created)
        print(f"Processing file: {newest_blob.name}")

        # Read the file directly from GCS as a stream
        with newest_blob.open("r") as file_stream:
            data = pd.read_csv(file_stream)
        print("Data read directly from GCS.")

        # Preprocess the data
        preprocessed_data = preprocessing.preprocess_new_data(data)

        # Save the preprocessed data to GCS directly
        processed_bucket = client.bucket(NEW_DATA_PROCESSED.split('/')[0])
        processed_folder = "/".join(NEW_DATA_PROCESSED.split('/')[1:])
        processed_blob_name = f"{processed_folder}/processed_{newest_blob.name.split('/')[-1]}"
        processed_blob = processed_bucket.blob(processed_blob_name)

        # Write the preprocessed data to GCS
        processed_blob.upload_from_string(preprocessed_data.to_csv(index=False), content_type="text/csv")
        print(f"Processed data uploaded directly to GCS bucket: {processed_blob_name}")

        return True

    except Exception as e:
        print(f"Error in preprocess_data_task: {e}")
        raise

def new_data_feature_selection_task():
    try:
        # Initialize the GCS client
        client = storage.Client()

        # Load the processed new data file from the NEW_DATA_PROCESSED bucket
        processed_bucket = client.bucket(NEW_DATA_PROCESSED.split('/')[0])
        processed_folder = "/".join(NEW_DATA_PROCESSED.split('/')[1:])
        blobs = list(processed_bucket.list_blobs(prefix=processed_folder))

        # Identify the newest processed file
        newest_processed_blob = max(blobs, key=lambda blob: blob.time_created)
        print(f"Using processed new data file: {newest_processed_blob.name}")

        # Read the newest processed data as a DataFrame
        with newest_processed_blob.open("r") as processed_stream:
            processed_data = pd.read_csv(processed_stream)

        # Load the training features file from the CLEAN_DATA_BUCKET
        clean_bucket = client.bucket(CLEAN_DATA_BUCKET.split('/')[0])
        clean_folder = "/".join(CLEAN_DATA_BUCKET.split('/')[1:])
        best_features_blob = clean_bucket.blob(f"{clean_folder}/best_features_for_churn.csv")
        
        # Read the training features as a DataFrame
        with best_features_blob.open("r") as features_stream:
            best_features_data = pd.read_csv(features_stream)

        # Select only the columns that are common between the two datasets
        common_features = list(set(processed_data.columns) & set(best_features_data.columns))
        selected_data = processed_data[common_features]
        print(f"Selected common features: {common_features}")

        # Save the selected data to the NEW_DATA_CLEAN bucket
        clean_bucket = client.bucket(NEW_DATA_CLEAN.split('/')[0])  # 'NEW_DATA_CLEAN' bucket
        clean_folder = "/".join(NEW_DATA_CLEAN.split('/')[1:])
        clean_blob_name = f"{clean_folder}/selected_features_{newest_processed_blob.name.split('/')[-1]}"
        clean_blob = clean_bucket.blob(clean_blob_name)

        # Write the selected features data to GCS
        clean_blob.upload_from_string(selected_data.to_csv(index=False), content_type="text/csv")
        print(f"Selected features file uploaded to GCS bucket: {clean_blob_name}")

        return True

    except Exception as e:
        print(f"Error in feature_selection_task: {e}")
        raise

def schema_validation_task():
    try:
        # Initialize the GCS client
        client = storage.Client()

        # Load the selected features file from NEW_DATA_CLEAN bucket
        clean_bucket = client.bucket(NEW_DATA_CLEAN.split('/')[0])
        clean_folder = "/".join(NEW_DATA_CLEAN.split('/')[1:])
        blobs = list(clean_bucket.list_blobs(prefix=clean_folder))

        # Identify the newest selected features file
        newest_clean_blob = max(blobs, key=lambda blob: blob.time_created)
        print(f"Using clean data file: {newest_clean_blob.name}")

        # Read the clean data file as a DataFrame
        with newest_clean_blob.open("r") as clean_stream:
            clean_data = pd.read_csv(clean_stream)

        # Generate schema for the new data using TFDV
        schema = tfdv.infer_schema(tfdv.generate_statistics_from_dataframe(clean_data))
        print("Schema generated for new data.")

        # Load the training schema from SCHEMA_TRAIN
        schema_bucket = client.bucket(SCHEMA_TRAIN.split('/')[0])
        schema_folder = "/".join(SCHEMA_TRAIN.split('/')[1:])
        schema_blob = schema_bucket.blob(f"{schema_folder}/schema.pbtxt")

        with schema_blob.open("r") as schema_stream:
            training_schema = tfdv.load_schema_text(schema_stream.read())
        print("Training schema loaded.")

        # Validate the new schema against the training schema
        anomalies = tfdv.validate_statistics(
            tfdv.generate_statistics_from_dataframe(clean_data), training_schema
        )

        if anomalies.anomaly_info:
            print("Schema validation failed. Detected anomalies:")
            print(anomalies)
        else:
            print("Schema validation successful. No anomalies detected.")

        # Save the generated schema for the new data into the SCHEMA_NEW path
        new_schema_bucket = client.bucket(SCHEMA_NEW.split('/')[0])
        new_schema_folder = "/".join(SCHEMA_NEW.split('/')[1:])
        new_schema_blob = new_schema_bucket.blob(f"{new_schema_folder}/new_data_schema.pbtxt")
        new_schema_blob.upload_from_string(tfdv.write_schema_text(schema), content_type="text/plain")
        print(f"Generated schema for new data saved in {SCHEMA_NEW}.")

        return True

    except Exception as e:
        print(f"Error in schema_validation_task: {e}")
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
    python_callable= schema_validation_task,
    dag=dag,
)

# Set task dependencies
training_data_sensor >> load_data >> preprocess_data >> feature_engineering_task_op >> generate_schema
new_data_sensor >> preprocess_new_data >> new_data_feature_selection >> schema_validation
