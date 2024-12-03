from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import CreateAutoMLTabularTrainingJobOperator
from google.cloud import aiplatform
from google.cloud import storage

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Constants for the project and bucket
PROJECT_ID = "axial-rigging-438817-h4"
LOCATION = "us-central1"
MODEL_DISPLAY_NAME = "churn_model_2"
EXISTING_DATASET_ID = "446996006911868928"  # Existing dataset ID
TARGET_COLUMN = "Churn"
BUDGET_MILLI_NODE_HOURS = 1000
BUCKET_NAME = "holdout_batches"

# Check if model already exists in Vertex AI
def check_model_existence(**kwargs):
    aiplatform.init(project=PROJECT_ID, location=LOCATION)
    models = aiplatform.Model.list(
        filter=f"display_name={MODEL_DISPLAY_NAME}",
        order_by="create_time desc"
    )
    if models:
        print(f"Model '{MODEL_DISPLAY_NAME}' already exists in Vertex AI model registry.")
        latest_model = models[0]
        print(f"Model ID: {latest_model.resource_name}")
        kwargs['ti'].xcom_push(key='model_exists', value=True)
        kwargs['ti'].xcom_push(key='model_name', value=latest_model.resource_name)  # Push model name
    else:
        print(f"No model with the display name '{MODEL_DISPLAY_NAME}' found.")
        kwargs['ti'].xcom_push(key='model_exists', value=False)

# Branching logic to skip the training if model exists
def branch_task(**kwargs):
    model_exists = kwargs['ti'].xcom_pull(task_ids='check_model_existence', key='model_exists')
    if model_exists:
        return 'skip_training'
    else:
        return 'train_auto_ml_model'

# Fetch the latest file from GCS
def get_latest_gcs_file(**kwargs):
    ti = kwargs['ti']
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs())  # List all files in the bucket

    # Find the latest file by timestamp
    latest_blob = max(blobs, key=lambda blob: blob.updated, default=None)
    if latest_blob:
        latest_file_name = latest_blob.name  # Only get the file name, not the full path
        print(f"Latest file found: {latest_file_name}")
        ti.xcom_push(key='latest_gcs_file', value=latest_file_name)
    else:
        raise FileNotFoundError("No files found in the GCS bucket.")

# Function to create batch prediction job
def create_batch_prediction_job(**kwargs):
    model_name = kwargs['ti'].xcom_pull(task_ids='check_model_existence', key='model_name')
    if not model_name:
        raise ValueError("No model found for batch prediction.")

    aiplatform.init(project=PROJECT_ID, location=LOCATION)

    # Set input and output configurations
    gcs_input = f"gs://{BUCKET_NAME}/holdout_batch_1_features_small_set.jsonl"
    bigquery_output_prefix = "axial-rigging-438817-h4.Big_query_batch_prediction"
    
    batch_prediction_job = aiplatform.BatchPredictionJob.create(
        job_display_name="batch_prediction_job",
        model_name=model_name,
        gcs_source=gcs_input,
        predictions_format="bigquery",

        bigquery_destination_prefix=bigquery_output_prefix

    )
    print(f"Batch prediction job created: {batch_prediction_job.resource_name}")

# Define the DAG
with DAG(
    dag_id="vertex_ai_churn_model_training",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Step 1: Check if Model Exists
    check_model_task = PythonOperator(
        task_id="check_model_existence",
        python_callable=check_model_existence,
        provide_context=True
    )

    # Step 2: Branch task to decide whether to skip or train the model
    branch_task_operator = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_task,
        provide_context=True
    )

    # Step 3: Train Model using AutoML Tabular Training Job with Existing Dataset
    train_auto_ml_model = CreateAutoMLTabularTrainingJobOperator(
        task_id="train_auto_ml_model",
        project_id=PROJECT_ID,
        region=LOCATION,
        display_name=MODEL_DISPLAY_NAME,
        optimization_prediction_type="classification",
        dataset_id=EXISTING_DATASET_ID,  # Use existing dataset ID
        target_column=TARGET_COLUMN,
        budget_milli_node_hours=BUDGET_MILLI_NODE_HOURS,
        disable_early_stopping=False
    )

    # Step 4: Skip training if model already exists (this will be a dummy task)
    skip_training = PythonOperator(
        task_id="skip_training",
        python_callable=lambda: print("Model already exists, skipping training.")
    )

    # Step 5: Fetch the latest file from GCS
    fetch_latest_gcs_file = PythonOperator(
        task_id="fetch_latest_gcs_file",
        python_callable=get_latest_gcs_file,
        provide_context=True
    )

    # Step 6: Create Batch Prediction Job
    create_batch_prediction_job_task = PythonOperator(
        task_id="create_batch_prediction_job",
        python_callable=create_batch_prediction_job,
        provide_context=True
    )

    # Set up task dependencies
    check_model_task >> branch_task_operator
    branch_task_operator >> train_auto_ml_model
    branch_task_operator >> skip_training
    skip_training >> fetch_latest_gcs_file >> create_batch_prediction_job_task
