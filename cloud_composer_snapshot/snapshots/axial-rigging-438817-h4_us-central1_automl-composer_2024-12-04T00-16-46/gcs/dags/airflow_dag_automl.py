import logging
from datetime import timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.vertex_ai.auto_ml import CreateAutoMLTabularTrainingJobOperator
from google.cloud import aiplatform
# from google.cloud.aiplatform_v1beta1.types import (
#     ModelMonitoringAlertConfig,
#     ModelMonitoringObjectiveConfig, ThresholdConfig)


# # Define the drift thresholds for features
# prediction_drift_config = ModelMonitoringObjectiveConfig.PredictionDriftDetectionConfig(
#     drift_thresholds={
#         "AgeHH1": ThresholdConfig(value=0.05),
#         "CurrentEquipmentDays": ThresholdConfig(value=0.05),
#         "PercChangeMinutes": ThresholdConfig(value=0.05),
#     }
# )


# # Define the full alert configuration
# model_monitoring_alert_config = ModelMonitoringAlertConfig(
#     enable_logging=True,  # Enable Cloud Logging for anomalies
#     email_alert_config=ModelMonitoringAlertConfig.EmailAlertConfig(
#     user_emails=["shiledarnakul@example.com"]  # Ensure the email is correct
# )  # Specify email alert config
# )

# Set up global logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Helper function to get a logger
def get_task_logger(task_name):
    return logging.getLogger(task_name)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Constants for the project and bucket
PROJECT_ID = "axial-rigging-438817-h4"
LOCATION = "us-central1"
MODEL_DISPLAY_NAME = "churn_model_2"
EXISTING_DATASET_ID = "446996006911868928"
TARGET_COLUMN = "Churn"
BUDGET_MILLI_NODE_HOURS = 1000
BUCKET_NAME = "vertex_model_data"

# Task: Check if model exists
def check_model_existence(**kwargs):
    log = get_task_logger("check_model_existence")
    try:
        log.info("Initializing Vertex AI platform...")
        aiplatform.init(project=PROJECT_ID, location=LOCATION)

        log.info(f"Checking for model with display name '{MODEL_DISPLAY_NAME}'...")
        models = aiplatform.Model.list(
            filter=f"display_name={MODEL_DISPLAY_NAME}",
            order_by="create_time desc"
        )
        if models:
            latest_model = models[0]
            log.info(f"Model '{MODEL_DISPLAY_NAME}' exists. Model ID: {latest_model.resource_name}")
            kwargs['ti'].xcom_push(key='model_exists', value=True)
            kwargs['ti'].xcom_push(key='model_name', value=latest_model.resource_name)
        else:
            log.info(f"No model with the display name '{MODEL_DISPLAY_NAME}' found.")
            kwargs['ti'].xcom_push(key='model_exists', value=False)
    except Exception as e:
        log.error("Error during model existence check.", exc_info=True)
        raise

# Task: Branch task logic
def branch_task(**kwargs):
    log = get_task_logger("branch_task")
    try:
        model_exists = kwargs['ti'].xcom_pull(task_ids='check_model_existence', key='model_exists')
        if model_exists:
            log.info("Model exists. Skipping training.")
            return 'skip_training'
        else:
            log.info("Model does not exist. Proceeding to training.")
            return 'train_auto_ml_model'
    except Exception as e:
        log.error("Error in branching logic.", exc_info=True)
        raise

# Task: Create batch prediction job
def create_batch_prediction_job(**kwargs):
    log = get_task_logger("create_batch_prediction_job")
    try:
        model_name = kwargs['ti'].xcom_pull(task_ids='check_model_existence', key='model_name')
        if not model_name:
            raise ValueError("No model found for batch prediction.")

        log.info("Initializing Vertex AI platform for batch prediction...")
        aiplatform.init(project=PROJECT_ID, location=LOCATION)

        gcs_input = f"gs://{BUCKET_NAME}/latest_best_features_for_churn.jsonl"
        bigquery_output_prefix = "axial-rigging-438817-h4.Big_query_batch_prediction"

        log.info("Creating batch prediction job...")
        batch_prediction_job = aiplatform.BatchPredictionJob.create(
            job_display_name="batch_prediction_job",
            model_name=model_name,
            gcs_source=gcs_input,
            predictions_format="bigquery",
            starting_replica_count=20,
            max_replica_count=30,
            machine_type="c2-standard-30",
            bigquery_destination_prefix=bigquery_output_prefix
            # model_monitoring_objective_config=prediction_drift_config,
            # model_monitoring_alert_config=model_monitoring_alert_config

)

        log.info(f"Batch prediction job created successfully: {batch_prediction_job.resource_name}")
    except Exception as e:
        log.error("Error during batch prediction job creation.", exc_info=True)
        raise

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

    # Step 2: Branch task
    branch_task_operator = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_task,
        provide_context=True
    )

    # Step 3: Train AutoML Model
    train_auto_ml_model = CreateAutoMLTabularTrainingJobOperator(
        task_id="train_auto_ml_model",
        project_id=PROJECT_ID,
        region=LOCATION,
        display_name=MODEL_DISPLAY_NAME,
        optimization_prediction_type="classification",
        dataset_id=EXISTING_DATASET_ID,
        target_column=TARGET_COLUMN,
        budget_milli_node_hours=BUDGET_MILLI_NODE_HOURS,
        disable_early_stopping=False
    )

    # Step 4: Skip training (dummy task)
    skip_training = PythonOperator(
        task_id="skip_training",
        python_callable=lambda: get_task_logger("skip_training").info("Model already exists, skipping training.")
    )

    # Step 5: Create Batch Prediction Job
    create_batch_prediction_job_task = PythonOperator(
        task_id="create_batch_prediction_job",
        python_callable=create_batch_prediction_job,
        provide_context=True
    )

    trigger_Dag_3 = TriggerDagRunOperator(task_id='Dag_3_trigger', trigger_dag_id='bigquery_to_pubsub', wait_for_completion=False )
    # Set up task dependencies
    check_model_task >> branch_task_operator
    branch_task_operator >> train_auto_ml_model
    branch_task_operator >> skip_training
    skip_training >> create_batch_prediction_job_task >> trigger_Dag_3