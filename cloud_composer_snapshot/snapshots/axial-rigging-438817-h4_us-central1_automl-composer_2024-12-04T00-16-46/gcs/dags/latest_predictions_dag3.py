from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.cloud import pubsub_v1
from datetime import datetime
import logging

# Pub/Sub topic
PUBSUB_TOPIC = "projects/axial-rigging-438817-h4/topics/customer_notifications"

# Default args for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Function to query BigQuery
def query_bigquery(**kwargs):    
    
    logging.info("Querying BigQuery for the latest table")
    project_id = "axial-rigging-438817-h4"
    dataset_id = "Big_query_batch_prediction"
    prefix = "predictions_"

    client = bigquery.Client(project=project_id)
    
    # Query to get the latest table
    metadata_query = f"""
        SELECT table_name
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE '{prefix}%'
        ORDER BY table_name DESC
        LIMIT 1
    """
    
    df = client.query(metadata_query).to_dataframe()

    latest_table = df.iloc[0]["table_name"]
    logging.info(f"Latest table identified: {latest_table}")

    query = f"""
        SELECT CustomerID, EmailID, CurrentEquipmentDays
        FROM `{project_id}.{dataset_id}.{latest_table}`
        WHERE "0" IN UNNEST(predicted_Churn.classes) AND SAFE_CAST(CurrentEquipmentDays AS INT64) > 340
    """
    
    result = client.query(query).to_dataframe()

    if result.empty:
        logging.info("No data found for the given condition")
        return

    logging.info(f"Fetched {len(result)} records")
    kwargs['ti'].xcom_push(key='filtered_data', value=result.to_dict('records'))
    return result

def query_latest_view(**kwargs):
    logging.info("Updating the latest view in BigQuery")

    project_id = "axial-rigging-438817-h4"
    client = bigquery.Client(project=project_id)

    # Dynamic SQL to update the view
    query = """
        DECLARE query_string STRING;
        DECLARE table_list STRING;

        -- Step 1: Get the list of table names
        SET table_list = (
          SELECT STRING_AGG(
            CONCAT(
              'SELECT *, ',
              'SAFE.PARSE_TIMESTAMP("%Y_%m_%dT%H_%M_%S", ',
              'REGEXP_EXTRACT("', table_name, '", r"_(\\\\d{4}_\\\\d{2}_\\\\d{2}T\\\\d{2}_\\\\d{2}_\\\\d{2})")) AS prediction_date, ',
              -- Add the logic to determine the FinalPredictedChurnClass
              'CASE ',
              'WHEN ARRAY_LENGTH(predicted_Churn.scores) = 2 AND predicted_Churn.scores[ORDINAL(1)] > predicted_Churn.scores[ORDINAL(2)] THEN 0 ',
              'WHEN ARRAY_LENGTH(predicted_Churn.scores) = 2 AND predicted_Churn.scores[ORDINAL(2)] > predicted_Churn.scores[ORDINAL(1)] THEN 1 ',
              'ELSE NULL END AS FinalPredictedChurnClass ',
              'FROM `axial-rigging-438817-h4.Big_query_batch_prediction.', table_name, '`'),
            ' UNION ALL '
          )
          FROM `axial-rigging-438817-h4.Big_query_batch_prediction.INFORMATION_SCHEMA.TABLES`
          WHERE table_name LIKE 'predictions_%'
        );

        -- Step 2: Create the dynamic query string to create the view
        SET query_string = CONCAT(
          'CREATE OR REPLACE VIEW `axial-rigging-438817-h4.Big_query_batch_prediction.latest_predictions_view` AS ',
          table_list
        );

        -- Step 3: Execute the dynamic query to create the view
        EXECUTE IMMEDIATE query_string;
    """

    # Execute the query
    job = client.query(query)
    job.result() 
    logging.info("View updated successfully")

# Function to publish messages to Pub/Sub
def send_pubsub_messages(**kwargs):
    logging.info("Publishing messages to Pub/Sub")
    data = kwargs['ti'].xcom_pull(key='filtered_data', task_ids='query_bigquery')
    if not data:
        logging.info("No data to publish to Pub/Sub")
        return

    publisher = pubsub_v1.PublisherClient()
    for record in data:
        message = {
            "CustomerID": record['CustomerID'],
            "EmailID": record['EmailID'],
            "CurrentEquipmentDays": record['CurrentEquipmentDays'],
        }
        message_bytes = str(message).encode('utf-8')
        try:
            publisher.publish(PUBSUB_TOPIC, message_bytes)
            logging.info(f"Message published for CustomerID {record['CustomerID']}")
        except Exception as e:
            logging.error(f"Failed to publish message for CustomerID {record['CustomerID']}: {e}")

# Define the DAG
with DAG(
    'bigquery_to_pubsub',
    default_args=default_args,
    description='Fetch predictions from BigQuery and send Pub/Sub notifications',
    schedule_interval=None,
    start_date=datetime(2024, 11, 22),
    catchup=False,
) as dag:

    query_bigquery_task = PythonOperator(
        task_id='query_bigquery',
        python_callable=query_bigquery,
        provide_context=True,
    )
    
    update_view_task = PythonOperator(
        task_id='update_latest_view',
        python_callable=query_latest_view,
        provide_context=True,
        dag=dag
    )

    send_pubsub_messages_task = PythonOperator(
        task_id='send_pubsub_messages',
        python_callable=send_pubsub_messages,
        provide_context=True,
    )

    query_bigquery_task >> update_view_task >> send_pubsub_messages_task