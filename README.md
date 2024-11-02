# Telecom-Device-Upgrade-Prediction
An MLOps project to predict telecom device upgrades using customer demographics and handset data. 

![Architechutre Diagram](https://github.com/MLOPS-Team-7/Telecom-Device-Upgrade-Prediction/blob/main/ETL_MLOps_Diagram.jpg)

Proposed Architechture Diagram and Workflow in GCP

**DAG 1 (Initial Data Processing):**
- Starts with raw data ingestion from external sources into Cloud Storage
- Triggers a Cloud Function/ Airflow DAG 1
- Processes data through Data Validation and Transformation in Python using Airflow
- This DAG handles the initial data preparation phase

**DAG 2 (Model Training):**
- Takes transformed data from BigQuery/GCS 
- Includes various data inputs (Train Text, Logs, Train Data)
- Uses Vertex AI for model training
- Stores the model in GCS Model Registry
- Includes Cloud Monitoring for tracking the training process
- Focuses on the machine learning model training phase

**DAG 3 (Model Evaluation):**
- Handles Model Evaluation and Data Drift Detection via Cloud Functions/ Python Scripts
- Checks if the model meets specified criteria
- Acts as a quality control phase for the trained model

**DAG 4 (Inference Pipeline):**
- Uses Compute Engine for model inference using Vertex AI (XGBoost)
- Processes predictions through BigQuery
- Includes drift detection and notification through PubSub
- Monitors the inference process
- This DAG manages the actual model deployment and prediction phase

**DAG 5 (Reporting and Notification):**
- Handles reporting through Looker
- Includes business analysis components
- Processes messaging rules/actions through PubSub
- Pushes notifications to mobile devices via App Engine
- This final DAG manages the communication and reporting aspects

Each DAG is orchestrated using Airflow, and they're interconnected to form a complete ML pipeline from data ingestion to final reporting. The workflow proposed includes comprehensive monitoring throughout the process with Cloud Monitoring integration at various stages.
