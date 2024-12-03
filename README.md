# Telecom-Device-Upgrade-Prediction

## Table of Contents

1. [Overview](#overview)
2. [Data](#data)
   - [Dataset Information](#dataset-information)
   - [Data Card](#data-card)
3. [Architecture](#architecture)
4. [Prerequisites](#prerequisites)
5. [Project Structure](#project-structure)
6. [Setup Overview](#setup-overview)
7. [Getting Started](#getting-started)
   - [1. Clone the Repository](#1-clone-the-repository)
   - [2. Install Dependencies](#2-install-dependencies)
   - [3. Set Up GCP Authentication](#3-set-up-gcp-authentication)
8. [Pipeline Workflow](#pipeline-workflow)
   - [1. Data Loading](#data-loading)
   - [2. Cloud Composer Workflow](#cloud-composer-workflow)
   - [3. Model Training on Vertex AI](#model-training-on-vertex-ai)
   - [4. Model Validation and Bias Detection](#model-validation-and-bias-detection)
   - [5. Experiment Tracking and Hyperparameter Tuning](#experiment-tracking-and-hyperparameter-tuning)


---

## Overview

In this project, we embark on a journey to build an end-to-end **Telecom Churn and Device Upgrade Prediction** MLOps pipeline, leveraging the capabilities of Airflow and Google Cloud Platform (GCP). This pipeline automates every stage of the machine learning workflow—from data acquisition and preprocessing to model training, deployment, and monitoring—ensuring scalability, reliability, and continuous improvement.

Key objectives include accurately predicting customer churn and identifying upgrade needs for loyal customers, enabling telecom companies to offer timely, personalized marketing strategies:

1. **Churn Prediction**: The model first predicts customer churn based on a combination of demographics and handset usage data.
2. **Device Upgrade Prediction**: For customers likely to stay, the model further assesses the probability of a device upgrade, allowing for targeted upgrade offers.

By integrating MLOps practices, this pipeline ensures reproducibility and robustness through continuous integration, data versioning, and automated model retraining. Additionally, the pipeline incorporates bias detection and mitigation, providing equitable model performance across diverse customer subgroups.

---

## Data

### Dataset Information

The project utilizes the **Cell2Cell dataset**, which is specifically designed to analyze telecom customer behavior and includes features essential for predicting device upgrades and customer churn. The dataset is managed by the Teradata Center for Customer Relationship Management at Duke University and is publicly available on Kaggle.

1. **Device-Related Attributes**: Includes details such as handset models, the number of handsets owned, and handset price. These attributes help capture the current specifications and value of the customer’s device, which are indicative of upgrade potential.
2. **Demographic Data**: Contains socioeconomic details like income group, occupation, and geographic location (represented by PrizmCode). These features provide insight into factors that may influence a customer’s upgrade decisions.

Since the dataset lacked a direct indicator for device upgrades, we created a custom **DeviceUpgrade** feature based on the following criteria:

- **MonthlyMinutes > 3000**
- **RetentionCalls > 2**
- **RetentionOffersAccepted > 0**
- **HandsetWebCapable == 0**
- **HandsetRefurbished == 1**
- **CurrentEquipmentDays > 340**
- **CreditRating > 5**
- **MadeCallToRetentionTeam == 1**
- **RespondsToMailOffers == 1**

These thresholds were defined using domain research and expert insights to capture upgrade likelihood.


### Data Card

| Attribute          | Details                                                                 |
|--------------------|-------------------------------------------------------------------------|
| **Dataset Name**   | Cell2Cell dataset                                                      |
| **Source**         | [Kaggle - Telecom Churn Dataset](https://www.kaggle.com/datasets/jpacse/datasets-for-churn-telecom)  |
| **Size**           | Training Data: ~51,047 rows, 58 columns<br>Holdout Data: ~20,000 rows, 58 columns |
| **Data Format**    | CSV (Comma-Separated Values)                                           |
| **Data Types**     | Numerical (e.g., handset price, days since last upgrade), Categorical (e.g., handset model, income group, occupation) |

---

## Architecture

![Architechutre Diagram](https://github.com/MLOPS-Team-7/Telecom-Device-Upgrade-Prediction/blob/main/GCP_MLOps_Diagram.jpg)

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

---


## Prerequisites

To set up and run this project, ensure you have the following tools and accounts:

- **Git**: For version control and managing code changes.
- **Docker**: To create consistent containerized environments.
- **Airflow**: Required for orchestrating the data pipeline workflow.
- **DVC (Data Version Control)**: Used to track and manage dataset versions.
- **Python 3.x**: Necessary for running the project’s scripts and code.
- **Pip**: Python package manager for installing project dependencies.
- **Google Cloud Platform (GCP) Account**: Required to access GCP services leveraged in the project.

These tools are essential for setting up a reproducible and robust MLOps pipeline.

---

## Project Structure

Here’s an overview of the project's directory structure:

### Description of Key Folders
- **airflow/dags/**: Contains the Airflow Directed Acyclic Graphs (DAGs) used to orchestrate different stages of the pipeline.
- **data/**: Holds raw and processed data files. You may use `data/raw` for unprocessed data and `data/processed` for prepared data.
- **src/**: Contains all core Python scripts organized by task:
  - **data_preprocessing/**: Data preparation and transformation scripts.
  - **model_training/**: Model training and hyperparameter tuning scripts.
  - **model_evaluation/**: Scripts for evaluating model performance and detecting drift.
  - **inference/**: Code for making predictions with the trained model.
  - **utils/**: Helper functions and utilities shared across various modules.
- **tests/**: Contains unit and integration tests for validating the project’s functionality.
- **Dockerfile**: Used to create a Docker image, enabling consistent environment setup.
- **requirements.txt**: Lists all the Python packages needed to run the project.

This structure ensures modularity, scalability, and ease of navigation for each stage of the project. Let me know if you need more details on any specific part!


---

## Setup Overview

### High-Level Steps
1. **GCP Setup**: Create a GCP project, enable APIs, and configure a service account.
2. **Environment Configuration**:
   - Install Docker and Airflow.
   - Set up a Python virtual environment for dependencies.
   - Configure GCP credentials.
3. **Data Preparation**:
   - Prepare and upload datasets (train, test, holdout) to GCS.
4. **Data Retrieval**: Use a script to fetch data from GCS for local processing.

---

## Getting Started

### 1. Clone the Repository
   ```bash
   git clone https://github.com/MLOPS-Team-7/Telecom-Device-Upgrade-Prediction.git
   cd Telecom-Device-Upgrade-Prediction
  ```

### 2. Install Dependencies
   ```bash
   pip install -r requirements.txt
  ```

### 3. Set Up GCP Authentication
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/your-service-account-file.json"
  ```

## Cloud-Based Machine Learning Pipeline with GCP

This section introduces the project's objective: building an end-to-end pipeline for machine learning using GCP. The pipeline incorporates preprocessing, automated workflows with Cloud Composer, and model training and inferences with Vertex AI.

**Objective:**
- Transition from local data preprocessing to cloud-based orchestration.
- Develop and evaluate machine learning models using both AutoML and custom-built ML models.
- Ensure reproducibility, scalability, and fairness in model development.

---

## Pipeline Workflow

### Data Loading
The transition from a local processing environment to the cloud was facilitated using Google Cloud Composer. After preprocessing the data locally in Visual Studio Code, it was essential to ensure scalability and automation, prompting the move to Google Cloud Platform (GCP). The data was uploaded to Google Cloud Storage (GCS), which served as a centralized repository for storing raw and processed data.

- **Airflow DAG using Cloud Composer:**  
  The use of Cloud Composer enabled the orchestration of complex workflows through Airflow’s Directed Acyclic Graphs (DAGs). These DAGs automated the execution of preprocessing modules, ensuring seamless integration with the cloud infrastructure. Additionally, all dependencies and Python libraries required for preprocessing were mirrored in the Composer environment, maintaining consistency with the local setup.

---

### Cloud Composer Workflow

#### Setting Up Cloud Composer
- Configured with necessary Google Cloud services, such as `google-cloud-aiplatform` and `google-cloud-storage`.
- Integrated with Google Cloud Storage (GCS) for seamless data ingestion and output handling.
- Environment mirrored local modules to ensure compatibility and scalability.

#### DAG 1: Data Processing Workflow
**Pipeline Steps:**
1. **Load Raw Data:** Ingested raw data from GCS buckets and validated its initial state.
2. **Integrate Local Modules:**  
   - Feature engineering to derive additional attributes.  
   - Cleaning operations to handle missing values, duplicates, and inconsistencies.
3. **Save Processed Data:** Cleaned datasets were stored back in GCS, ready for subsequent machine learning tasks.

**Outputs:**  
Fully cleaned and preprocessed datasets stored in GCS for downstream operations like training machine learning models in Vertex AI.

---

### Model Training on Vertex AI

#### Approach 1: AutoML via Colab Notebooks
- Automated tasks such as uploading data, initiating training jobs, and retrieving evaluation metrics using Python scripts.
- **Benefits:** Greater control and customizability over the AutoML process.
- **Challenges:** Required resource optimization for long-running training tasks.

#### Approach 2: Custom Model Development
- Used XGBoost for manual training, hyperparameter tuning, and evaluation.
- **Workflow:**
  - Data was loaded from GCS, preprocessed programmatically, and trained using XGBoost.
  - Dockerized the model for consistency and scalability, stored the image in Google Container Registry (GCR).
- **Drawbacks:** Achieved ~65% accuracy, which was suboptimal compared to AutoML results.

#### Approach 3: Airflow-Integrated AutoML
- Integrated AutoML with Airflow DAGs in Cloud Composer.
- **Pipeline Steps:**
  1. Check Model Registry for existing trained models.
  2. Trigger AutoML training only if no models exist.
  3. Evaluate model performance and select the best candidate for deployment.
- **Advantages:** Fully automated and efficient training workflow.

#### Conditional Model Utilization Workflow
- Checked for pre-trained models in the registry before proceeding with new training.
- Efficiently managed resources and maintained a seamless model lifecycle.

#### Notification and Alerts
- Automatic email notifications were triggered upon successful model training and batch predictions.

---

### Model Validation and Bias Detection

#### Validation
- AutoML performed dataset validation, handled hyperparameter tuning, and selected the best-performing model using metrics like precision, recall, F1 score, and ROC AUC.

#### Inferences after Model Validation
- Generated batch predictions using the test set, stored results in BigQuery for scalability and integration.

#### Sensitivity Analysis
- Vertex AI’s tools, including SHAP values, analyzed feature importance and the robustness of predictions.
- Results were stored in GCS for review and presentation.

#### Bias Detection
- Focused on the `CurrentEquipmentDays` column, dividing it into slices to evaluate fairness.
- Results were stored in BigQuery for further analysis.

---

### Experiment Tracking and Hyperparameter Tuning
- Vertex AI managed experiment tracking, model versioning, and artifact management.
- Provided comprehensive logs of hyperparameter configurations, metrics, and visualizations like confusion matrices and feature importance graphs.

---

This successfully demonstrates the development of a fully cloud-native machine learning pipeline, eliminating the need for local dependencies. By leveraging GCP services such as Cloud Composer, Vertex AI, BigQuery, and Google Cloud Storage, all components of the pipeline—from data preprocessing to model training and deployment—were executed seamlessly on the cloud.

- Cloud Composer streamlined workflows by orchestrating preprocessing and training tasks.
- Vertex AI facilitated robust model development and tracking, enabling reproducibility and scalability.
- This approach highlights the flexibility and efficiency of cloud-based workflows for deploying advanced machine learning models.



