# Telecom Customer Churn and Mobile Device Upgrade Prediction

## Table of Contents

1. [Overview](#overview)
2. [Data](#data)
   - [Dataset Information](#dataset-information)
   - [Data Card](#data-card)
3. [Architecture](#architecture)
4. [Prerequisites](#prerequisites)
5. [Project Structure](#project-structure)
6. [Setup Overview](#setup-overview)
   - [High-Level Steps](#high-level-steps)
7. [Getting Started](#getting-started)
   - [1. Clone the Repository](#1-clone-the-repository)
   - [2. Install Dependencies](#2-install-dependencies)
   - [3. Set Up GCP Authentication](#3-set-up-gcp-authentication)

---

## Overview

## Overview

The **Telecom Customer Churn and Device Upgrade Prediction** project aims to develop an end-to-end MLOps pipeline capable of forecasting churn and device upgrade events. By leveraging demographic and device-related features, the pipeline identifies customers likely to churn or upgrade, enabling telecom companies to target personalized offers. This solution is designed to enhance customer satisfaction, retention, and revenue growth.

### Objectives:
1. **Churn Prediction**: Identify customers at risk of leaving, enabling proactive retention measures.
2. **Upgrade Prediction**: Pinpoint non-churned customers likely to upgrade their devices soon.

---

## Data

### Dataset Information

The project uses the **Cell2Cell dataset** from the Teradata Center for Customer Relationship Management. Key features include device specifications, demographic attributes, and historical usage patterns. A custom feature, `DeviceUpgrade`, predicts device upgrades for non-churned customers based on criteria such as `CurrentEquipmentDays`.

### Data Card

| Attribute             | Details                                                                 |
|-----------------------|-------------------------------------------------------------------------|
| **Dataset Name**      | Cell2Cell dataset                                                      |
| **Source**            | [Kaggle - Telecom Churn Dataset](https://www.kaggle.com/datasets/jpacse/datasets-for-churn-telecom)  |
| **Size**              | Training: ~51,047 rows, 58 columns<br>Holdout: ~20,000 rows, 58 columns |
| **Data Types**        | Numerical (e.g., CurrentEquipmentDays, Age), Categorical (e.g., CreditRating, IncomeGroup) |
| **Target Variable**   | Churn; Upgrade prediction inferred from `CurrentEquipmentDays`         |

The dataset adheres to privacy regulations, ensuring no Personally Identifiable Information (PII)

---

## Architecture

![Architechutre Diagram](https://github.com/MLOPS-Team-7/Telecom-Device-Upgrade-Prediction/blob/main/GCP_MLOps_Diagram.jpg)

### Workflow:
1. **Data Ingestion**: Raw data is loaded into Google Cloud Storage.
2. **Data Preprocessing**: Transformation and feature engineering on Airflow DAGs.
3. **Model Training**:
   - Churn prediction using Vertex AI's AutoML.
   - SQL-based device upgrade prediction for non-churned customers.
4. **Model Deployment**: Managed via Vertex AI Model Registry.
5. **Monitoring**: Includes data drift detection, bias analysis, and alerting through GCP Monitoring and Pub/Sub.

---

## Prerequisites

- **Git** for version control.
- **Airflow** for pipeline orchestration.
- **Python 3.x** for development.
- **Google Cloud Platform Account** with services like Vertex AI, BigQuery, and Cloud Composer enabled.

---

## Project Structure

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

--




