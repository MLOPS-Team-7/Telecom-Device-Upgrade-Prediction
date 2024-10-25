# %%
from google.cloud import storage
import os

# Define your GCS bucket and file paths
BUCKET_NAME = "data-source-telecom-customers"
TRAIN_FILE = "data/raw_data/train/train.csv"
TEST_FILE = "data/raw_data/test/test.csv"
HOLDOUT_DATASETS_PATH= "data/raw_data/validation_data"

# Define local folder paths
# Update these paths to match your local environment
LOCAL_TRAIN_PATH = "/Users/svs/Desktop/Projects/Telecom-Device-Upgrade-Prediction/data/raw/train.csv"
LOCAL_TEST_PATH = "/Users/svs/Desktop/Projects/Telecom-Device-Upgrade-Prediction/data/raw/test.csv"
LOCAL_HOLDOUT_PATH = "/Users/svs/Desktop/Projects/Telecom-Device-Upgrade-Prediction/data/raw"

HOLDOUT_DATASETS = ["holdout_batch_1.csv", "holdout_batch_2.csv", "holdout_batch_3.csv", "holdout_batch_4.csv"]

# Initialize the GCS client
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

def download_file_from_gcs(blob_name, local_path):
    """Downloads a file from GCS to a local directory."""
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)
    print(f"Downloaded {blob_name} to {local_path}")

# Download files from GCS to local environment
# Ensure the local directory exists
os.makedirs("/Users/svs/Desktop/Projects/Telecom-Device-Upgrade-Prediction/data/raw", exist_ok=True)
download_file_from_gcs(TRAIN_FILE, LOCAL_TRAIN_PATH)
download_file_from_gcs(TEST_FILE, LOCAL_TEST_PATH)

for dataset in HOLDOUT_DATASETS:
    HOLDOUT_GCS_PATH = HOLDOUT_DATASETS_PATH + "/" + dataset
    LOCAL_PATH = LOCAL_HOLDOUT_PATH + "/" + dataset
    download_file_from_gcs(HOLDOUT_GCS_PATH, LOCAL_PATH)

