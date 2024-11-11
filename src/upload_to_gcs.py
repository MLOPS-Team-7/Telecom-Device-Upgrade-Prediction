
from google.cloud import storage

def upload_to_gcs(local_file_path, bucket_name, destination_blob_name):
    """
    Uploads a file to Google Cloud Storage.

    Parameters:
    - local_file_path: Path to the local file to be uploaded.
    - bucket_name: GCS bucket name.
    - destination_blob_name: Path in GCS bucket where the file will be stored.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)
    print(f"File {local_file_path} uploaded to {destination_blob_name} in bucket {bucket_name}.")


