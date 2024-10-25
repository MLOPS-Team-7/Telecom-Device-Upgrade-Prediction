from google.cloud import storage

def list_buckets():
    """Lists all buckets in your GCS project."""
    client = storage.Client()
    buckets = list(client.list_buckets())
    for bucket in buckets:
        print(bucket.name)

list_buckets()
