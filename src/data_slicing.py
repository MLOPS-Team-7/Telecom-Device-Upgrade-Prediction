import pandas as pd
from google.cloud import storage
from config import CHURN_TEST_PATH

# Upload DataFrame as a CSV directly to GCS
def upload_to_gcs(dataframe, bucket_name, destination_blob_name):
    """
    Uploads a DataFrame to Google Cloud Storage as a CSV file.

    Parameters:
    - dataframe: The DataFrame to upload.
    - bucket_name: GCS bucket name.
    - destination_blob_name: Path in GCS bucket where the CSV file will be stored.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(dataframe.to_csv(index=False), 'text/csv')
    print(f"Data uploaded to {destination_blob_name} in bucket {bucket_name}.")

# Slice data into equal-sized bins for a given column and upload each bin to GCS
def slice_and_upload(data, column_name, num_bins, bucket_name):
    # Create a temporary copy of the data to avoid modifying the original DataFrame
    temp_data = data.copy()
    
    # Generate bins for the column with equal size
    range_column = f'{column_name}_range'
    temp_data[range_column] = pd.cut(temp_data[column_name], bins=num_bins, labels=[f'bin_{i+1}' for i in range(num_bins)], include_lowest=True)
    
    # Group data by the binned column
    grouped_data = temp_data.groupby(range_column)
    
    # Upload each bin to GCS
    for label, subset in grouped_data:
        # Drop the temporary range column before uploading
        subset = subset.drop(columns=[range_column])
        
        filename = f'{column_name}_{label}.csv'
        destination_blob_name = f"{column_name}/{filename}"
        upload_to_gcs(subset, bucket_name, destination_blob_name)

# Main function to perform slicing and upload for multiple columns
def main():
    # Load the data
    test_data = pd.read_csv(CHURN_TEST_PATH)
    
    # Define the columns to slice and the number of bins
    columns_to_slice = ['CurrentEquipmentDays', 'PercChangeMinutes', 'AgeHH1']
    num_bins = 5
    GCS_BUCKET_NAME = 'data_slices_for_bias_detection' 

    # Slice and upload data for each specified column
    for column in columns_to_slice:
        slice_and_upload(test_data, column, num_bins, GCS_BUCKET_NAME)

# Run the main function
if __name__ == "__main__":
    main()
