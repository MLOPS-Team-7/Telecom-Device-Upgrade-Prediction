# tests/test_download_data.py
import sys
import os

# Ensure the src folder is on the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import unittest
from unittest.mock import patch, MagicMock
from src.download_data import download_file_from_gcs

class TestDownloadData(unittest.TestCase):

    @patch("src.download_data.storage.Client")
    def test_download_file_from_gcs(self, mock_storage_client):
        # Mock the GCS client and bucket
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        mock_storage_client.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Set up the blob's download_to_filename method
        mock_blob.download_to_filename.return_value = None

        # Call the function to test
        download_file_from_gcs("test_blob", "test_path")

        # Assert that download_to_filename was called with the correct arguments
        mock_blob.download_to_filename.assert_called_with("test_path")
        print("Test passed: download_to_filename called with correct path")

if __name__ == "__main__":
    unittest.main()
