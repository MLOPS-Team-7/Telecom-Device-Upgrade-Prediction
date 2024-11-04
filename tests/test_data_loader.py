# tests/test_data_loader.py
import sys
import unittest
import os
import pandas as pd

# Add the `src` directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.data_loader import load_data

class TestDataLoader(unittest.TestCase):
    def setUp(self):
        """
        Setup method to create a sample CSV file for testing.
        """
        # Create a sample CSV file to test with
        self.test_file_path = 'tests/test_data.csv'
        sample_data = pd.DataFrame({
            'A': [1, 2, 3],
            'B': ['x', 'y', 'z']
        })
        sample_data.to_csv(self.test_file_path, index=False)

    def tearDown(self):
        """
        Cleanup method to remove the sample CSV file after testing.
        """
        if os.path.exists(self.test_file_path):
            os.remove(self.test_file_path)

    def test_load_data_success(self):
        """
        Test that data is loaded successfully and returned as a DataFrame.
        """
        data = load_data(self.test_file_path)
        self.assertIsNotNone(data, "Data should not be None")
        self.assertFalse(data.empty, "Data should not be empty")
        self.assertIsInstance(data, pd.DataFrame, "Data should be a pandas DataFrame")
        self.assertEqual(data.shape, (3, 2), "Data shape should match the sample data")

    def test_load_data_file_not_found(self):
        """
        Test that loading data from a non-existent file returns None and prints an error message.
        """
        data = load_data('non_existent_file.csv')
        self.assertIsNone(data, "Data should be None if the file does not exist")

if __name__ == "__main__":
    unittest.main()
