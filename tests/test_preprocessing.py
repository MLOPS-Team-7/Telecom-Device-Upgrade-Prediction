# tests/test_preprocessing.py
import sys
import os
# Ensure the src folder is on the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
import unittest
import pandas as pd
from unittest.mock import patch, MagicMock


from src.preprocessing import drop_unnecessary_columns, fill_missing_values, encode_categorical_columns, preprocess_data

class TestPreprocessing(unittest.TestCase):

    def setUp(self):
        """
        Set up a sample DataFrame for testing purposes.
        """
        self.sample_data = pd.DataFrame({
            'CustomerID': [101, 102, 103, 104, 105],
            'Churn': ['Yes', 'No', 'Yes', 'No', 'Yes'],
            'MonthlyRevenue': [100.0, 150.0, None, 130.0, 120.0],
            'CreditRating': ['1-Highest', '2-High', '3-Good', None, '4-Medium'],
            'IncomeGroup': [3, 1, 2, None, 5],
            'OwnsMotorcycle': ['Yes', 'No', None, 'Yes', 'No']
        })

    def test_drop_unnecessary_columns(self):
        """
        Test the drop_unnecessary_columns function.
        """
        processed_data = drop_unnecessary_columns(self.sample_data.copy())
        self.assertNotIn('CustomerID', processed_data.columns)
        print("Test passed: CustomerID column was dropped.")

    def test_fill_missing_values(self):
        """
        Test the fill_missing_values function.
        """
        processed_data = fill_missing_values(self.sample_data.copy())
        self.assertFalse(processed_data['MonthlyRevenue'].isnull().any())
        self.assertFalse(processed_data['CreditRating'].isnull().any())
        print("Test passed: Missing values were filled in numerical and categorical columns.")

    def test_encode_categorical_columns(self):
        """
        Test the encode_categorical_columns function.
        """
        processed_data = encode_categorical_columns(self.sample_data.copy())
        self.assertTrue(pd.api.types.is_integer_dtype(processed_data['Churn']))
        self.assertTrue(pd.api.types.is_integer_dtype(processed_data['OwnsMotorcycle']))
        self.assertTrue(pd.api.types.is_integer_dtype(processed_data['CreditRating']))
        print("Test passed: Categorical columns were encoded correctly.")

    def test_preprocess_data(self):
        """
        Test the complete preprocess_data function.
        """
        processed_data = preprocess_data(self.sample_data.copy())
        self.assertNotIn('CustomerID', processed_data.columns)
        self.assertFalse(processed_data.isnull().any().any())  # Ensure no missing values
        print("Test passed: Preprocess data function completed successfully.")

if __name__ == "__main__":
    unittest.main()
