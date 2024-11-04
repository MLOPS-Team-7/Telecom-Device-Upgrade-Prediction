# tests/test_data_slicing.py
import sys
import os
os.makedirs("C:/Users/raaga/OneDrive/Desktop/Projects/Telecom-Device-Upgrade-Prediction/data/raw", exist_ok=True)


# Ensure the src folder is on the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import unittest
import pandas as pd
from src.data_slicing import slice_by_churn, slice_by_column, slice_by_equipment_days


class TestDataSlicing(unittest.TestCase):
    def setUp(self):
        """
        Set up a sample DataFrame for testing purposes.
        """
        self.sample_data = pd.DataFrame({
            'CustomerID': [1, 2, 3, 4, 5],
            'Churn': ['No', 'Yes', 'No', 'Yes', 'No'],
            'IncomeGroup': ['Low', 'Medium', 'High', 'Medium', 'Low'],
            'CreditRating': ['Good', 'Excellent', 'Fair', 'Good', 'Poor'],
            'CurrentEquipmentDays': [50, 150, 250, 350, 450]
        })

    def test_slice_by_churn(self):
        """
        Test the slice_by_churn function.
        """
        churn_0, churn_1 = slice_by_churn(self.sample_data)
        self.assertEqual(len(churn_0), 3)  # Check if the number of 'No' churn rows is correct
        self.assertEqual(len(churn_1), 2)  # Check if the number of 'Yes' churn rows is correct

    def test_slice_by_column(self):
        """
        Test the slice_by_column function.
        """
        income_group_slices = slice_by_column(self.sample_data, 'IncomeGroup')
        self.assertIn('Low', income_group_slices)
        self.assertEqual(len(income_group_slices['Low']), 2)  # Check if the number of 'Low' income rows is correct
        self.assertIn('Medium', income_group_slices)
        self.assertEqual(len(income_group_slices['Medium']), 2)  # Check if the number of 'Medium' income rows is correct

    def test_slice_by_equipment_days(self):
        """
        Test the slice_by_equipment_days function.
        """
        bins = [0, 100, 200, 300, 400, 500]
        labels = ['0-100', '100-200', '200-300', '300-400', '400-500']
        equipment_days_range_slices = slice_by_equipment_days(self.sample_data, bins, labels)
        self.assertIn('0-100', equipment_days_range_slices)
        self.assertEqual(len(equipment_days_range_slices['0-100']), 1)  # Check if the number of rows in '0-100' range is correct
        self.assertIn('100-200', equipment_days_range_slices)
        self.assertEqual(len(equipment_days_range_slices['100-200']), 1)  # Check if the number of rows in '100-200' range is correct

if __name__ == '__main__':
    unittest.main()
