# tests/test_feature_engineering.py
import sys
import os
# Ensure the src folder is on the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import unittest
import pandas as pd
from unittest.mock import patch, MagicMock



from src.feature_engineering import find_optimal_k, select_best_k_features, create_device_upgrade_subset

class TestFeatureEngineering(unittest.TestCase):

    def setUp(self):
        """
        Set up a sample DataFrame for testing purposes.
        """
        self.sample_data = pd.DataFrame({
            'Churn': [0, 1, 0, 1, 0],
            'MonthlyMinutes': [2500, 3200, 3100, 2900, 3050],
            'RetentionCalls': [1, 3, 2, 1, 4],
            'RetentionOffersAccepted': [0, 1, 1, 0, 1],
            'HandsetWebCapable': [1, 0, 0, 1, 0],
            'HandsetRefurbished': [1, 1, 0, 0, 1],
            'CurrentEquipmentDays': [400, 350, 200, 500, 360],
            'CreditRating': [6, 7, 4, 8, 5],
            'MadeCallToRetentionTeam': [0, 1, 1, 0, 1],
            'RespondsToMailOffers': [0, 1, 1, 0, 1]
        })
        self.target_column = 'Churn'

    def test_find_optimal_k(self):
        """
        Test the find_optimal_k function.
        """
        optimal_k = find_optimal_k(self.sample_data, self.target_column, k_range=range(25, 30))
        self.assertTrue(optimal_k > 0)
        print(f"Test passed: Optimal k found is {optimal_k}.")

    def test_select_best_k_features(self):
        """
        Test the select_best_k_features function.
        """
        best_k_features_df = select_best_k_features(self.sample_data, self.target_column)
        self.assertFalse(best_k_features_df.empty)
        print("Test passed: select_best_k_features produced a non-empty DataFrame.")

    def test_create_device_upgrade_subset(self):
        """
        Test the create_device_upgrade_subset function.
        """
        device_upgrade_subset_df = create_device_upgrade_subset(self.sample_data)
        self.assertIn('DeviceUpgrade', device_upgrade_subset_df.columns)
        self.assertFalse(device_upgrade_subset_df.empty)
        print("Test passed: create_device_upgrade_subset produced a DataFrame with 'DeviceUpgrade' column.")

if __name__ == "__main__":
    unittest.main()
