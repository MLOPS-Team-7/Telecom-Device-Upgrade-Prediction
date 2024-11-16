# src/main.py
import sys
import os
import pandas as pd
from config import RAW_DATA_PATH, PROCESSED_DATA_PATH, TEST_DATA_PATH, PROCESSED_TEST_PATH, CHURN_TEST_PATH
from config import CHURN_FEATURES_PATH, DEVICE_UPGRADE_FEATURES_PATH
from data_loader import load_data
from preprocessing import preprocess_data
from upload_to_gcs import upload_to_gcs 
from feature_engineering import find_optimal_k, select_best_k_features, transform_and_save_test_features

def main():
    """
    Main function to run the entire pipeline sequentially from loading, preprocessing, 
    training, and saving the model.
    """
    data = load_data(TEST_DATA_PATH)
    preprocessed_data = preprocess_data(data)
    preprocessed_data.to_csv(PROCESSED_TEST_PATH, index=False)
     
    processed_data = pd.read_csv(PROCESSED_TEST_PATH)
    
    # Feature engineering
    target_column = 'Churn'
    #find_optimal_k(processed_data, target_column, k_range=range(25, 31))
    #best_features_churn = select_best_k_features(processed_data, target_column)
    best_features_test = transform_and_save_test_features(r'C:\Users\A V NITHYA\MLOpsProject\Telecom-Device-Upgrade-Prediction\data\processed\best_features_for_churn.csv', PROCESSED_TEST_PATH, target_column)
    best_features_test.to_csv(CHURN_TEST_PATH, index=False)
    #best_features_churn.to_csv(CHURN_FEATURES_PATH, index=False)
    upload_to_gcs(CHURN_TEST_PATH, 'data-source-telecom-customers', 'data/clean_data/test_clean/best_features_test.csv')
    print('Features for churn pushed to GCS bucket')
    # Uncomment and complete for device upgrade features
    # best_features_device_upgrade.to_csv(DEVICE_UPGRADE_FEATURES_PATH, index=False)
    # upload_to_gcs(DEVICE_UPGRADE_FEATURES_PATH, GCS_BUCKET_NAME, 'device_upgrade_features.csv')


if __name__ == "__main__":
    main()
