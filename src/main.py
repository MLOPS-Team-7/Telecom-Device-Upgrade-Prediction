# src/main.py
import sys
import os
import pandas as pd
from config import RAW_DATA_PATH, PROCESSED_DATA_PATH
from config import CHURN_FEATURES_PATH, DEVICE_UPGRADE_FEATURES_PATH
from data_loader import load_data
from preprocessing import preprocess_data
from feature_engineering import find_optimal_k, select_best_k_features, create_device_upgrade_subset

def main():
    """
    Main function to run the entire pipeline sequentially from loading, preprocessing, 
    training, and saving the model.
    """
    data = load_data(RAW_DATA_PATH)
    preprocessed_data = preprocess_data(data)
    preprocessed_data.to_csv(PROCESSED_DATA_PATH, index=False)
    processed_data = pd.read_csv(PROCESSED_DATA_PATH)
    #Feature engineering
    target_column = 'Churn'
    find_optimal_k(processed_data, target_column, k_range=range(25, 31))
    best_features_churn = select_best_k_features(processed_data, target_column)
    best_features_device_upgrade = create_device_upgrade_subset(processed_data)
    best_features_churn.to_csv(CHURN_FEATURES_PATH, index=False) 
    best_features_device_upgrade.to_csv(DEVICE_UPGRADE_FEATURES_PATH, index=False)

if __name__ == "__main__":
    main()
