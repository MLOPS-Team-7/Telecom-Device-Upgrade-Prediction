# src/data_loader.py

import pandas as pd
import os
import sys
from config import RAW_DATA_PATH, PROCESSED_DATA_PATH

def load_data(file_path=RAW_DATA_PATH): #adjust path as needed
    """
    Load the dataset from the specified file path, using DVC if the file is not found locally.

    Args:
        file_path (str): Path to the dataset file.

    Returns:
        pd.DataFrame: Loaded dataset as a DataFrame, or None if an error occurs.
    """
    if not os.path.exists(file_path):
        print("Data file not found locally, pulling data using DVC...")
        os.system(f'dvc pull {file_path}')

    try:
        data = pd.read_csv(file_path)
        print("Data loaded successfully")
        return data
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def main():
    """
    Main function to test data loading from a specified file path.
    """
    data = load_data()
    if data is not None:
        print(data.head())  # Display first few rows for verification

if __name__ == "__main__":
    main()
