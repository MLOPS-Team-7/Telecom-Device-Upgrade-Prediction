# src/main.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import pandas as pd
from src.config import RAW_DATA_PATH, PROCESSED_DATA_PATH
from src.data_loader import load_data
from src.preprocessing import preprocess_data


def main():
    """
    Main function to run the entire pipeline sequentially from loading, preprocessing, 
    training, and saving the model.
    """
    data = load_data(RAW_DATA_PATH)
    preprocessed_data = preprocess_data(data)
    preprocessed_data.to_csv(PROCESSED_DATA_PATH, index=False)
  

if __name__ == "__main__":
    main()
