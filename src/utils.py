# src/utils.py

import pickle
import logging
from src.config import MODEL_SAVE_PATH

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_model(model, path=MODEL_SAVE_PATH):
    """
    Save the trained model to disk.

    Args:
        model: The trained machine learning model.
        path (str): File path to save the model.
    """
    with open(path, 'wb') as file:
        pickle.dump(model, file)
    logging.info(f"Model saved to {path}")

def load_model(path=MODEL_SAVE_PATH):
    """
    Load a trained model from disk.

    Args:
        path (str): File path to load the model from.
        
    Returns:
        The loaded model.
    """
    with open(path, 'rb') as file:
        model = pickle.load(file)
    logging.info(f"Model loaded from {path}")
    return model

def main():
    """
    Main function to test saving and loading a dummy model.
    """
    dummy_model = {"model": "dummy"}  # For testing purposes
    save_model(dummy_model)
    loaded_model = load_model()
    print(f"Loaded model: {loaded_model}")

if __name__ == "__main__":
    main()
