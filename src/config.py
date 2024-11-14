# src/config.py
import os

# Dynamically get the base directory (parent of the current script's location)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Paths to data files and directories (relative to BASE_DIR)
RAW_DATA_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'train.csv')
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'train_processed.csv')
PROCESSED_TEST_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'test_processed.csv')
CHURN_FEATURES_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'best_features_for_churn.csv')
CHURN_TEST_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'best_features_test.csv')
DEVICE_UPGRADE_FEATURES_PATH = os.path.join(BASE_DIR, 'data', 'processed', 'best_features_for_device_upgrade.csv')
TEST_DATA_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'test.csv')
MODEL_SAVE_PATH = os.path.join(BASE_DIR, 'models', 'trained_model.pkl')

# Ensure all necessary directories exist
required_dirs = [
    os.path.join(BASE_DIR, 'data', 'raw'),
    os.path.join(BASE_DIR, 'data', 'processed'),
    os.path.join(BASE_DIR, 'models')
]

for directory in required_dirs:
    os.makedirs(directory, exist_ok=True)


# Model parameters
MODEL_PARAMS = {
    'decision_tree': {
        'max_depth': 5,
        'random_state': 42
    }
}

# Other constants
SEED = 42
TEST_SIZE = 0.2

def ensure_directories():
    """
    Ensure that necessary directories for processed data and models exist.
    Creates directories if they are not already present.
    """
    os.makedirs(os.path.dirname(PROCESSED_DATA_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(MODEL_SAVE_PATH), exist_ok=True)

def main():
    """
    Main function to ensure that necessary directories exist for storing processed data and models.
    """
    ensure_directories()
    print("Directories ensured.")

if __name__ == "__main__":
    main()
