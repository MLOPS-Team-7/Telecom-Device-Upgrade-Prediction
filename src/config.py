# src/config.py

import os

# Paths to data files and directories #adjust accordingly 
RAW_DATA_PATH = r'C:\Users\A V NITHYA\Downloads\train.csv'
PROCESSED_DATA_PATH = r'C:\Users\A V NITHYA\MLOpsProject\Telecom-Device-Upgrade-Prediction\data\processed\train_processed.csv'
CHURN_FEATURES_PATH = r'C:\Users\A V NITHYA\MLOpsProject\Telecom-Device-Upgrade-Prediction\data\processed\best_features_for_churn.csv'
DEVICE_UPGRADE_FEATURES_PATH = r'C:\Users\A V NITHYA\MLOpsProject\Telecom-Device-Upgrade-Prediction\data\processed\best_features_for_device_upgrade.csv'

MODEL_SAVE_PATH = 'models/trained_model.pkl'

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
