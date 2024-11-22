# code to test locally, the automated dag has all these parts
import pandas as pd
def transform_and_save_holdout_features(train_features_path, holdout_path, target_column):
    """
    Transforms the test data to have the same selected features as the training data 
    and saves the selected features and target column as a CSV file.

    Parameters:
    train_features_path (str): Path to the CSV file with selected training features.
    test_data_path (str): Path to the CSV file containing the raw test data.
    target_column (str): The name of the target column.
    output_path (str): Path where the selected test features CSV will be saved.
    """
    # Load selected features from training features CSV
    train_features_df = pd.read_csv(train_features_path)
    selected_feature_names = train_features_df.drop(columns=[target_column]).columns  # Exclude target column
    
    # Load the test data
    holdout_data = pd.read_csv(holdout_path)
    
    # Select only the columns present in selected_feature_names
    best_k_features_holdout = holdout_data[selected_feature_names]
    
    return best_k_features_holdout 

def main():
    """
    Main function to test the transform_and_save_holdout_features function.
    """
    # Define the file paths (change these as per your actual files)
    train_features_path = r'C:\Users\A V NITHYA\MLOpsProject\Telecom-Device-Upgrade-Prediction\data\processed\best_features_for_churn.csv'  # Path to training feature file
    holdout_path = r'C:\Users\A V NITHYA\MLOpsProject\Telecom-Device-Upgrade-Prediction\data\processed\holdout_batch_4_processed.csv'  # Path to test data file
    target_column = 'Churn'  # Replace with your target column name

    # Call the function to transform and save features from the test data
    transformed_holdout_data = transform_and_save_holdout_features(train_features_path, holdout_path, target_column)
    
    # Optionally, save the result to a CSV file
    output_path = r'C:\Users\A V NITHYA\MLOpsProject\Telecom-Device-Upgrade-Prediction\data\processed\holdout_batch_4_features.csv'  # Change path as needed
    transformed_holdout_data.to_csv(output_path, index=False)
    
    print(f"Transformed data has been saved to {output_path}")


if __name__ == "__main__":
    main()