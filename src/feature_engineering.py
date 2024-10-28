import pandas as pd
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.model_selection import cross_val_score
from sklearn.ensemble import RandomForestClassifier

def find_optimal_k(data, target_column, k_range=range(25, 31)):
    """
    Finds the optimal number of top features (k) in the specified range for SelectKBest 
    by evaluating the sum of f_classif scores for each k.

    Parameters:
    data (pd.DataFrame): The pre-processed dataset.
    target_column (str): The name of the target column.
    k_range (range): The range of k values to evaluate.

    Returns:
    int: The optimal value of k.
    """
    X = data.drop(columns=[target_column])
    y = data[target_column]

    best_k = k_range.start
    best_score_sum = 0

    for k in k_range:
        selector = SelectKBest(score_func=f_classif, k=k)
        X_new = selector.fit_transform(X, y)
        score_sum = selector.scores_[selector.get_support()].sum()  # Sum of selected features' scores

        if score_sum > best_score_sum:
            best_k = k
            best_score_sum = score_sum

    print(f"Optimal k found: {best_k}")
    return best_k

def select_best_k_features(data, target_column):
    """
    Selects the top k features based on the optimal k value from SelectKBest 
    for predicting the target variable.

    Parameters:
    data (pd.DataFrame): The pre-processed dataset.
    target_column (str): The name of the target column.

    Returns:
    pd.DataFrame: DataFrame containing the selected features.
    """
    # Find optimal k
    k = find_optimal_k(data, target_column)
    
    # Separate target and features
    X = data.drop(columns=[target_column])
    y = data[target_column]
    
    # Select top k features
    selector = SelectKBest(score_func=f_classif, k=k)
    selector.fit_transform(X, y)
    selected_feature_names = X.columns[selector.get_support(indices=True)]
    
    # Create a DataFrame of selected features
    best_k_features_df = data[selected_feature_names]
    
    print(f"Top {k} features for churn selected.")
    return best_k_features_df

def create_device_upgrade_subset(data):
    """
    Creates a subset for non-churned customers, adds a device-upgrade column based on specified rules, 
    and returns the subset DataFrame.

    Parameters:
    data (pd.DataFrame): The dataset including churn and other relevant features.

    Returns:
    pd.DataFrame: DataFrame containing the device upgrade subset.
    """
    # Filter customers who did not churn
    non_churned_customers = data[data['Churn'] == 0].copy()
    
    # Define the conditions for device upgrade
    upgrade_conditions = (
        (non_churned_customers['MonthlyMinutes'] > 3000) &
        (non_churned_customers['RetentionCalls'] > 2) &
        (non_churned_customers['RetentionOffersAccepted'] > 0) &
        (non_churned_customers['HandsetWebCapable'] == 0) &
        (non_churned_customers['HandsetRefurbished'] == 1) &
        (non_churned_customers['CurrentEquipmentDays'] > 340) &
        (non_churned_customers['CreditRating'] > 5) &
        (non_churned_customers['MadeCallToRetentionTeam'] == 1) &
        (non_churned_customers['RespondsToMailOffers'] == 1)
    )
    
    # Create the 'device-upgrade' column
    non_churned_customers['DeviceUpgrade'] = upgrade_conditions.astype(int)
    
    # Select only the necessary columns for the final subset
    selected_columns = [
        'MonthlyMinutes', 'RetentionCalls', 'RetentionOffersAccepted', 'HandsetWebCapable',
        'HandsetRefurbished', 'CurrentEquipmentDays', 'CreditRating', 'MadeCallToRetentionTeam',
        'RespondsToMailOffers', 'DeviceUpgrade'
    ]
    device_upgrade_subset_df = non_churned_customers[selected_columns]

    print(f"Device upgrade subset created.")
    return device_upgrade_subset_df

def main():
    """
    Main function to run the feature engineering and other processes.
    """
    # Load the dataset (adjust path as necessary)
    data_path = r'C:\Users\A V NITHYA\MLOpsProject\Telecom-Device-Upgrade-Prediction\data\processed\train_processed.csv'
    data = pd.read_csv(data_path)
    target_column = 'Churn'

    # Run feature engineering functions and capture the returned DataFrames
    find_optimal_k(data, target_column, k_range=range(25, 31))
    best_k_features_df = select_best_k_features(data, target_column)
    device_upgrade_subset_df = create_device_upgrade_subset(data)

    print(best_k_features_df.head(5))
    print(device_upgrade_subset_df.head(5))

    print("Feature Engineering Completed")

if __name__ == "__main__":
    main()