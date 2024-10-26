# src/preprocessing.py

import pandas as pd
from sklearn.preprocessing import LabelEncoder

def drop_unnecessary_columns(data):
    """
    Drop columns that are not needed for modeling.
    
    Args:
        data (pd.DataFrame): The input dataset.
        
    Returns:
        pd.DataFrame: The dataset with unnecessary columns dropped.
    """
    columns_to_drop = ['CustomerID', 'ServiceArea']  # Adjust as necessary
    data.drop(columns=columns_to_drop, inplace=True, errors='ignore')
    return data

def fill_missing_values(data):
    """
    Fill missing values in numerical columns based on churn vs. non-churn groups.

    Args:
        data (pd.DataFrame): The input dataset with missing values.
        
    Returns:
        pd.DataFrame: The dataset with filled missing values.
    """
    numerical_columns = ['MonthlyRevenue', 'MonthlyMinutes', 'TotalRecurringCharge', 'DirectorAssistedCalls',
                         'OverageMinutes', 'RoamingCalls', 'PercChangeMinutes', 'PercChangeRevenues', 
                         'DroppedCalls', 'BlockedCalls', 'UnansweredCalls', 'CustomerCareCalls', 
                         'ThreewayCalls', 'ReceivedCalls', 'OutboundCalls', 'InboundCalls', 'PeakCallsInOut', 
                         'OffPeakCallsInOut', 'DroppedBlockedCalls', 'CallForwardingCalls', 'CallWaitingCalls', 
                         'MonthsInService', 'UniqueSubs', 'ActiveSubs', 'Handsets', 'HandsetModels', 
                         'CurrentEquipmentDays', 'AgeHH1', 'AgeHH2', 'RetentionCalls', 'RetentionOffersAccepted', 
                         'ReferralsMadeBySubscriber', 'AdjustmentsToCreditRating']

    # Fill missing values based on churn vs non-churn groups
    for column in numerical_columns:
        if column in data.columns:
            data[column] = data.groupby('Churn')[column].transform(lambda x: x.fillna(x.median()))

    return data

def encode_categorical_columns(data):
    """
    Encode categorical columns using Label Encoding for multi-class columns
    and One-Hot Encoding for binary columns.

    Args:
        data (pd.DataFrame): The dataset with categorical columns.
        
    Returns:
        pd.DataFrame: The dataset with encoded categorical columns.
    """
    # One-Hot Encode binary categorical columns
    binary_columns = ['Churn', 'OwnsMotorcycle', 'HandsetRefurbished', 'HandsetWebCapable', 'TruckOwner', 
                      'RVOwner', 'Homeownership', 'BuysViaMailOrder', 'RespondsToMailOffers', 'OptOutMailings', 
                      'NonUSTravel', 'OwnsComputer', 'HasCreditCard', 'NewCellphoneUser', 'NotNewCellphoneUser', 
                      'MadeCallToRetentionTeam']
    
    for column in binary_columns:
        if column in data.columns:
            data[column] = pd.get_dummies(data[column], drop_first=True)
    
    # Label Encode multi-class categorical columns
    label_encoder = LabelEncoder()
    multi_class_columns = ['CreditRating', 'PrizmCode', 'Occupation', 'MaritalStatus', 'IncomeGroup', 'HandsetPrice']
    for column in multi_class_columns:
        if column in data.columns:
            data[column] = label_encoder.fit_transform(data[column].astype(str))
    
    return data

def preprocess_data(data):
    """
    Preprocess the dataset by handling missing values, encoding categorical features,
    and dropping unnecessary columns.
    
    Args:
        data (pd.DataFrame): The input dataset to preprocess.
        
    Returns:
        pd.DataFrame: The preprocessed dataset ready for modeling.
    """
    # Drop unnecessary columns
    data = drop_unnecessary_columns(data)
    
    # Fill missing values
    data = fill_missing_values(data)
    
    # Encode categorical columns
    data = encode_categorical_columns(data)
    
    print("Data preprocessing completed")
    return data

def main():
    """
    Main function to test the preprocessing steps.
    """
    # Load the dataset (adjust path as necessary)
    data_path = 'data/raw/train.csv'  # Adjust path as needed
    data = pd.read_csv(data_path)
    
    # Apply preprocessing steps
    preprocessed_data = preprocess_data(data)
    
    # Display first few rows of the preprocessed data for verification
    print(preprocessed_data.head())

if __name__ == "__main__":
    main()
