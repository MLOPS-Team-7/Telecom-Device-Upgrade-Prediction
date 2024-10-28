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
    columns_to_drop = ['CustomerID']  #customerID not relevant for prediction
    data.drop(columns=columns_to_drop, inplace=True, errors='ignore')
    return data

def fill_missing_values(data):
    """
    Fill missing values in numerical columns based on churn vs. non-churn groups,
    and fill missing values in categorical columns with the mode of each churn group.

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
    
    categorical_columns = ['Churn', 'OwnsMotorcycle', 'HandsetRefurbished', 'HandsetWebCapable', 'TruckOwner', 
                      'RVOwner', 'Homeownership', 'BuysViaMailOrder', 'RespondsToMailOffers', 'OptOutMailings', 
                      'NonUSTravel', 'OwnsComputer', 'HasCreditCard', 'NewCellphoneUser', 'NotNewCellphoneUser', 
                      'MadeCallToRetentionTeam','ChildrenInHH','CreditRating', 'PrizmCode', 'Occupation', 'MaritalStatus', 'IncomeGroup', 'HandsetPrice','ServiceArea']  

    # Fill missing values in numerical columns based on churn vs non-churn groups
    for column in numerical_columns:
        if column in data.columns:
            data[column] = data.groupby('Churn')[column].transform(lambda x: x.fillna(x.median()))
    
    # Fill missing values in categorical columns with the mode of each churn group
    for column in categorical_columns:
        if column in data.columns:
            data[column] = data.groupby('Churn')[column].transform(lambda x: x.fillna(x.mode()[0] if not x.mode().empty else None))
    #print(data.head(10))
    return data


def encode_categorical_columns(data):
    
    """Encode categorical columns using Label Encoding for multi-class columns
    and One-Hot Encoding for binary columns. Custom dictionaries are used for
    ordered encoding of CreditRating and IncomeGroup.
    
    Args:
        data (pd.DataFrame): The dataset with categorical columns.
        
    Returns:
        pd.DataFrame: The dataset with encoded categorical columns.
    """
    # Custom encoding dictionaries
    credit_rating_mapping = {'1-Highest': 1, '2-High': 2, '3-Good': 3, '4-Medium': 4, '5-Low': 5, '6-VeryLow': 6, '7-Lowest': 7}
    income_group_mapping = {0:0, 1:1, 2:2, 3:3, 4:4, 5:5, 6:6, 7:7, 8:8, 9:9} 

    # One-Hot Encode binary categorical columns in a single step
    binary_columns = ['Churn', 'OwnsMotorcycle', 'HandsetRefurbished', 'HandsetWebCapable', 'TruckOwner', 
                      'RVOwner', 'BuysViaMailOrder', 'RespondsToMailOffers', 'OptOutMailings', 
                      'NonUSTravel', 'OwnsComputer', 'HasCreditCard', 'NewCellphoneUser', 'NotNewCellphoneUser', 
                      'MadeCallToRetentionTeam','ChildrenInHH']
    for column in binary_columns:
        if column in data.columns:
        # Replace 'Yes' with 1 and 'No' with 0
            data[column] = data[column].replace({'Yes': 1, 'No': 0}).astype(int)

        
    # Map CreditRating and IncomeGroup using predefined dictionaries
    if 'CreditRating' in data.columns:
        data['CreditRating'] = data['CreditRating'].map(credit_rating_mapping)
    
    if 'IncomeGroup' in data.columns:
        data['IncomeGroup'] = data['IncomeGroup'].map(income_group_mapping)
    
    # Label Encode other multi-class categorical columns
    label_encoder = LabelEncoder()
    multi_class_columns = ['Homeownership','PrizmCode', 'Occupation', 'MaritalStatus', 'HandsetPrice', 'ServiceArea']
    
    for column in multi_class_columns:
        if column in data.columns:
            data[column] = label_encoder.fit_transform(data[column].astype(str))
    
    return data

   
def preprocess_data(data):

    """ Preprocess the dataset by handling missing values, encoding categorical features,
    and dropping unnecessary columns.
    
    Args:
        data (pd.DataFrame): The input dataset to preprocess.
        
    Returns:
        pd.DataFrame: The preprocessed dataset ready for modeling.
    """
    # Drop unnecessary columns
    data = drop_unnecessary_columns(data)
    
    #Fill missing values
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
    data_path = r'C:\Users\A V NITHYA\Downloads\train.csv'  # Adjust path as needed
    data = pd.read_csv(data_path)
    
    # Apply preprocessing steps
    preprocessed_data = preprocess_data(data)
    
    # Display first few rows of the preprocessed data for verification
    print(preprocessed_data.head())

if __name__ == "__main__":
    main()
