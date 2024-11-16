#Device Upgrade conditions
# churn model predictions are fed to this script (modifications neccessary based on the final churn model)
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
