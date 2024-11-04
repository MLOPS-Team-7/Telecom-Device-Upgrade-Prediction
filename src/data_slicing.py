# Code to slice test data for later use (performance evaluation and mitigation of bias)  
import pandas as pd
from src.config import TEST_DATA_PATH

# Slice the data by churn category (0 or 1)
def slice_by_churn(data):
    churn_0 = data[data['Churn'] == 'No']
    churn_1 = data[data['Churn'] == 'Yes']
    return churn_0, churn_1

# Slice the data by a specific column and return a dictionary of DataFrames
def slice_by_column(data, column_name):
    grouped_data = data.groupby(column_name)
    return {group: subset for group, subset in grouped_data}

# Create a new column for binning based on 'CurrentEquipmentDays' and slice by bins
def slice_by_equipment_days(data, bins, labels):
    data['equipment_days_range'] = pd.cut(data['CurrentEquipmentDays'], bins=bins, labels=labels, include_lowest=True)
    grouped_data = data.groupby('equipment_days_range')
    return {label: subset for label, subset in grouped_data}

# Main function to execute the slicing and display results
def main():
    # Load the data
    test_data = pd.read_csv(TEST_DATA_PATH)
    
    # Slice by churn
    churn_0, churn_1 = slice_by_churn(test_data)
    print("Churn 0 slice:")
    print(churn_0.head())
    print("\nChurn 1 slice:")
    print(churn_1.head())
    
    # Slice by income group
    income_group_slices = slice_by_column(test_data, 'IncomeGroup')
    print("\nIncome group slices:")
    for group, data in income_group_slices.items():
        print(f"Income group: {group}")
        print(data.head())
    
    # Slice by credit rating
    credit_rating_slices = slice_by_column(test_data, 'CreditRating')
    print("\nCredit rating slices:")
    for rating, data in credit_rating_slices.items():
        print(f"Credit rating: {rating}")
        print(data.head())
    
    # Define bins and labels for equipment days
    bins = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
    labels = ['0-100', '100-200', '200-300', '300-400', '400-500', '500-600', '600-700', '700-800', '800-900', '900-1000']
    
    # Slice by equipment days range
    equipment_days_range_slices = slice_by_equipment_days(test_data, bins, labels)
    print("\nEquipment days range slices:")
    for label, data in equipment_days_range_slices.items():
        print(f"Equipment days range: {label}")
        print(data.head())

# Run the main function
if __name__ == "__main__":
    main()
