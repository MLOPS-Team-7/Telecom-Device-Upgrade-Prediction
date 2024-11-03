import pandas as pd

# Assuming df is your test dataset
# Replace 'churn', 'income_group', 'credit_rating', 'current_equipment_days' with actual column names

# Slicing by churn (0 or 1)
churn_0 = df[df['churn'] == 0]
churn_1 = df[df['churn'] == 1]

# Slicing by different categories of income group
income_groups = df.groupby('income_group')

# Example of creating separate DataFrames for each income group
income_group_slices = {group: data for group, data in income_groups}

# Slicing by different categories of credit rating
credit_rating_groups = df.groupby('credit_rating')

# Example of creating separate DataFrames for each credit rating category
credit_rating_slices = {rating: data for rating, data in credit_rating_groups}

# Slicing by different bins of current equipment days
# Define bins for current equipment days (customize as needed)
bins = [0, 30, 60, 90, 120, 180, 365]  # Adjust the bin edges as necessary
labels = ['0-30', '31-60', '61-90', '91-120', '121-180', '181-365']

# Create a new column in the DataFrame for binning
df['equipment_days_range'] = pd.cut(df['current_equipment_days'], bins=bins, labels=labels, include_lowest=True)

# Group by the newly created bins column
equipment_days_slices = df.groupby('equipment_days_range')

# Example of creating separate DataFrames for each bin range
equipment_days_range_slices = {label: data for label, data in equipment_days_slices}

# Displaying the slices
print("Churn 0 slice:")
print(churn_0.head())
print("\nChurn 1 slice:")
print(churn_1.head())

print("\nIncome group slices:")
for group, data in income_group_slices.items():
    print(f"Income group: {group}")
    print(data.head())

print("\nCredit rating slices:")
for rating, data in credit_rating_slices.items():
    print(f"Credit rating: {rating}")
    print(data.head())

print("\nEquipment days range slices:")
for label, data in equipment_days_range_slices.items():
    print(f"Equipment days range: {label}")
    print(data.head())
