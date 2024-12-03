import pandas as pd
import json

# Load your CSV data into a pandas DataFrame
csv_file = 'ML_Model\holdout_batch_1_features_small_set.csv'  # Path to your input CSV file
df = pd.read_csv(csv_file)

# Convert the DataFrame to JSON format
jsonl_file = 'ML_Model\holdout_batch_1_features_small_set.jsonl'  # Output JSON file path
with open(jsonl_file, 'w') as jsonl_out:
    # Iterate through each row and write it as a JSON object on a new line
    for record in df.to_dict(orient='records'):
        jsonl_out.write(json.dumps(record) + '\n')