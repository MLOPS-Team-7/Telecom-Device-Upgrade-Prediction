import os
import pandas as pd
import json

def convert_csv_to_jsonl(folder_path):
    # List all files in the folder
    files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    for file in files:
        csv_path = os.path.join(folder_path, file)
        jsonl_path = os.path.join(folder_path, os.path.splitext(file)[0] + '.jsonl')
        
        try:
            # Read CSV into a DataFrame
            df = pd.read_csv(csv_path)
            
            # Write each row as a JSON object in JSONL format
            with open(jsonl_path, 'w') as jsonl_file:
                for _, row in df.iterrows():
                    jsonl_file.write(json.dumps(row.to_dict()) + '\n')
            
            print(f"Converted: {file} -> {os.path.basename(jsonl_path)}")
        except Exception as e:
            print(f"Error processing {file}: {e}")

# Replace with the path to your folder containing CSV files
folder_path = "ML_Model\Hold out folder"
convert_csv_to_jsonl(folder_path)
