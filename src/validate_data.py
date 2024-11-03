import tensorflow_data_validation as tfdv
import os

# Paths
data_path = 'data/test.csv'  # Update with the path to new data
schema_path = 'data/schema/schema.pbtxt'  # Path to your schema

def validate_data():
    # Check if data and schema files exist
    if not os.path.exists(data_path):
        print(f"Data file not found: {data_path}")
        return

    if not os.path.exists(schema_path):
        print(f"Schema file not found: {schema_path}")
        return

    # Load the schema
    try:
        schema = tfdv.load_schema_text(schema_path)
    except Exception as e:
        print(f"Error loading schema: {e}")
        return
    
    # Generate statistics for the new data
    try:
        new_data_statistics = tfdv.generate_statistics_from_csv(data_path)
    except Exception as e:
        print(f"Error generating statistics: {e}")
        return
    
    # Validate the new data against the schema
    anomalies = tfdv.validate_statistics(statistics=new_data_statistics, schema=schema)
    
    if anomalies.anomaly_info:
        print("Data validation anomalies detected:")
        for anomaly in anomalies.anomaly_info:
            print(f"- Anomaly Type: {anomaly.type} | Details: {anomaly.details}")
    else:
        print("Data validation passed with no anomalies.")

if __name__ == "__main__":
    validate_data()
