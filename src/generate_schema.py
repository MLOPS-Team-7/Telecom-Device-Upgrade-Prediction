import tensorflow_data_validation as tfdv

# Path to your dataset (adjust as necessary)
data_path = 'data/raw/train.csv'
output_schema_path = 'data/schema/schema.pbtxt'

def generate_schema():
    # Generate statistics from the dataset
    statistics = tfdv.generate_statistics_from_csv(data_path)
    
    # Infer schema from the statistics
    schema = tfdv.infer_schema(statistics)
    
    # Save the schema for future validation
    tfdv.write_schema_text(schema, output_schema_path)
    print("Schema generated and saved.")

if __name__ == "__main__":
    generate_schema()