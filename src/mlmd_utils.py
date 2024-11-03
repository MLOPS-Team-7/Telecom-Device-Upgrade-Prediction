from ml_metadata import metadata_store
from ml_metadata.proto import metadata_store_pb2

# Initialize connection to MLMD store
def get_mlmd_store():
    connection_config = metadata_store_pb2.ConnectionConfig()
    connection_config.sqlite.filename_uri = 'data/mlmd.db'  # or use a persistent location
    connection_config.sqlite.connection_mode = metadata_store_pb2.ConnectionConfig.SQLITE
    return metadata_store.MetadataStore(connection_config)

def log_schema(schema_path):
    store = get_mlmd_store()
    schema_artifact = metadata_store_pb2.Artifact()
    schema_artifact.uri = schema_path
    schema_artifact.type_id = store.get_artifact_type("Schema").id
    store.put_artifacts([schema_artifact])
    print(f"Schema logged in MLMD at {schema_path}")
    
# Call this in `generate_schema.py` after creating the schema log_schema(output_schema_path)