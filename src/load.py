import os
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = os.getenv("AZURE_CONTAINER_NAME")
local_folder = "Data"

# Set up connection to Azure storage
connect_str = connection_string
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

#Create container if it does not exist
try:
    container_client.create_container()
except Exception:
    pass

# Upload file
for root, _, files in os.walk(local_folder):
    for file in files:
        local_file_path = os.path.join(root, file)
        blob_path = os.path.relpath(local_file_path, start=local_folder)  # Preserve folder structure
        blob_client = container_client.get_blob_client(blob=blob_path)

        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            print(f"Uploaded: {blob_path}")

print("File uploaded to Azure Blob Storage.")




# PostgreSQL config
pg_host = os.getenv("PG_HOST")
pg_port = os.getenv("PG_PORT")
pg_db = os.getenv("PG_DATABASE")
pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASSWORD")
pg_table = os.getenv("PG_TABLE")

# Create PostgreSQL engine
engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

# Connect to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_client = blob_service_client.get_container_client(container_name)

# Extract + Transform
dataframes = []

for blob in container_client.list_blobs():
    if blob.name.endswith('.csv'):
        print(f"⬇️ Downloading: {blob.name}")
        blob_client = container_client.get_blob_client(blob)
        blob_data = blob_client.download_blob().readall()
        
        df = pd.read_csv(BytesIO(blob_data))
        df['source_file'] = blob.name
        dataframes.append(df)

# Combine all data
combined_df = pd.concat(dataframes, ignore_index=True)

# Optional: Clean/transform data here
combined_df.dropna(inplace=True)

# Load to PostgreSQL

combined_df.to_sql(pg_table, con=engine, if_exists='replace', index=False)
