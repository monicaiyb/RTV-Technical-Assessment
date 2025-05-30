from dotenv import load_dotenv
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
load_dotenv() 
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
# Set up connection
connect_str = connection_string
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# Get container
container_name = "RTV_Files"
container_client = blob_service_client.get_container_client(container_name)

# Upload file
blob_client = container_client.get_blob_client("cleaned_data.csv")
with open("cleaned_data.csv", "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

print("✅ File uploaded to Azure Blob Storage.")



from sqlalchemy import create_engine

# Replace with your actual connection string
server = 'your_server.database.windows.net'
database = 'your_db'
username = 'your_user'
password = 'your_password'
driver = 'ODBC Driver 17 for SQL Server'

connection_string = f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver}"
engine = create_engine(connection_string)

# Upload DataFrame
df_long.to_sql('longitudinal_data', con=engine, if_exists='replace', index=False)

print("✅ Data uploaded to Azure SQL Database.")
