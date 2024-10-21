# connector/blob.py
from azure.storage.blob import BlobServiceClient
# from src.configuration.configuration import connect_str, container_name_blob
from dotenv import load_dotenv
import os

load_dotenv()

connect_str = os.getenv('connect_str')
container_name_blob = os.getenv('container_name_blob')

def upload_to_blob(csv_data, actual_date_str):
    
        # Create a filename with the actual date
        file_name = f"Harti_data_{actual_date_str}.csv"
        print(f"File name to upload: {file_name}")

        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        print("Connected to Azure Blob Storage")

        # Create a blob client
        blob_client = blob_service_client.get_blob_client(container=container_name_blob, blob=file_name)
        print(f"Created blob client for container: {container_name_blob}")

        # Upload the new CSV data to the blob
        blob_client.upload_blob(csv_data.encode('utf-8'), overwrite=True)
        print(f"Uploaded {file_name} to Azure Blob Storage")
