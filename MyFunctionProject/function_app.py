import azure.functions as func
from datetime import datetime
import json
import logging
import requests
import os
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential

app = func.FunctionApp()
@app.route(route="http_trigger", auth_level=func.AuthLevel.FUNCTION)
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    url = 'http://api.worldbank.org/v2/countries/br;cn;us;de/indicators/SP.POP.TOTL/?format=json&per_page=1000'
    r = requests.get(url)
    json_file = r.text
   
    connect_str = ""
    current_datetime = datetime.now()
    filename = 'world_bank_file'
    file_path = 'jsonpath'
    full_name = f"{file_path}_{current_datetime}.json"

    upload_file_path = os.path.join(file_path, full_name)
    
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(
        container='conteinertest', 
        blob=upload_file_path
        )

    blob_client.upload_blob(data=json_file, blob_type="BlockBlob")

    return func.HttpResponse(
             f"{file_path}",
             status_code=200
            )