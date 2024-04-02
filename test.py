import azure.functions as func
import datetime
import json
import logging
import requests
import pandas as pd


import azure.functions as func
import datetime
import json
import logging
import requests
import pandas as pd
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential

def azure_upload_df(container, dataframe, filename):
    #connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
 
    filename = 'world_bank_file'
    file_path = 'jsonpath'
    #upload_file_path = os.path.join(file_path, f"{filename}.csv")
    upload_file_path = os.path.join(file_path, f"{filename}.csv")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(
        container=container, 
        blob=upload_file_path
        )
    output = dataframe.to_csv(index=False, encoding="utf-8")
    blob_client.upload_blob(output, blob_type="BlockBlob")

def azure_upload_json(container, data, filename):
    #connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    current_datetime = datetime.now()
    filename = 'world_bank_file'
    file_path = 'jsonpath'
    #upload_file_path = os.path.join(file_path, f"{filename}.csv")
    upload_file_path = os.path.join(file_path, f"{filename}_{current_datetime}.json")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(
        container=container, 
        blob=upload_file_path
        )
    #output = dataframe.to_csv(index=False, encoding="utf-8")
    blob_client.upload_blob(data, blob_type="BlockBlob")

def http_trigger():
    url = 'http://api.worldbank.org/v2/countries/br;cn;us;de/indicators/SP.POP.TOTL/?format=json&per_page=1000'
    r = requests.get(url)
    json_file = r.text
    json_data = json.loads(json_file)
    x = json_data[1]
    df = pd.DataFrame(x)    

   
    #service = DataLakeServiceClient.from_connection_string(conn_str=conn_string)
    
    #file_system_client = service_client.create_file_system(file_system="createdfilesystem")

    #service_client = DataLakeServiceClient(account_url, credential=sas_token)
   
    #azure_upload_json(container="conteinertest", data=json_file, filename='testfilename')
    current_datetime = datetime.now()
    filename = 'world_bank_file'
    file_path = 'jsonpath'
    #upload_file_path = os.path.join(file_path, f"{filename}.csv")
    upload_file_path = os.path.join(file_path, f"{filename}_{current_datetime}.json")
    #upload_file_path = os.path.join(file_path, f"{filename}.csv")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(
        container='conteinertest', 
        blob=upload_file_path
        )
    #output = dataframe.to_csv(index=False, encoding="utf-8")
    blob_client.upload_blob(data=json_file, blob_type="BlockBlob")

    print("file_system_client")

http_trigger()