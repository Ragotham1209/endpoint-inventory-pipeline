import azure.functions as func
import logging
import json
import uuid
import os
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="InventoryIngest", methods=["POST"])
def InventoryIngest(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('InventoryIngest triggered.')

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON payload.", status_code=400)

    endpoint_id = req_body.get('EndpointId')
    if not endpoint_id:
        return func.HttpResponse("Missing 'EndpointId'", status_code=400)
        
    payload_type = req_body.get('PayloadType', 'unknown').lower()

    # Add ingestion metadata
    now = datetime.utcnow()
    req_body['IngestionTimestamp'] = now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    req_body['MessageId'] = str(uuid.uuid4())

    year, month, day = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')
    file_name = f"{endpoint_id}_{req_body['MessageId']}.json"

    # Save to Azure Data Lake Gen2 using Managed Identity
    adls_account = os.environ.get('ADLS_ACCOUNT_NAME')
    
    if adls_account:
        try:
            account_url = f"https://{adls_account}.dfs.core.windows.net"
            # DefaultAzureCredential uses the Function App's System-Assigned Managed Identity
            credential = DefaultAzureCredential()
            service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
            
            # The Bicep script created a file system (container) named 'raw'
            file_system_client = service_client.get_file_system_client("raw")
            
            # Define the directory path (e.g., hardware/2026/02/25)
            directory_path = f"{payload_type}/{year}/{month}/{day}"
            directory_client = file_system_client.get_directory_client(directory_path)
            
            # Ensure the directory exists (doesn't hurt if it already does)
            try:
                directory_client.create_directory()
            except ResourceExistsError:
                pass
                
            # Create the file and upload the JSON
            file_client = directory_client.get_file_client(file_name)
            json_bytes = json.dumps(req_body).encode('utf-8')
            file_client.upload_data(json_bytes, overwrite=True)
            
            logging.info(f"Successfully saved to ADLS: {directory_path}/{file_name}")
            return func.HttpResponse("Payload accepted and saved securely to ADLS Gen2.", status_code=202)
            
        except Exception as e:
            logging.error(f"Failed to write to ADLS Gen2: {e}")
            return func.HttpResponse("Internal Server Error: Data Lake unavailable.", status_code=500)
    else:
        # Fallback for local testing if ADLS_ACCOUNT_NAME isn't set
        local_landing_zone = os.environ.get("LOCAL_LANDING_ZONE", "C:/Temp/raw_lake")
        landing_path = os.path.join(local_landing_zone, payload_type, year, month, day)
        os.makedirs(landing_path, exist_ok=True)
        with open(os.path.join(landing_path, file_name), "w") as f:
            json.dump(req_body, f, indent=4)
        return func.HttpResponse("Saved to local mock landing zone.", status_code=202)
