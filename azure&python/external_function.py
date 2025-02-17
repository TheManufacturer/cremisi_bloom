import msal
import os
import logging
import json
import requests
import auth

#Dataverse URL eg: https://<orgname>.crm.dynamics.com
dataverse_url = os.getenv("DATAVERSE_URL")

api_version = "v9.2" #API Version

#Table Name
table_name_repo = os.getenv("TABLE_NAME")
primary_column_repo = os.getenv("PRIMARY_COLUMN")

#please change the below values to match your dataverse schema -logical name
# "logical_name": "column_name"
dataverse_personal_fields = {
    "fullname",
    "emailaddress1",
    "email"
}

access_token_dataverse = auth.acquire_dataverse_token()

def dataverse_read_all(access_token_dataverse):
    access_token_dataverse = auth.acquire_dataverse_token()
    headers = auth.create_headers(access_token_dataverse)

    existing_records = {}
    select_attr = ",".join(dataverse_personal_fields)

    query_url = f"{auth.dataverse_url}/api/data/{auth.api_version}/{auth.table_name_repo}?$select={select_attr}"

    logging.info(f"Query URL: {query_url}")

    while query_url:
        response = requests.get(query_url, headers=headers)
        logging.info(f"Response: {response.status_code} - {response.text}")

        if response.status_code == 200:
            result = response.json()
            value_list = result.get("value", [])

            for record in value_list:
                assett_name = record.get(auth.primary_column_repo)
                if assett_name:
                    existing_records[assett_name] = record
            
            if "@odata.nextLink" in result:
                query_url = result["@odata.nextLink"]
            else:
                query_url = None
        else:
            logging.error(f"Failed to read records from Dataverse: {response.status_code}- {response.text}")
            break

def post_force_dataverse(access_token_dataverse):
    
    access_token_dataverse = auth.acquire_dataverse_token()
    headers = auth.create_headers(access_token_dataverse)

    #Insert logical name of the columns
    data = {
        "fullname": "Marilyn",
        "middlename": "Monroe",
        "emailaddress1": "email_test@mail.com"
    }

    query_url = f"{auth.dataverse_url}/api/data/{auth.api_version}/{auth.table_name_repo}"

    response = requests.post(query_url, headers=headers, json=data)
    logging.info(f"Response: {response.status_code} - {response.text}")

    if response.status_code in [201, 204]:
        return {"status" : "success", "message": "Record created successfully"}
    else:
        logging.error(f"Failed to create record in Dataverse: {response.status_code}- {response.text}")
        return False
    

def post_active_input_dataverse(nome, cognome, email):
    access_token_dataverse = auth.acquire_dataverse_token()
    headers = auth.create_headers(access_token_dataverse)

    data = {
        "name": nome,
        "middlename": cognome,
        "emailaddress1": email
    }

    query_url = f"{auth.dataverse_url}/api/data/{auth.api_version}/{auth.table_name_repo}"

    response = requests.post(query_url, headers=headers, json=data)
    logging.info(f"Response: {response.status_code} - {response.text}")

    if response.status_code in [201, 204]:
        return {"status" : "success", "message": "Record created successfully"}
    else:
        logging.error(f"Failed to create record in Dataverse: {response.status_code}- {response.text}")
        return False
    
def update_dataverse_record(record_id, update_data):
    access_token_dataverse = auth.acquire_dataverse_token()
    headers = auth.create_headers(access_token_dataverse)

    query_url = f"{auth.dataverse_url}/api/data/{auth.api_version}/{auth.table_name_repo}({record_id})"

    response = requests.patch(query_url, headers=headers, json=update_data)
    logging.info(f"Response: {response.status_code} - {response.text}")

    if response.status_code in [204]:
        return {"status" : "success", "message": "Record updated successfully"}
    else:
        logging.error(f"Failed to update record in Dataverse: {response.status_code}- {response.text}")
        return False

def delete_dataverse_record(record_id):
    access_token_dataverse = auth.acquire_dataverse_token()
    headers = auth.create_headers(access_token_dataverse)

    query_url = f"{auth.dataverse_url}/api/data/{auth.api_version}/{auth.table_name_repo}({record_id})"

    response = requests.delete(query_url, headers=headers)
    logging.info(f"Response: {response.status_code} - {response.text}")

    if response.status_code in [201, 204]:
        logging.info(f"status: success, message: Record deleted successfully {response.status_code} - {response.text}")        
        return True
    else:
        logging.error(f"Failed to delete record in Dataverse: {response.status_code}- {response.text}")
        return False

