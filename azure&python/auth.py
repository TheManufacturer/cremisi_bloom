import azure.functions as func
import logging
import json
import os
import requests
import msal

#Import Variables

#Dataverse URL eg: https://<orgname>.crm.dynamics.com
dataverse_url = os.getenv("DATAVERSE_URL")

api_version = "v9.2" #API Version

#Table Name
table_name_repo = os.getenv("TABLE_NAME")
primary_column_repo = os.getenv("PRIMARY_COLUMN")

#Acquire Dataverse Token
def acquire_dataverse_token():

    try:
        #configurations
        client_id = os.getenv("CLIENT_ID")
        client_secret = os.getenv("CLIENT_SECRET")
        tenant_id = os.getenv("TENANT_ID")

        #acquire token
        authority = f"https://login.microsoftonline.com/{tenant_id}"

        #get token
        scope = f"{dataverse_url}/.default"

        #Object for authentication
        app = msal.ConfidentialClientApplication(
            client_id, authority=authority, client_credential=client_secret
            )
    
        result = app.acquire_token_silent(scope, account=None)

        if not result:
            result = app.acquire_token_for_client(scopes=[scope])
    
        if "access_token" in result:
            return result["access_token"]
    
        logging.error("Failed to acquire dataverse token")
        return None
    except Exception as e:
        logging.error(f"Error in acquire_dataverse_token: {e}")

#create headers
def create_headers(token):
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Accept": "application/json",
            "Prefer": "odata.include-annotations=*, odata.maxpagesize=500"
        }
        return headers
    except Exception as e:
        logging.error(f"Error in create_headers: {e}")