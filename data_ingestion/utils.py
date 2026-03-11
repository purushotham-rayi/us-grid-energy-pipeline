# import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.filedatalake import DataLakeServiceClient
from opencensus.ext.azure.log_exporter import AzureLogHandler
import logging
import json

KEY_VAULT_URL = "https://keyvault-usgrid.vault.azure.net/"


def fetch_secret(secret_name):
    credential = DefaultAzureCredential()
    client=SecretClient(vault_url=KEY_VAULT_URL,credential=credential)
    return client.get_secret(secret_name).value

def get_api_key():
    return fetch_secret("eia-api-key")

def get_adls_client():
    conn_str= fetch_secret("adls-connection-string")
    return DataLakeServiceClient.from_connection_string(conn_str)

def get_appinsights_connection_string():
    return fetch_secret("appinsights-connection-string")

def get_logger(name):
    logger=logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(AzureLogHandler(connection_string=get_appinsights_connection_string()))
    return logger

logger=get_logger(__name__)

def save_to_adls(adls_client, payload, container, file_path):
    file_system_client=adls_client.get_file_system_client(container)
    file_client = file_system_client.get_file_client(file_path)
    file_client.upload_data(json.dumps(payload,indent=4), overwrite=True)
    logger.info(f"Uploaded data to {file_path}")
