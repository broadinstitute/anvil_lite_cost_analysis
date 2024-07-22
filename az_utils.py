import subprocess
import json
import os
import requests
from .src import Config, latest_export_from_last_month, format_previous_export_filename

def az_storage_blob_list(
    prefix, 
    cost_management_key, 
    cost_management_storage_container, 
    cost_management_storage_account
):    
    script_path = f"{os.path.dirname(__file__)}/az-storage-blob-list.sh"
    process = subprocess.run([
        script_path,
        prefix,
        cost_management_key, 
        cost_management_storage_container, 
        cost_management_storage_account],
        capture_output=True, text=True, check=True)

    return json.loads(process.stdout)


def run_process(command_list):
    process = subprocess.run(
        command_list,
        capture_output=True, text=True, check=True
    )
    return process.stdout


def get_azure_token():
    run_process('az login --identity --allow-no-subscriptions'.split())
    access_token_response = run_process('az account get-access-token'.split())
    azure_token = json.loads(access_token_response)['accessToken']
    return azure_token


def get_sas_token(azure_token, config: Config):
    sas_token_url = config.wsm_url + "/api/workspaces/v1/" \
        + config.current_workspace_id + "/resources/controlled/azure/storageContainer/" \
        + config.container_id + "/getSasToken?sasExpirationDuration=28800"

    headers = {"Authorization": "Bearer " + azure_token, "accept": "application/json"}
    response = requests.post(sas_token_url, headers=headers)
    status_code = response.status_code
    
    if status_code != 200:
        print(response.text)
        raise Exception("Failed to retrieve SAS token")
    
    return json.loads(response.text)["token"]


def copy_to_workspace_storage(sourcefile, destinationfile, sas_token, cost_management_key, config: Config):
    # generate a sas token for the custom storage container
    end = run_process(['date', '-u', '-d', "30 minutes", '+%Y-%m-%dT%H:%MZ'])
    source_uri_raw = run_process(
        f'''
        az storage blob generate-sas
            -c {config.cost_management_storage_container}
            -n {sourcefile}
            --account-name {config.cost_management_storage_account}
            --account-key "{cost_management_key}"
            --permissions r
            --expiry {end.rstrip()}
            --full-uri
        '''.split()
    )
    source_uri = source_uri_raw.rstrip('\n').strip('"')
    
    output = run_process(
        f'''
        az storage blob copy start
            --requires-sync true
            --source-uri {source_uri}
            --destination-blob {destinationfile}
            --destination-container {config.container_name}
            --account-name {config.storage_account}
            --sas-token {sas_token}
        '''.split()
    )
    return json.loads(output)


def copy_exports_to_workspace(azure_token, cost_management_key, config: Config):
    # Find the most recent cost export from the custom storage container.
    sorted_cost_exports = az_storage_blob_list(
        config.cost_management_directory, 
        cost_management_key,
        config.cost_management_storage_container, 
        config.cost_management_storage_account)
    latest_cost_export = sorted_cost_exports[0]
    previous_cost_export = latest_export_from_last_month(sorted_cost_exports)


    # Find the most recent AKS cost export, using the same technique
    sorted_aks_cost_exports = az_storage_blob_list(
        config.aks_management_directory,
        cost_management_key, 
        config.cost_management_storage_container, 
        config.cost_management_storage_account)
    latest_aks_cost_export = sorted_aks_cost_exports[0]
    previous_aks_cost_export = latest_export_from_last_month(sorted_aks_cost_exports)


    sas_token = get_sas_token(azure_token, config)

    latest_cost_export_responses = [
        copy_to_workspace_storage(
            latest_cost_export["name"], config.local_costs_url, sas_token, cost_management_key, config),
        copy_to_workspace_storage(
            latest_aks_cost_export["name"], config.local_aks_costs_url, sas_token, cost_management_key, config)
    ]

    local_previous_costs_url = format_previous_export_filename(
        previous_cost_export, "costexport/{}.csv")

    local_previous_aks_costs_url = format_previous_export_filename(
        previous_aks_cost_export, "costexport/{}-aks.csv")

    previous_cost_export_responses = [
        copy_to_workspace_storage(
            previous_cost_export["name"], local_previous_costs_url, sas_token, cost_management_key, config),
        copy_to_workspace_storage(
            previous_aks_cost_export["name"], local_previous_aks_costs_url, sas_token, cost_management_key, config)
    ]

    return [latest_cost_export_responses, previous_cost_export_responses]


