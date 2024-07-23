import subprocess
import json
import os

from util import *


def run_process(command_list):
    process = subprocess.run(
        command_list,
        capture_output=True, text=True, check=True
    )
    return process.stdout


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


def get_azure_token():
    run_process('az login --identity --allow-no-subscriptions'.split())
    access_token_response = run_process('az account get-access-token'.split())
    azure_token = json.loads(access_token_response)['accessToken']
    return azure_token


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


def get_latest_blobmanifest(sas_token, config):
    # Find the most recent blob inventory files.
    # Blob inventory output is spread across multiple files. We need to jump through some hoops to find them.
    # Any given inventory report will have a checksum, a manifest, and some number of CSV files.

    # Find the most recent manifest by listing all files, filtering to those whose name ends in "manifest.json",
    # sorting by most-recent, and taking the single most recent manifest.

    raw_output = run_process(
        f'''
        az storage blob list
        -c {config.container_name}
        --prefix {config.blob_inventory_prefix}
        --account-name {config.storage_account}
        --sas-token {sas_token}
        '''.split()
    )
    parsed_output = json.loads(raw_output)
    manifest_output = [
        entry 
        for entry in parsed_output 
        if entry['name'].endswith('manifest.json')
    ]
    sorted_manifest_output = sorted(
        manifest_output,
        key = lambda m: m['properties']['lastModified'],
        reverse = True
    )
    return sorted_manifest_output[0]['name']
