import subprocess
import json
import os
import requests
from uuid import UUID
from collections import namedtuple
import pandas as pd



from .src import Config, latest_export_from_last_month, format_previous_export_filename, filtered_export_df_rolling_window


Exports = namedtuple('Exports', [
    'latest_cost_export',
    'previous_cost_export',
    'latest_aks_cost_export',
    'previous_aks_cost_export'
])


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


def get_exports(cost_management_key, config):
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

    return Exports(
        latest_cost_export=latest_cost_export,
        previous_cost_export=previous_cost_export,
        latest_aks_cost_export=latest_aks_cost_export,
        previous_aks_cost_export=previous_aks_cost_export,
    )


def copy_exports_to_workspace(exports: Exports, sas_token, cost_management_key, config: Config):
    latest_cost_export_responses = [
        copy_to_workspace_storage(
            exports.latest_cost_export["name"], config.local_costs_url, sas_token, cost_management_key, config),
        copy_to_workspace_storage(
            exports.latest_aks_cost_export["name"], config.local_aks_costs_url, sas_token, cost_management_key, config)
    ]

    local_previous_costs_url = format_previous_export_filename(
        exports.previous_cost_export, "costexport/{}.csv")

    local_previous_aks_costs_url = format_previous_export_filename(
        exports.previous_aks_cost_export, "costexport/{}-aks.csv")

    previous_cost_export_responses = [
        copy_to_workspace_storage(
            exports.previous_cost_export["name"], local_previous_costs_url, sas_token, cost_management_key, config),
        copy_to_workspace_storage(
            exports.previous_aks_cost_export["name"], local_previous_aks_costs_url, sas_token, cost_management_key, config)
    ]

    return [latest_cost_export_responses, previous_cost_export_responses]


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


def get_sas_enabled_url(blob_name, azure_token, config):
    sas_token_url = config.wsm_url + "/api/workspaces/v1/" + config.current_workspace_id + \
        "/resources/controlled/azure/storageContainer/" + config.container_id + \
    "/getSasToken?sasExpirationDuration=28800&sasBlobName=" + blob_name

    headers = {"Authorization": "Bearer " + azure_token, "accept": "application/json"}
    response = requests.post(sas_token_url, headers=headers)
    status_code = response.status_code
    
    if status_code != 200:
        print(response.text)
        raise Exception("Failed to retrieve SAS token")
    
    return json.loads(response.text)["url"]


# Load storage into a data frame
def load_export_to_dataframe(sasurl):
    return pd.read_csv(sasurl.replace(" ", "%20"), low_memory=False)


# extract the workspace id from the container name, where possible
def extract_workspace_id(container_name):
    # TODO: if desired, handle VM-attached storage containers here; they start with "ls-saturn-"
    if container_name.startswith("sc-"):
        try:
            return UUID(container_name[-36:])
        except ValueError:
            return None
    return None

# extract a workspace id from tags, where available
def extract_workspace_tag(tags):
    if tags and isinstance(tags, str):
        try:
            # tagobj = json.loads("{" + tags + "}")
            tagobj = json.loads(tags)
            if "workspaceId" in tagobj.keys():
                return tagobj["workspaceId"]
        except Exception as e:
            print("Error parsing tags as json: " + str(tags) + " " + repr(e))
    return None

# Look up the names for each workspace.
# 
# Getting the workspace name assumes that the person executing this script has PROJECT_OWNER permissions on
# the billing project, or otherwise can read the workspace. Else, Rawls will return an error when asking for the name.
# 
# def get_workspace_name(workspace_id, azure_token, config):
#     ws_url = config.rawls_url + "/api/workspaces/id/" + config.workspace_id + "?fields=workspace.namespace,workspace.name"

#     headers = {"Authorization": "Bearer " + azure_token, "accept": "application/json"}
#     response = requests.get(ws_url, headers=headers)
#     status_code = response.status_code
    
#     if status_code != 200:
#         print(response.text)
#         return "id: " + config.workspace_id
    
#     ws = json.loads(response.text)
#     return ws["workspace"]["name"]


def list_workspaces(azure_token, config):
    ws_url = config.rawls_url + "/api/workspaces?fields=workspace.name,workspace.workspaceId"

    headers = {"Authorization": "Bearer " + azure_token, "accept": "application/json"}
    response = requests.get(ws_url, headers=headers)
    status_code = response.status_code
    
    if status_code != 200:
        print(response.text)
        return "id: " + config.workspace_id
    
    wslist = json.loads(response.text)
    
    workspaces = {}
    for ws in wslist:
        workspaces[ws["workspace"]["workspaceId"]] = ws["workspace"]["name"]

    return workspaces

def first_non_empty(col1, col2):
    if col1 is None:
        return col2
    else:
        return col1

def build_analysis_dataframes(exports, azure_token, sas_token, config):
    blobmanifest = get_latest_blobmanifest(sas_token, config)

    print("********** found blob inventory manifest file: " + blobmanifest + " **********")

    # Get a SAS-token-enabled URL to the manifest file
    blobmanifest_url = get_sas_enabled_url(blobmanifest, azure_token, config)

    print("********** Appended SAS token to the storage manifest. **********")

    # Read the manifest file as json, then identify the inventory CSVs it references
    response = requests.get(blobmanifest_url)
    status_code = response.status_code
       
    if status_code != 200:
        print(response.text)
        raise Exception("Failed to read manifest file")

    manifestjson = json.loads(response.text)

    blobcsvs = list(map(lambda x: x["blob"], manifestjson["files"]))

    print(blobcsvs)
    print("********** Found storage CSVs **********")

    # blobcsv_urls = list(map(get_sas_enabled_url, blobcsvs))
    blobcsv_urls = [
        get_sas_enabled_url(b, azure_token, config) for b in blobcsvs]

    # Get SAS-token-enabled URLs to each of the exports we want to analyze

    costexport_url = get_sas_enabled_url(config.local_costs_url, azure_token, config)
    aks_costexport_url = get_sas_enabled_url(config.local_aks_costs_url, azure_token, config)

    local_previous_costs_url = format_previous_export_filename(
        exports.previous_cost_export, "costexport/{}.csv")

    local_previous_aks_costs_url = format_previous_export_filename(
        exports.previous_aks_cost_export, "costexport/{}-aks.csv")
    
    
    if exports.latest_cost_export['name'] != exports.previous_cost_export['name']:
        previous_costexport_url = get_sas_enabled_url(local_previous_costs_url, azure_token, config)
    else:
        previous_costexport_url = None

    if exports.latest_aks_cost_export['name'] != exports.previous_aks_cost_export['name']:
        previous_aks_costexport_url = get_sas_enabled_url(local_previous_aks_costs_url, azure_token, config)
    else:
        previous_aks_costexport_url = None

    print("********** Appended SAS token to each of the exports. **********")


    storageframes = list(map(load_export_to_dataframe, blobcsv_urls))

    storage = pd.concat(storageframes)
    print("********** Storage loaded to data frame: " + str(len(storage.index)) + " rows. **********")

    # Load costs into a data frame
    latest_costs = load_export_to_dataframe(costexport_url)
    latest_akscosts = load_export_to_dataframe(aks_costexport_url)

    if previous_costexport_url:
        previous_costs = load_export_to_dataframe(previous_costexport_url)
        nonakscosts = filtered_export_df_rolling_window(
            latest_costs, 
            previous_costs, 
            exports.latest_cost_export, 
            window_size=config.analysis_window_size)
    else:
        nonakscosts = latest_costs
        
    if previous_aks_costexport_url:
        previous_akscosts = load_export_to_dataframe(previous_aks_costexport_url)
        akscosts = filtered_export_df_rolling_window(
            latest_akscosts, 
            previous_akscosts, 
            exports.latest_aks_cost_export, 
            window_size=config.analysis_window_size)
    else:
        akscosts = latest_akscosts

    costs = pd.concat([nonakscosts, akscosts])
    del nonakscosts
    del akscosts

    # and filter costs to only the MRGs we want to consider
    costs = costs[costs["ResourceGroup"].str.lower().isin(config.target_mrgs)]


    print("********** Costs loaded to data frame: " + str(len(costs.index)) + " rows. **********")

    # Massage the storage data frame

    storage = storage[["Name", "Content-Length"]]

    # extract the storage container name
    storage["container_name"] = storage.apply(lambda x: x["Name"].split("/")[0], axis=1)
    storage["workspace_id"] = storage.apply(lambda x: extract_workspace_id(x["container_name"]), axis=1)

    # Find the unique workspaces in the storage data frame
    storage_workspace_ids = storage["workspace_id"].unique()

    print("********** Found " + str(storage_workspace_ids.size) + " workspaces represented in storage export. **********")

    # Massage the costs data frame
    costs["workspace_id"] = costs.apply(lambda x: extract_workspace_tag(x["Tags"]),axis=1)

    # Find the unique workspaces in the costs data frame
    costs_workspace_ids = costs["workspace_id"].unique()

    print("********** Found " + str(costs_workspace_ids.size) + " workspaces represented in cost export. **********")


    all_workspace_ids = pd.concat([storage["workspace_id"], costs["workspace_id"]]).unique()

    print("Retrieving workspace names ...")
    workspaces = list_workspaces(azure_token, config)

    workspace_names = {}
    for wsid in all_workspace_ids:
        if wsid is not None and str(wsid) in workspaces:
            workspace_names[wsid] = workspaces[str(wsid)]
        
    print("********** Found names for " + str(len(workspace_names)) + "/" + str(len(all_workspace_ids)) + " workspaces. **********")

    # add the workspace names to the data frames
    storage["workspace_name"] = storage.apply(lambda x: workspace_names.get(x["workspace_id"], None), axis=1)

    costs["workspace_name"] = costs.apply(lambda x: workspace_names.get(x["workspace_id"], None), axis=1)

    print("********** Annotated data frames with workspace names. **********")

    # storage: calculate a value that is the workspace name if available and otherwise the container_name.
    storage["workspace_or_container"] = storage.apply(lambda x: first_non_empty(x["workspace_name"], x["container_name"]), axis=1)

    print("********** Calculated grouping key for storage. **********")

    # costs: calculate a value that is the workspace name if available and otherwise the meter category.

    costs["workspace_or_category"] = costs.apply(lambda x: first_non_empty(x["workspace_name"], x["MeterCategory"] + " (shared)"), axis=1)

    print("********** Calculated grouping key for costs. **********")

    return storage, costs
