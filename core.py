import pandas as pd
import humanize
import json
import requests

from .util import *
from .azure import *


def get_exports(cost_management_key, config: Config):
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

    # storage: group by workspace_or_container, sum the Content-Length
    storage_grouped = storage.groupby("workspace_or_container")["Content-Length"].sum().to_frame().sort_values(by="Content-Length", ascending=False).reset_index()

    # convert bytes to megabytes for plotting, below
    storage_grouped["MB"] = storage_grouped["Content-Length"]/(1024*1024)

    # convert bytes to a human-readable string
    storage_grouped["Total Size"] = storage_grouped["Content-Length"].map(humanize.naturalsize)

    return storage_grouped, costs

