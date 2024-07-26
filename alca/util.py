import pandas as pd
import json
import requests

from uuid import UUID
from datetime import datetime, timedelta
from collections import namedtuple


Config = namedtuple('Config', [
    'wsm_url',
    'rawls_url',
    'current_workspace_id',
    'subscription_id',
    'storage_account',
    'target_mrgs',
    'container_name',
    'container_id',
    'blob_inventory_name',
    'blob_inventory_prefix',
    'cost_management_storage_account',
    'cost_management_storage_container',
    'cost_management_directory',
    'aks_management_storage_account',
    'aks_management_storage_container',
    'aks_management_directory',
    'local_costs_url',
    'local_aks_costs_url',
    'analysis_window_size',
    'cost_column_name'
])


Exports = namedtuple('Exports', [
    'latest_cost_export',
    'previous_cost_export',
    'latest_aks_cost_export',
    'previous_aks_cost_export'
])


AnalysisData = namedtuple('AnalysisData', [
    'storage_grouped',
    'costs',
    'costs_grouped',
    'costs_workspace_grouped',
    'costs_shared_grouped'
])


def from_last_month(last_modified_datetime, now=None):
    if not now:
        now = datetime.now()
    
    # get a datetime object representing the last day of last month
    # by taking the "now" datetime, setting its "day" param to the first of the month,
    # and subtracting one day from it.
    last_month_last_day = now.replace(day=1) - timedelta(days=1)
    
    return last_modified_datetime.month == last_month_last_day.month and \
        last_modified_datetime.year == last_month_last_day.year


def latest_export_from_last_month(export_list): 
    last_modified_dt = lambda ex: datetime.fromisoformat(ex['properties']['lastModified'])
    
    sorted_export_list = sorted(
        [ex for ex in export_list if from_last_month(last_modified_dt(ex))],
        key=last_modified_dt,
        reverse=True
    )
    if sorted_export_list:
        return sorted_export_list[0]


def format_previous_export_filename(cost_export, template):
    export_datetime = datetime.fromisoformat(cost_export['properties']['lastModified'])
    return template.format(f"{export_datetime.year}_{export_datetime.month}_{export_datetime.day}")


def filtered_export_df_rolling_window(latest_cost_df, previous_cost_df, latest_cost_export, window_size=30):
    cost_df = pd.concat([latest_cost_df, previous_cost_df])
    cost_df['UsageDateTime'] = pd.to_datetime(cost_df['UsageDateTime'])
    last_modified = latest_cost_export['properties']['lastModified']
    rolling_window_delta = pd.to_datetime(last_modified).to_datetime64() - pd.Timedelta(days=window_size)
    return cost_df[cost_df['UsageDateTime'] >= rolling_window_delta]


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


def first_non_empty(col1, col2):
    if col1 is None:
        return col2
    else:
        return col1


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
    
    return json.loads(response.text)['token']


def get_sas_enabled_url(blob_name, azure_token, config: Config):
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

