import pandas as pd
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

