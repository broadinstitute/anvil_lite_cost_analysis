import subprocess
import json
import os
from datetime import datetime
import pandas as pd


def az_storage_blob_list(
    prefix, 
    cost_management_key, 
    cost_management_storage_container, 
    cost_management_storage_account
):    
    script_path = f"{os.path.dirname(__file__)}/az-storage-blob-list.sh"
    return run_bash_get_json([
        script_path,
        prefix,
        cost_management_key, 
        cost_management_storage_container, 
        cost_management_storage_account])


def run_bash_get_json(args):
    try:
        process = subprocess.run(args, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise Exception(f"Bash script execution failed: {args[0]}. Error code: {e.returncode}")
    return json.loads(process.stdout)


def from_last_month(export):
    return datetime.fromisoformat(export['properties']['lastModified']).month == datetime.now().month-1


def latest_export_from_last_month(export_list): 
    sorted_export_list = sorted(
        [ex for ex in export_list if from_last_month(ex)],
        key=lambda ex: datetime.fromisoformat(ex['properties']['lastModified']),
        reverse=True
    )
    if sorted_export_list:
        return sorted_export_list[0]


def filtered_export_df_rolling_window(latest_cost_df, previous_cost_df, latest_cost_export, window_size=30):
    cost_df = pd.concat([latest_cost_df, previous_cost_df])
    cost_df['UsageDateTime'] = pd.to_datetime(cost_df['UsageDateTime'])
    last_modified = latest_cost_export['properties']['lastModified']
    rolling_window_delta = pd.to_datetime(last_modified).to_datetime64() - pd.Timedelta(days=window_size)
    return cost_df[cost_df['UsageDateTime'] >= rolling_window_delta]
