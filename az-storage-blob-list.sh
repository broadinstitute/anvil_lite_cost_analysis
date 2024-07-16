#!/bin/bash

# Find the cost exports from the custom storage container.
# using jq, sort descending by lastModified to find the latest export.

prefix=$1
cost_management_key=$2
cost_management_storage_container=$3
cost_management_storage_account=$4

az storage blob list \
        -c ${cost_management_storage_container} \
        --prefix ${prefix} \
        --account-name ${cost_management_storage_account} \
        --account-key ${cost_management_key} \
        | jq 'sort_by(.properties["lastModified"]) | reverse'


