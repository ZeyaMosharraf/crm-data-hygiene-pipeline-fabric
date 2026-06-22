# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "db4bbdff-a1e7-42ae-8fa9-8b6f88636891",
# META       "default_lakehouse_name": "bronze_lakehouse",
# META       "default_lakehouse_workspace_id": "54d86dd4-6d0f-44c9-bd71-9772a45d70a6",
# META       "known_lakehouses": [
# META         {
# META           "id": "db4bbdff-a1e7-42ae-8fa9-8b6f88636891"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "e2938438-aee1-406b-8691-3ff5d56540a9",
# META       "known_warehouses": [
# META         {
# META           "id": "e2938438-aee1-406b-8691-3ff5d56540a9",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run "config_credentials"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run "hubspot_properties"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from datetime import datetime, timezone
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_start = time.time()
print("Notebook Started")
print(datetime.now())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_hubspot_session():
    
    session = requests.Session()

    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_credentials():

    if not HUBSPOT_ACCESS_TOKEN:
        raise ValueError("HUBSPOT_ACCESS_TOKEN is missing")

    if not BASE_URL:
        raise ValueError("BASE_URL is missing")

    return {
        "access_token": HUBSPOT_ACCESS_TOKEN,
        "base_url": BASE_URL
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fetch_object(object_type, after=None, last_updated=None, limit: int = 100):

    creds = get_credentials()
    session = get_hubspot_session()
    properties = hubspot_properties.get(object_type, {}).get("properties", [])
    lastmodified_field = hubspot_properties.get(object_type, {}).get("lastmodified_field", "lastmodifieddate")

    if not properties:
        raise ValueError(f"No properties defined for {object_type}")

    headers = {
        "Authorization": f"Bearer {creds['access_token']}",
        "Content-Type": "application/json"
    }

    try:                                   
        if last_updated and after is None:
            url = f"{creds['base_url']}/crm/v3/objects/{object_type}/search"

            dt = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
            ms = int(dt.timestamp() * 1000)

            body = {
                "limit": limit,
                "properties": properties,
                "filterGroups": [{
                    "filters": [{
                        "propertyName": lastmodified_field,
                        "operator": "GT",
                        "value": str(ms)
                    }]
                }]
            }

            if after:
                body["after"] = after

            api_start = time.time()

            response = session.post(url, headers=headers, json=body, timeout=30)

            api_end = time.time()

            print(
            f"{object_type} Search API Time: "
            f"{round(api_end - api_start, 2)} sec"
            )

        else:
            url = f"{creds['base_url']}/crm/v3/objects/{object_type}"

            params = {
                "limit": limit,
                "properties": ",".join(properties),
            }

            if after:
                params["after"] = after

            api_start = time.time()

            response = session.get(url, headers=headers, params=params, timeout=30)

            api_end = time.time()

            print(
            f"{object_type} GET API Time: "
            f"{round(api_end - api_start, 2)} sec"
            )

        if not response.ok:
            print(f"Request failed!")
            print(f"Response:", response.text)

        response.raise_for_status()

        data = response.json()
        results = data.get("results", [])
        paging = data.get("paging", {})
        next_after = paging.get("next", {}).get("after")

        last_id = None
        last_updated_at = None
        batch_size = 0

        if results:
            last_record = results[-1]
            last_id = last_record.get("id")
            last_updated_at = last_record.get("updatedAt")
            batch_size = len(results)

        return {
            "data": results,
            "next_after": next_after,
            "last_id": last_id,
            "last_updated_at": last_updated_at,
            "batch_size": batch_size
        }

    except Exception as e:               
        print(f"Error fetching {object_type}: {str(e)}")
        if 'response' in locals():
            print("Response status:", response.status_code)
            print("Response error:", response.text)
        if last_updated:
            print("Request body:", json.dumps(body, indent=2))
        else:
            print("Request params:", params)
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_to_bronze(object_type, data):

    if not data:
        print(f"No data to save for {object_type}")
        return

    folder_path = f"/lakehouse/default/Files/bronze/hubspot/{object_type}"
    
    os.makedirs(folder_path, exist_ok=True)

    file_name = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    file_path = f"{folder_path}/{file_name}.json"

    save_start = time.time()

    with open(file_path, "w") as f:
        json.dump(data, f)

    print(f"Saved {len(data)} records → {file_name}")
    save_end = time.time()
    print(f"{object_type} Save Time: " 
        f"{round(save_end - save_start, 2)} sec"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_checkpoint(object_type, after=None, last_id=None, last_updated_at=None):

    folder = f"/lakehouse/default/Files/bronze/hubspot/{object_type}/checkpoint"
    os.makedirs(folder, exist_ok=True)

    path = f"{folder}/checkpoint.json"

    checkpoint_data = {
        "last_after": after,
        "last_id": last_id,
        "last_updated_at": last_updated_at,
        "last_run_time": datetime.now().isoformat()
    }

    with open(path, "w") as f:
        json.dump(checkpoint_data, f)

    print(f"Checkpoint saved: {checkpoint_data}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_checkpoint(object_type):

    path = f"/lakehouse/default/Files/bronze/hubspot/{object_type}/checkpoint/checkpoint.json"

    if not os.path.exists(path):
        return None

    with open(path, "r") as f:
        data = json.load(f)

    return data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_ingestion(object_type):

    start_time = time.time()

    checkpoint = load_checkpoint(object_type) or {}

    after = checkpoint.get("last_after")
    last_updated = checkpoint.get("last_updated_at")

    total_records = 0
    batch_count = 0
    max_updated = last_updated
    api_calls = 0

    while True:

        result = fetch_object(object_type, after, last_updated)

        data = result["data"]
        after = result["next_after"]
        api_calls += 1

        if not data:
            break

        save_to_bronze(object_type, data)

        batch_updated = result.get("last_updated_at")

        if batch_updated:
            if not max_updated or batch_updated > max_updated:
                max_updated = batch_updated

        total_records += len(data)
        batch_count += 1
            

        save_checkpoint(
            object_type,
            after=after,
            last_id=None,
            last_updated_at=max_updated
        )

        if not after:
            break

    end_time = time.time()

    print("\n" + "="*40)
    print(f"Ingestion Completed: {object_type}")
    print("="*40)
    print(f"Total Records : {total_records}")
    print(f"Total Batches : {batch_count}")
    print(f"Last Updated  : {max_updated}")
    print(f"Time Taken    : {round(end_time - start_time, 2)} sec")
    print("="*40)
    print(f"API Calls     : {api_calls}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

objects = ["contacts", "companies", "deals"]

def run_all_objects():

    for obj in objects:
        print(f"\nStarting → {obj}")
        run_ingestion(obj)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_all_objects()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_end = time.time()

print(
    f"Notebook Runtime: "
    f"{round(notebook_end - notebook_start, 2)} sec"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
