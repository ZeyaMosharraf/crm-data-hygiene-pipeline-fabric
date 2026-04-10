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

hubspot_properties = {
    "contacts": {
        "properties": ["email", "firstname", "lastname", "phone", "lifecyclestage", ],
        "lastmodified_field": "lastmodifieddate"
    },
    "companies": {
        "properties": ["name", "domain"],
        "lastmodified_field": "hs_lastmodifieddate"
    },
    "deals": {
        "properties": ["dealname", "amount"],
        "lastmodified_field": "hs_lastmodifieddate"
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
