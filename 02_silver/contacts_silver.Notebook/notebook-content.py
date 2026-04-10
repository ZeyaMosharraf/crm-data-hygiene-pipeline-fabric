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

df_raw = spark.read.option("multiline", "true").json("Files/bronze/hubspot/contacts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_raw.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_flat = df_raw.select(
    col("id").alias("contact_id"),
    col("properties.firstname").alias("first_name"),
    col("properties.lastname").alias("last_name"),
    col("properties.email").alias("email_id"),
    col("properties.phone").alias("phone_number"),
    col("properties.createdate").alias("create_date"),
    col("updatedAt").alias("updated_date")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_flat)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when

df_silver = df_flat \
    .withColumn("is_missing_id", when(col("contact_id").isNull(), 1).otherwise(0)) \
    .withColumn("is_missing_email", when(col("email_id").isNull(), 1).otherwise(0)) \
    .withColumn("is_missing_firstname", when(col("first_name").isNull(), 1).otherwise(0)) \
    .withColumn("is_missing_lastname", when(col("last_name").isNull(), 1).otherwise(0)) \
    .withColumn("is_missing_phone", when(col("phone_number").isNull(), 1).otherwise(0))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import count, sum
df_silver.select(
    count("*").alias("total_count"),
    sum("is_missing_email").alias("total_email"),
    sum("is_missing_firstname").alias("total_first_name"),
    sum("is_missing_lastname").alias("total_lastname"),
    sum("is_missing_phone").alias("total_phone")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
