# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer
# MAGIC This notebook prepares the **Bronze Layer**, the first layer in a Lakehouse.
# MAGIC The Bronze layer stores **raw, ingested, minimally processed** data.
# MAGIC Goals:
# MAGIC - Ingest CSV files from workspace folders
# MAGIC - Preserve original schema
# MAGIC - Store all data as Delta tables
# MAGIC - Apply partitioning for large / frequently queried tables
# MAGIC - Enable incremental streaming ingestion for reliability

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Environment Setup

# COMMAND ----------

import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Create Spark session
spark = SparkSession.builder.appName("BronzeLayerSetup").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Paths and configuration

# COMMAND ----------

VOLUME_NAME = "assets"
CATALOG = "bike_store_project"
SCHEMA = "bronze"


PROJECT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
RAW_BASE = f"{PROJECT_BASE}/raw"
BRONZE_BASE = PROJECT_BASE
LOGS_PATH_DBFS = f"{PROJECT_BASE}/logs"
CHECK_PATH_DBFS = f"{PROJECT_BASE}/checkpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Catalog, Schema & Volume Setup

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME_NAME}`")
    print(f"[INFO] Verified catalog/schema/volume: {CATALOG}.{SCHEMA}.{VOLUME_NAME}")
except Exception as e:
    print(f"[WARN] Could not create catalog/schema/volume: {e}")

# COMMAND ----------

# Switch to catalog and schema
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Loading CSV Files (Folder Per Entity Required)

# COMMAND ----------

workspace_path = "/Workspace/Users/mohammedkamalalsyd@gmail.com/PySpark-Projects/PySpark-DBT #1/Raw Data"

# Detect entities from folder names
entities = [name for name in os.listdir(workspace_path) if os.path.isdir(os.path.join(workspace_path, name))]

# Infer schemas for all entities
schemas = {}
for entity in entities:
    sample_path = f"{workspace_path}/{entity}/{entity}.csv"
    df_sample = spark.read.csv(sample_path, header=True, inferSchema=True, samplingRatio=0.3)
    schemas[entity] = df_sample.schema

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Partition Rules for Bronze Layer

# COMMAND ----------

PARTITIONED_TABLES = {
"orders": "order_date",
"customers": "customer_id",
"stocks": "store_id",
"products": "category_id",
"order_items": "order_id"
}

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Streaming Load Into Bronze Tables

# COMMAND ----------

for entity in entities:
    source_path = f"{workspace_path}/{entity}/"
    schema = schemas[entity]

    df_stream = (
        spark.readStream.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(source_path)
    )

    df_stream = df_stream.withColumn(
        "ingested_at", 
        current_timestamp()
    ) # Will be Used in the Upsert logic to update the existing record based on last version of the record

    writer = (
        df_stream.writeStream.format("delta")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", f"{CHECK_PATH_DBFS}/{entity}")
    )

    # Apply partitioning if defined
    partition_col = PARTITIONED_TABLES.get(entity)
    if partition_col:
        writer = writer.option("delta.partitionColumns", partition_col)


    # --- FINAL WRITE ---
    writer.toTable(f"`{CATALOG}`.`{SCHEMA}`.`{entity}`")
    print(f"[INFO] Loaded entity: {entity} (partitioned by: {partition_col})")