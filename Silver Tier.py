# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer
# MAGIC
# MAGIC The **Silver Layer** transforms Bronze raw data into clean, standardized, analytics-ready tables.
# MAGIC
# MAGIC ## Purpose of the Silver Layer
# MAGIC - Apply data cleaning and normalization
# MAGIC - Standardize column naming
# MAGIC - Deduplicate using business keys
# MAGIC - Enrich records (e.g., extracting email domain, computing full names)
# MAGIC - Add processing timestamps
# MAGIC - Ensure consistent schema for Later Usage
# MAGIC
# MAGIC All transformations are handled inside `GeneralTransformer.py` using entity-specific cleaning pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Environment Setup

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from GeneralTransformer import GeneralTransformer

spark = SparkSession.builder.appName("SilverLayerSetup").getOrCreate()
transformer = GeneralTransformer()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Paths and configuration

# COMMAND ----------

# Pathes
CATALOG = "bike_store_project"
BRONZE = "bronze"
SILVER = "silver"

# Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{SILVER}")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SILVER}`")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Load Bronze Tables
# MAGIC Import raw Delta tables as DataFrames to be transformed.

# COMMAND ----------

bronze_brands = spark.table(f"{CATALOG}.{BRONZE}.brands")
bronze_categories = spark.table(f"{CATALOG}.{BRONZE}.categories")
bronze_customers = spark.table(f"{CATALOG}.{BRONZE}.customers")
bronze_order_items = spark.table(f"{CATALOG}.{BRONZE}.order_items")
bronze_orders = spark.table(f"{CATALOG}.{BRONZE}.orders")
bronze_products = spark.table(f"{CATALOG}.{BRONZE}.products")
bronze_staffs = spark.table(f"{CATALOG}.{BRONZE}.staffs")
bronze_stocks = spark.table(f"{CATALOG}.{BRONZE}.stocks")
bronze_stores = spark.table(f"{CATALOG}.{BRONZE}.stores")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Transform DataFrames Using GeneralTransformer
# MAGIC Each entity passes through its own cleaning pipeline.

# COMMAND ----------

silver_brands = transformer.clean_brands(bronze_brands)
silver_categories = transformer.clean_categories(bronze_categories)
silver_customers = transformer.clean_customers(bronze_customers)
silver_order_items = transformer.clean_order_items(bronze_order_items)
silver_orders = transformer.clean_orders(bronze_orders)
silver_products = transformer.clean_products(bronze_products)
silver_staffs = transformer.clean_staffs(bronze_staffs)
silver_stocks = transformer.clean_stocks(bronze_stocks)
silver_stores = transformer.clean_stores(bronze_stores)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Write Silver Tables
# MAGIC Transformed Silver tables overwrite the prior version (schema preserved).

# COMMAND ----------

silver_brands.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.brands")
silver_categories.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.categories")
silver_customers.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.customers")
silver_order_items.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.order_items")
silver_orders.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.orders")
silver_products.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.products")
silver_staffs.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.staffs")
silver_stocks.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.stocks")
silver_stores.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER}.stores")


print("[INFO] Silver Layer tables created successfully.")

# COMMAND ----------

