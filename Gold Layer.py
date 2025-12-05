# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer
# MAGIC This notebook builds the **Gold Layer**, the curated, analytics‑ready layer of the Lakehouse. It combines and enriches data from the **Silver Layer** to produce:
# MAGIC - Business KPIs
# MAGIC - Clean dimensional models
# MAGIC - Star-schema–style tables
# MAGIC - Aggregations for BI dashboards (Power BI, Tableau, Databricks SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Setup

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("GoldLayerSetup").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. configuration

# COMMAND ----------

CATALOG = "bike_store_project"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"


# Ensure schema exists
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{GOLD_SCHEMA}")
    print(f"[INFO] Verified catalog/schema: {CATALOG}.{GOLD_SCHEMA}")
except Exception as e:
    print(f"[WARN] Could not create catalog/schema: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Load Silver Tables

# COMMAND ----------



orders = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")
order_items = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.order_items")
products = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.products")
brands = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.brands")
categories = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.categories")
customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")
stores = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.stores")
staffs = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.staffs")
stocks = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.stocks")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Build Gold Dimension Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Dim Products

# COMMAND ----------

dim_products = (
    products
    .join(brands, "brand_id", "left")
    .join(categories, "category_id", "left")
    .select(
        "product_id",
        "product_name",
        "brand_id",
        "brand_name",
        "category_id",
        "category_name",
        "model_year",
        "list_price"
    )
)

dim_products.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Dim Customers

# COMMAND ----------

dim_customers = customers.select(
    "customer_id",
    "full_name",
    "city",
    "state",
    "zip_code",
    "phone",
    "email_domain"
)

dim_customers.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Dim Stores

# COMMAND ----------

dim_stores = stores.select(
    "store_id",
    "store_name",
    "city",
    "state",
    "street",
    "zip_code",
    "phone",
    "email")

dim_stores.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Dim Staffs

# COMMAND ----------

dim_staffs = staffs.select(
    "staff_id",
    "full_name",
    "manager_id",
    "store_id",
    "email"
)

dim_staffs.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_staffs")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Build Gold Fact Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Fact Sales

# COMMAND ----------

fact_sales = (
    order_items.alias("oi")
    .join(orders.alias("o"), "order_id")
    .join(products.alias("p"), "product_id")
    .select(
        col("oi.order_id"),
        col("oi.product_id"),
        col("o.store_id"),
        col("o.customer_id"),
        col("o.staff_id"),
        col("oi.quantity"),
        (col("oi.list_price") * col("oi.quantity")).alias("gross_amount"),
        col("oi.discount"),
        ((col("oi.list_price") * col("oi.quantity")) * (1 - col("oi.discount"))).alias("net_amount"),
        col("o.order_date"),
        col("o.required_date"),
        col("o.shipped_date"),
        col("o.order_status").alias("status")
    )
)

fact_sales.write.format("delta").mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Fact Inventory

# COMMAND ----------

fact_inventory = stocks.select(
    "store_id",
    "product_id",
    "quantity"
).withColumn("last_updated", current_timestamp())

fact_inventory.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Build Business KPIs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1 Revenue by Day

# COMMAND ----------

kpi_daily_revenue = fact_sales.groupBy("order_date").agg(
    sum("net_amount").alias("daily_revenue")
)

kpi_daily_revenue.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.kpi_daily_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 Top Products

# COMMAND ----------

top_products = (
    fact_sales.groupBy("product_id")
    .agg(sum("net_amount").alias("total_sales"))
    .orderBy(col("total_sales").desc())
)

top_products.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.kpi_top_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3 Long-Term Customer Value (LTV) Calculation
# MAGIC
# MAGIC This section calculates **Customer Lifetime Value (LTV)** using the Gold `fact_sales` table.
# MAGIC
# MAGIC The model is simple but effective for retail BI:
# MAGIC
# MAGIC * **Total Revenue** = sum of `net_amount` per customer
# MAGIC * **Purchase Frequency** = number of unique orders
# MAGIC * **Recency** = last purchase date
# MAGIC * **Average Order Value (AOV)** = total\_revenue / purchase\_count
# MAGIC * **LTV (simple)** = AOV × purchase\_count
# MAGIC
# MAGIC >(You may later replace with a probabilistic or ML-based model)

# COMMAND ----------

# Load fact and dimension tables
sales = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_sales")
customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")

# Compute core customer metrics
ltv_df = (
    sales.groupBy("customer_id")
    .agg(
        sum("net_amount").alias("total_revenue"),
        countDistinct("order_id").alias("purchase_count"),
        max("order_date").alias("last_purchase_date")
    )
    .withColumn(
        "avg_order_value",
        col("total_revenue") / col("purchase_count")
    )
    .withColumn(
        "ltv",
        col("avg_order_value") * col("purchase_count")
    )
)

# Add customer attributes
ltv_df = (
    ltv_df.join(customers, "customer_id", "left")
)

# Write to Gold layer
ltv_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.{GOLD_SCHEMA}.customer_ltv"
)