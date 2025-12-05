from typing import List, Optional
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

class GeneralTransformer:
    """
    Centralized transformation class used by the Silver Layer.

    - Private helper methods implement small, testable transformations.
    - Public `clean_<entity>` methods call those helpers in a consistent pipeline.
    - `upsert` supports merging into a Delta **table name** (catalog.schema.table) or a **path**.
      If `identity_cols` are provided they will be excluded from INSERT/UPDATE assignments
      to avoid errors with GENERATED ALWAYS AS IDENTITY columns.
    """

    def __init__(self, water_mark: str = "10 minutes", spark_session: Optional[SparkSession] = None):
        self.water_mark = water_mark
        self.spark = spark_session or SparkSession.builder.appName("SilverLayerSetup").getOrCreate()

    # ---------------------
    # Private helpers
    # ---------------------
    def _standardize_cols(self, df: DataFrame) -> DataFrame:
        for c in df.columns:
            if c != c.lower():
                df = df.withColumnRenamed(c, c.lower())
        return df

    def _deduplicate(self, df: DataFrame, key_cols: List[str]) -> DataFrame:
        # If no key provided, fallback to dropDuplicates on all columns
        if not key_cols:
            df = df.dropDuplicates()
        else:
            df = df.dropDuplicates(key_cols)
        return df

    def _extract_email_domain(self, df: DataFrame, email_col: str) -> DataFrame:
        if email_col in df.columns:
            df = df.withColumn("email_domain", split(col(email_col), "@").getItem(1))
        return df

    def _concat_name(self, df: DataFrame, first: str, last: str, out_col: str = "full_name") -> DataFrame:
        if first in df.columns and last in df.columns:
            df = df.withColumn(out_col, concat_ws(" ", col(first), col(last))).drop(first, last)
        return df

    def _add_process_ts(self, df: DataFrame, col_name: str = "process_timestamp") -> DataFrame:
        return df.withColumn(col_name, current_timestamp())

    def _fill_defaults(self, df: DataFrame, defaults: dict) -> DataFrame:
        return df.fillna(defaults)

    # ---------------------
    # Public cleaning pipelines per entity
    # ---------------------
    def clean_brands(self, df: DataFrame) -> DataFrame:
        df = self._standardize_cols(df)
        df = self._deduplicate(df, ["brand_id"] if "brand_id" in df.columns else [])
        df = self._add_process_ts(df)
        return df

    def clean_categories(self, df: DataFrame) -> DataFrame:
        df = self._standardize_cols(df)
        df = self._deduplicate(df, ["category_id"] if "category_id" in df.columns else [])
        df = self._add_process_ts(df)
        return df

    def clean_customers(self, df: DataFrame) -> DataFrame:
        df = self._standardize_cols(df)
        df = self._extract_email_domain(df, "email")
        df = self._concat_name(df, "first_name", "last_name")
        df = self._deduplicate(df, ["customer_id"] if "customer_id" in df.columns else [])
        df = self._add_process_ts(df)
        return df

    def clean_products(self, df: DataFrame) -> DataFrame:
        df = self._standardize_cols(df)
        df = self._fill_defaults(df, {"list_price": 0.0})
        df = self._deduplicate(df, ["product_id"] if "product_id" in df.columns else [])
        df = self._add_process_ts(df)
        return df

    def clean_orders(self, df: DataFrame) -> DataFrame:
        df = self._standardize_cols(df)
        # Normalize order date/time
        for c in ["order_date", "created_at", "created"]:
            if c in df.columns:
                df = df.withColumn("order_date", to_date(col(c)))
                break
        df = self._deduplicate(df, ["order_id"] if "order_id" in df.columns else [])
        df = self._add_process_ts(df)
        return df

    def clean_order_items(self, df: DataFrame) -> DataFrame:
        df = self._standardize_cols(df)
        # composite dedupe on order_id + product_id + item_id if present
        keys = []
        if "order_id" in df.columns:
            keys.append("order_id")
        if "product_id" in df.columns:
            keys.append("product_id")
        if "order_item_id" in df.columns:
            keys.append("order_item_id")
        df = self._deduplicate(df, keys)
        df = self._add_process_ts(df)
        return df

    def clean_staffs(self, df: DataFrame) -> DataFrame:
        df = self._standardize_cols(df)
        df = self._concat_name(df, "first_name", "last_name")
        df = self._deduplicate(df, ["staff_id"] if "staff_id" in df.columns else [])
        df = self._add_process_ts(df)
        return df

    def clean_stocks(self, df: DataFrame) -> DataFrame:
        df = self._standardize_cols(df)
        keys = [k for k in ["store_id", "product_id", "stock_id"] if k in df.columns]
        df = self._deduplicate(df, keys)
        df = self._add_process_ts(df)
        return df

    def clean_stores(self, df: DataFrame) -> DataFrame:
        df = self._standardize_cols(df)
        df = self._deduplicate(df, ["store_id"] if "store_id" in df.columns else [])
        df = self._add_process_ts(df)
        return df

    # ---------------------
    # Upsert / Merge logic
    # ---------------------
    def upsert(self, df: DataFrame, target: str, key_cols: List[str], identity_cols: Optional[List[str]] = None, timestamp_col: str = "process_timestamp") -> DataFrame:
        """
        Upsert `df` into target. `target` may be either:
          - a catalog table name like "catalog.schema.table" OR
          - a Delta path on storage like "/mnt/.../table"

        `identity_cols` if provided will be excluded from INSERT and UPDATE assignments so that
        Delta can generate identity values (avoids GENERATED ALWAYS AS IDENTITY explicit insert errors).
        """
        identity_cols = identity_cols or []

        # Normalize column names in source to be safe
        for c in df.columns:
            if c != c.lower():
                df = df.withColumnRenamed(c, c.lower())

        # Determine if target is a catalog.table (contains a dot and not a path)
        is_table = "." in target and not target.startswith("/")

        # Check existence
        target_exists = True
        try:
            if is_table:
                self.spark.table(target)
            else:
                if not DeltaTable.isDeltaTable(self.spark, target):
                    target_exists = False
        except Exception:
            target_exists = False

        # Initial create if needed
        if not target_exists:
            if is_table:
                df.write.format("delta").mode("overwrite").saveAsTable(target)
            else:
                df.write.format("delta").mode("overwrite").save(target)
            return df

        # Build DeltaTable reference
        if is_table:
            delta_tbl = DeltaTable.forName(self.spark, target)
        else:
            delta_tbl = DeltaTable.forPath(self.spark, target)

        # Create temp view for source
        view_name = f"tmp_src_{abs(hash(target))}_{int(datetime.now().timestamp())}"
        df.createOrReplaceTempView(view_name)

        # Merge condition
        match_cond = " AND ".join([f"target.{k} = source.{k}" for k in key_cols])

        # Source columns excluding identity_cols
        src_cols = [c for c in df.columns if c not in identity_cols]

        # Build update assignments excluding key columns and identity columns
        update_cols = [c for c in src_cols if c not in key_cols]
        if not update_cols:
            update_assignment_sql = ""
        else:
            update_assignment_sql = ", ".join([f"target.`{c}` = source.`{c}`" for c in update_cols])

        insert_cols = key_cols + [c for c in src_cols if c not in key_cols]
        insert_cols_sql = ", ".join([f"`{c}`" for c in insert_cols])
        insert_vals_sql = ", ".join([f"source.`{c}`" for c in insert_cols])

        merge_sql = f"""
        MERGE INTO {target} AS target
        USING (SELECT * FROM {view_name}) AS source
        ON {match_cond}
        WHEN MATCHED AND source.{timestamp_col} >= target.{timestamp_col} THEN
          UPDATE SET {update_assignment_sql}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols_sql}) VALUES ({insert_vals_sql})
        """

        self.spark.sql(merge_sql)

        return delta_tbl.toDF()
