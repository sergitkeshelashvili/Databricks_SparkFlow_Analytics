# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer Data Load Script
# MAGIC
# MAGIC ##### This script processes raw data from the bronze schema of the dwh_project catalog and loads cleaned, transformed data into the silver schema. It utilizes PySpark for data processing and includes a DataValidation class for deduplication based on key and CDC columns. 
# MAGIC ##### The script handles six tables (crm_cust_info, crm_prd_info, crm_sales_details, erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2), performing tasks like trimming strings, standardizing values, handling nulls, and applying business logic for data consistency. Each table is saved as a Delta table with an added dwh_create_date column. The script logs processing durations and includes error handling for robust execution.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import *
import time

# ====================================================
# Data Validation Class
# ====================================================
class DataValidation:
    def __init__(self, df: DataFrame):
        self.df = df

    def dedup(self, key_col: str = None, cdc_col: str = None, drop_nulls: bool = True) -> DataFrame:
        """
        Deduplicate DataFrame and drop rows with nulls.
        """
        df = self.df

        # Drop rows with nulls if required
        if drop_nulls:
            df = df.na.drop()

        cols = df.columns

        # Deduplication logic
        if key_col and key_col in cols:
            if cdc_col and cdc_col in cols:
                window = Window.partitionBy(key_col).orderBy(desc(cdc_col))
                df = df.withColumn("row_num", row_number().over(window))
                df = df.filter(col("row_num") == 1).drop("row_num")
            else:
                df = df.dropDuplicates([key_col])
        else:
            df = df.dropDuplicates()

        return df


# ====================================================
# Clean string columns
# ====================================================
def clean_string_columns(df: DataFrame, columns: list) -> DataFrame:
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))
    return df


# ====================================================
# Silver Layer Loader
# ====================================================
def load_silver():
    batch_start_time = time.time()
    print('================================================')
    print('Loading Silver Layer')
    print('================================================')

    try:
        # Ensure schema exists
        spark.sql("CREATE SCHEMA IF NOT EXISTS dwh_project.silver")

        # -------------------------------
        # 1. crm_cust_info
        # -------------------------------
        print(">> Processing crm_cust_info")
        df = spark.table("dwh_project.bronze.crm_cust_info")
        validator = DataValidation(df)
        df = validator.dedup("cst_id", "cst_create_date")

        df = clean_string_columns(df, ["cst_firstname", "cst_lastname"])

        if "cst_marital_status" in df.columns:
            df = df.withColumn(
                "cst_marital_status",
                when(upper(col("cst_marital_status")) == "S", "Single")
                .when(upper(col("cst_marital_status")) == "M", "Married")
                .otherwise("n/a")
            )

        if "cst_gndr" in df.columns:
            df = df.withColumn(
                "cst_gndr",
                when(upper(col("cst_gndr")) == "F", "Female")
                .when(upper(col("cst_gndr")) == "M", "Male")
                .otherwise("n/a")
            )

        df = df.withColumn("dwh_create_date", current_date())
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dwh_project.silver.crm_cust_info")

        # -------------------------------
        # 2. crm_prd_info
        # -------------------------------
        print(">> Processing crm_prd_info")
        df = spark.table("dwh_project.bronze.crm_prd_info")
        validator = DataValidation(df)
        df = validator.dedup("prd_id", "prd_start_dt")

        df = df.withColumn("cat_id", regexp_replace(substring(col("prd_key"), 1, 5), "-", "_"))
        df = df.withColumn("prd_key", substring(col("prd_key"), 7, length(col("prd_key"))))
        df = df.withColumn("prd_cost", coalesce(col("prd_cost"), lit(0)))

        if "prd_line" in df.columns:
            df = df.withColumn(
                "prd_line",
                when(upper(trim(col("prd_line"))) == "M", "Mountain")
                .when(upper(trim(col("prd_line"))) == "R", "Road")
                .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
                .when(upper(trim(col("prd_line"))) == "T", "Touring")
                .otherwise("n/a")
            )

        df = df.withColumn("prd_start_dt", to_date(col("prd_start_dt")))

        window_prd = Window.partitionBy("prd_key").orderBy("prd_start_dt")
        df = df.withColumn("prd_end_dt", lead("prd_start_dt").over(window_prd) - expr("INTERVAL 1 DAYS"))
        df = df.withColumn("dwh_create_date", current_date())

        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dwh_project.silver.crm_prd_info")

        # -------------------------------
        # 3. crm_sales_details
        # -------------------------------
        print(">> Processing crm_sales_details")
        df = spark.table("dwh_project.bronze.crm_sales_details")
        validator = DataValidation(df)
        df = validator.dedup("sls_ord_num", "sls_order_dt")

        for dt_col in ["sls_order_dt", "sls_ship_dt", "sls_due_dt"]:
            df = df.withColumn(
                dt_col,
                when((col(dt_col) == 0) | (length(col(dt_col).cast("string")) != 8), None)
                .otherwise(to_date(col(dt_col).cast("string"), "yyyyMMdd"))
            )

        df = df.withColumn(
            "sls_sales",
            when((col("sls_sales").isNull()) | (col("sls_sales") <= 0) | (col("sls_sales") != col("sls_quantity") * abs(col("sls_price"))),
                 col("sls_quantity") * abs(col("sls_price")))
            .otherwise(col("sls_sales"))
        )

        df = df.withColumn(
            "sls_price",
            when((col("sls_price").isNull()) | (col("sls_price") <= 0),
                 col("sls_sales") / nullif(col("sls_quantity"), lit(0)))
            .otherwise(col("sls_price"))
        )

        df = df.withColumn("dwh_create_date", current_date())
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dwh_project.silver.crm_sales_details")

        # -------------------------------
        # 4. erp_cust_az12
        # -------------------------------
        print(">> Processing erp_cust_az12")
        df = spark.table("dwh_project.bronze.erp_cust_az12")
        validator = DataValidation(df)
        df = validator.dedup("cid", "bdate")

        df = df.withColumn(
            "cid",
            when(col("cid").startswith("NAS"), substring(col("cid"), 4, length(col("cid"))))
            .otherwise(col("cid"))
        )

        df = df.withColumn(
            "bdate",
            when(col("bdate") > current_date(), None).otherwise(col("bdate"))
        )

        df = df.withColumn(
            "gen",
            when(upper(trim(col("gen"))).isin("F", "FEMALE"), "Female")
            .when(upper(trim(col("gen"))).isin("M", "MALE"), "Male")
            .otherwise("n/a")
        )

        df = df.withColumn("dwh_create_date", current_date())
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dwh_project.silver.erp_cust_az12")

        # -------------------------------
        # 5. erp_loc_a101
        # -------------------------------
        print(">> Processing erp_loc_a101")
        df = spark.table("dwh_project.bronze.erp_loc_a101")

        df = df.withColumn("cid", regexp_replace(col("cid"), "-", ""))
        df = df.withColumn(
            "cntry",
            when(trim(col("cntry")) == "DE", "Germany")
            .when(trim(col("cntry")).isin("US", "USA"), "United States")
            .when((trim(col("cntry")) == "") | col("cntry").isNull(), "n/a")
            .otherwise(trim(col("cntry")))
        )

        df = df.na.drop()
        df = df.dropDuplicates(["cid", "cntry"])
        df = df.withColumn("dwh_create_date", current_date())
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dwh_project.silver.erp_loc_a101")

        # -------------------------------
        # 6. erp_px_cat_g1v2
        # -------------------------------
        print(">> Processing erp_px_cat_g1v2")
        df = spark.table("dwh_project.bronze.erp_px_cat_g1v2")
        validator = DataValidation(df)
        df = validator.dedup("id")

        df = df.withColumn("dwh_create_date", current_date())
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dwh_project.silver.erp_px_cat_g1v2")

        # -------------------------------
        # Done
        # -------------------------------
        batch_end_time = time.time()
        print('==========================================')
        print('Loading Silver Layer is Completed')
        print('   - Total Load Duration: ' + str(int(batch_end_time - batch_start_time)) + ' seconds')
        print('==========================================')

    except Exception as e:
        print('==========================================')
        print('ERROR OCCURRED DURING LOADING SILVER LAYER')
        print('Error Message: ' + str(e))
        print('==========================================')


# ====================================================
# Run
# ====================================================
try:
    spark.sql("CREATE CATALOG IF NOT EXISTS dwh_project")
except Exception as e:
    print(f"Warning: Could not create catalog 'dwh_project'. Error: {str(e)}. Ensure catalog exists or use hive_metastore.")

load_silver()
