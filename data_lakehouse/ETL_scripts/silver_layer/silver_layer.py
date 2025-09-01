# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer Data Load Script
# MAGIC
# MAGIC ##### This script processes raw data from the bronze schema of the dwh_project catalog and loads cleaned, transformed data into the silver schema.
# MAGIC  
# MAGIC ##### The script handles six tables (crm_cust_info, crm_prd_info, crm_sales_details, erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2), performing tasks like trimming strings, standardizing values, handling nulls, and applying business logic for data consistency. Each table is saved as a Delta table with an added dwh_create_date column. The script logs processing durations and includes error handling for robust execution.

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
import time

# ====================================================
# Silver Layer Loader
# ====================================================
def load_silver():
    batch_start_time = time.time()
    print('================================================')
    print('Loading Silver Layer')
    print('================================================')

    try:
        # Ensure catalog and schema exist
        spark.sql("CREATE CATALOG IF NOT EXISTS dwh_project")
        spark.sql("CREATE SCHEMA IF NOT EXISTS dwh_project.silver")

        # -------------------------------
        # 1. crm_cust_info
        # -------------------------------
        print(">> Processing crm_cust_info")
        df = spark.table("dwh_project.bronze.crm_cust_info")

        # Deduplicate by cst_id, keeping the latest record based on cst_create_date
        window_cust = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())
        df = df.withColumn("flag_last", row_number().over(window_cust))
        df = df.filter(col("flag_last") == 1).drop("flag_last")

        # Clean string columns
        df = df.withColumn("cst_firstname", trim(col("cst_firstname")))
        df = df.withColumn("cst_lastname", trim(col("cst_lastname")))

        # Standardize cst_marital_status
        df = df.withColumn(
            "cst_marital_status",
            when(upper(trim(col("cst_marital_status"))) == "S", "Single")
            .when(upper(trim(col("cst_marital_status"))) == "M", "Married")
            .otherwise("n/a")
        )

        # Standardize cst_gndr
        df = df.withColumn(
            "cst_gndr",
            when(upper(trim(col("cst_gndr"))) == "F", "Female")
            .when(upper(trim(col("cst_gndr"))) == "M", "Male")
            .otherwise("n/a")
        )

        # Add audit column
        df = df.withColumn("dwh_create_date", current_date())

        # Save to silver layer (overwrite to mimic TRUNCATE)
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dwh_project.silver.crm_cust_info")

        # -------------------------------
        # 2. crm_prd_info
        # -------------------------------
        print(">> Processing crm_prd_info")
        df = spark.table("dwh_project.bronze.crm_prd_info")

        # Transform prd_key to derive cat_id and update prd_key
        df = df.withColumn("cat_id", regexp_replace(substring(col("prd_key"), 1, 5), "-", "_"))
        df = df.withColumn("prd_key", substring(col("prd_key"), 7, length(col("prd_key"))))

        # Handle null prd_cost
        df = df.withColumn("prd_cost", coalesce(col("prd_cost"), lit(0)))

        # Standardize prd_line
        df = df.withColumn(
            "prd_line",
            when(upper(trim(col("prd_line"))) == "M", "Mountain")
            .when(upper(trim(col("prd_line"))) == "R", "Road")
            .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
            .when(upper(trim(col("prd_line"))) == "T", "Touring")
            .otherwise("n/a")
        )

        # Cast prd_start_dt to date
        df = df.withColumn("prd_start_dt", to_date(col("prd_start_dt")))

        # Calculate prd_end_dt using LEAD
        window_prd = Window.partitionBy("prd_key").orderBy("prd_start_dt")
        df = df.withColumn("prd_end_dt", lead("prd_start_dt").over(window_prd) - expr("INTERVAL 1 DAYS"))

        # Add audit column
        df = df.withColumn("dwh_create_date", current_date())

        # Save to silver layer
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dwh_project.silver.crm_prd_info")

        # -------------------------------
        # 3. crm_sales_details
        # -------------------------------
        print(">> Processing crm_sales_details")
        df = spark.table("dwh_project.bronze.crm_sales_details")

        # Validate and cast date columns
        for dt_col in ["sls_order_dt", "sls_ship_dt", "sls_due_dt"]:
            df = df.withColumn(
                dt_col,
                when(
                    (col(dt_col) == 0) | (length(col(dt_col).cast("string")) != 8),
                    None
                ).otherwise(to_date(col(dt_col).cast("string"), "yyyyMMdd"))
            )

        # Recalculate sls_sales if invalid
        df = df.withColumn(
            "sls_sales",
            when(
                (col("sls_sales").isNull()) | 
                (col("sls_sales") <= 0) | 
                (col("sls_sales") != col("sls_quantity") * abs(col("sls_price"))),
                col("sls_quantity") * abs(col("sls_price"))
            ).otherwise(col("sls_sales"))
        )

        # Derive sls_price if invalid
        df = df.withColumn(
            "sls_price",
            when(
                (col("sls_price").isNull()) | (col("sls_price") <= 0),
                col("sls_sales") / nullif(col("sls_quantity"), lit(0))
            ).otherwise(col("sls_price"))
        )

        # Add audit column
        df = df.withColumn("dwh_create_date", current_date())

        # Save to silver layer
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dwh_project.silver.crm_sales_details")

        # -------------------------------
        # 4. erp_cust_az12
        # -------------------------------
        print(">> Processing erp_cust_az12")
        df = spark.table("dwh_project.bronze.erp_cust_az12")

        # Clean cid by removing 'NAS' prefix
        df = df.withColumn(
            "cid",
            when(col("cid").startswith("NAS"), substring(col("cid"), 4, length(col("cid"))))
            .otherwise(col("cid"))
        )

        # Validate bdate
        df = df.withColumn(
            "bdate",
            when(col("bdate") > current_date(), None).otherwise(col("bdate"))
        )

        # Normalize gen
        df = df.withColumn(
            "gen",
            when(upper(trim(col("gen"))).isin("F", "FEMALE"), "Female")
            .when(upper(trim(col("gen"))).isin("M", "MALE"), "Male")
            .otherwise("n/a")
        )

        # Add audit column
        df = df.withColumn("dwh_create_date", current_date())

        # Save to silver layer
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dwh_project.silver.erp_cust_az12")

        # -------------------------------
        # 5. erp_loc_a101
        # -------------------------------
        print(">> Processing erp_loc_a101")
        df = spark.table("dwh_project.bronze.erp_loc_a101")

        # Clean cid by removing hyphens
        df = df.withColumn("cid", regexp_replace(col("cid"), "-", ""))

        # Normalize cntry
        df = df.withColumn(
            "cntry",
            when(trim(col("cntry")) == "DE", "Germany")
            .when(trim(col("cntry")).isin("US", "USA"), "United States")
            .when((trim(col("cntry")) == "") | col("cntry").isNull(), "n/a")
            .otherwise(trim(col("cntry")))
        )

        # Drop nulls and duplicates
        df = df.na.drop()
        df = df.dropDuplicates(["cid", "cntry"])

        # Add audit column
        df = df.withColumn("dwh_create_date", current_date())

        # Save to silver layer
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("dwh_project.silver.erp_loc_a101")

        # -------------------------------
        # 6. erp_px_cat_g1v2
        # -------------------------------
        print(">> Processing erp_px_cat_g1v2")
        df = spark.table("dwh_project.bronze.erp_px_cat_g1v2")

        # Add audit column
        df = df.withColumn("dwh_create_date", current_date())

        # Save to silver layer
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