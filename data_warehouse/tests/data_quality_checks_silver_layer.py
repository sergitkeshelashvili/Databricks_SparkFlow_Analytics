# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver Layer Data Quality Checks
# MAGIC
# MAGIC ##### Purpose
# MAGIC
# MAGIC This PySpark script performs quality checks on Silver Layer tables (crm_sales_details, erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2) in a Spark/Databricks environment to ensure data accuracy and consistency for gold layer processing. Checks include:
# MAGIC
# MAGIC Invalid/out-of-range dates (e.g., sales dates outside 1900-2050, birthdates before 1924).
# MAGIC Incorrect date order (e.g., order date after shipping/due date).
# MAGIC Inconsistent calculations (e.g., sales ≠ quantity * price).
# MAGIC Unwanted spaces in strings (e.g., category, subcategory).
# MAGIC Non-standardized categorical values (e.g., gender, country).
# MAGIC
# MAGIC ##### Usage
# MAGIC
# MAGIC Run: Execute silver_quality_checks.py in a Databricks notebook or PySpark environment after loading Silver Layer data. Ensure SparkSession is active and tables are accessible (e.g., silver schema).
# MAGIC Review: Check console output (via .show() and counts) for flagged issues. Non-zero counts indicate problems.
# MAGIC Resolve: Investigate and fix issues (e.g., data cleansing, ETL updates) before gold layer transformations.
# MAGIC Notes: Assumes parseable date formats (e.g., YYYYMMDD, YYYY-MM-DD). Adjust parsing if needed. Add checks for referential integrity if gold layer joins produce NULLs.
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, to_date, current_date, trim

# Initialize SparkSession (assumed to be already created as 'spark')
# spark = SparkSession.builder.appName("SilverLayerQualityChecks").getOrCreate()

def run_silver_quality_checks():
    """
    Performs quality assurance checks on the silver layer tables to ensure data consistency,
    accuracy, and adherence to standards. Checks include nulls, duplicates, invalid dates,
    data standardization, and consistency across related fields.
    """
    print("=" * 50)
    print("Starting Silver Layer Quality Checks")
    print("=" * 50)

    # Check 1: Invalid Dates in silver.crm_sales_details
    print("\n-- Checking 'silver.crm_sales_details' for Invalid Dates")
    print("-- Expectation: No Invalid Dates")
    invalid_dates = spark.sql("""
    SELECT 
        NULLIF(sls_due_dt, 0) AS sls_due_dt 
    FROM dwh_project.bronze.crm_sales_details
    WHERE sls_due_dt <= 0 
        OR LENGTH(CAST(sls_due_dt AS VARCHAR(8))) != 8 
        OR sls_due_dt > 20500101 
        OR sls_due_dt < 19000101
 
    """)
    invalid_dates.show(truncate=False)
    print(f"Invalid Dates Count: {invalid_dates.count()}")

    # Check 2: Invalid Date Orders in silver.crm_sales_details
    print("\n-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)")
    print("-- Expectation: No Results")
    invalid_date_orders = spark.sql("""
        SELECT *
        FROM dwh_project.silver.crm_sales_details
        WHERE sls_order_dt > sls_ship_dt 
           OR sls_order_dt > sls_due_dt
    """)
    invalid_date_orders.show(truncate=False)
    print(f"Invalid Date Orders Count: {invalid_date_orders.count()}")

    # Check 3: Data Consistency in silver.crm_sales_details (Sales = Quantity * Price)
    print("\n-- Check Data Consistency: Sales = Quantity * Price")
    print("-- Expectation: No Results")
    sales_consistency = spark.sql("""
        SELECT DISTINCT 
            sls_sales,
            sls_quantity,
            sls_price
        FROM dwh_project.silver.crm_sales_details
        WHERE sls_sales != sls_quantity * sls_price
           OR sls_sales IS NULL 
           OR sls_quantity IS NULL 
           OR sls_price IS NULL
           OR sls_sales <= 0 
           OR sls_quantity <= 0 
           OR sls_price <= 0
        ORDER BY sls_sales, sls_quantity, sls_price
    """)
    sales_consistency.show(truncate=False)
    print(f"Inconsistent Sales Records Count: {sales_consistency.count()}")

    # Check 4: Out-of-Range Birthdates in silver.erp_cust_az12
    print("\n-- Checking 'silver.erp_cust_az12' for Out-of-Range Birthdates")
    print("-- Expectation: Birthdates between 1924-01-01 and Today")
    invalid_birthdates = spark.sql("""
        SELECT DISTINCT 
            bdate
        FROM dwh_project.silver.erp_cust_az12
        WHERE bdate < '1924-01-01' 
           OR bdate > CURRENT_DATE
    """)
    invalid_birthdates.show(truncate=False)
    print(f"Out-of-Range Birthdates Count: {invalid_birthdates.count()}")

    # Check 5: Data Standardization in silver.erp_cust_az12 (Gender)
    print("\n-- Checking 'silver.erp_cust_az12' for Gender Standardization")
    gender_standardization = spark.sql("""
        SELECT DISTINCT 
            gen
        FROM dwh_project.silver.erp_cust_az12
    """)
    gender_standardization.show(truncate=False)
    print(f"Distinct Gender Values Count: {gender_standardization.count()}")

    # Check 6: Data Standardization in silver.erp_loc_a101 (Country)
    print("\n-- Checking 'silver.erp_loc_a101' for Country Standardization")
    country_standardization = spark.sql("""
        SELECT DISTINCT 
            cntry
        FROM dwh_project.silver.erp_loc_a101
        ORDER BY cntry
    """)
    country_standardization.show(truncate=False)
    print(f"Distinct Country Values Count: {country_standardization.count()}")

    # Check 7: Unwanted Spaces in silver.erp_px_cat_g1v2
    print("\n-- Checking 'silver.erp_px_cat_g1v2' for Unwanted Spaces")
    print("-- Expectation: No Results")
    unwanted_spaces = spark.sql("""
        SELECT *
        FROM dwh_project.silver.erp_px_cat_g1v2
        WHERE cat != TRIM(cat) 
           OR subcat != TRIM(subcat) 
           OR maintenance != TRIM(maintenance)
    """)
    unwanted_spaces.show(truncate=False)
    print(f"Records with Unwanted Spaces Count: {unwanted_spaces.count()}")

    # Check 8: Data Standardization in silver.erp_px_cat_g1v2 (Maintenance)
    print("\n-- Checking 'silver.erp_px_cat_g1v2' for Maintenance Standardization")
    maintenance_standardization = spark.sql("""
        SELECT DISTINCT 
            maintenance
        FROM dwh_project.silver.erp_px_cat_g1v2
    """)
    maintenance_standardization.show(truncate=False)
    print(f"Distinct Maintenance Values Count: {maintenance_standardization.count()}")

    print("\n" + "=" * 50)
    print("Silver Layer Quality Checks Completed")
    print("=" * 50)

# Execute the quality checks
run_silver_quality_checks()