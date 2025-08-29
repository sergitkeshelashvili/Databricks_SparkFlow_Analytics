# Databricks notebook source
# MAGIC %md
# MAGIC ##Bronze Layer Data Load Script
# MAGIC
# MAGIC ##### This script is designed to load raw data into the bronze schema of the dwh_project catalog in a data warehouse. It creates the catalog and schema if they do not exist, then ingests data from CSV files. 
# MAGIC
# MAGIC ##### The script processes three CRM tables (crm_cust_info, crm_prd_info, crm_sales_details) and three ERP tables (erp_loc_a101, erp_cust_az12, erp_px_cat_g1v2). It includes error handling, logs load durations for each table, and reports the total execution time for the bronze layer loading process.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Create calalog dwh_project & the bronze schema in the dwh_project catalog

spark.sql("CREATE CATALOG IF NOT EXISTS dwh_project")
spark.sql("CREATE SCHEMA IF NOT EXISTS dwh_project.bronze")

# COMMAND ----------

from pyspark.sql import SparkSession
import time

def load_bronze():
    batch_start_time = time.time()
    print('================================================')
    print('Loading Bronze Layer')
    print('================================================')

    try:
        print('------------------------------------------------')
        print('Loading CRM Tables')
        print('------------------------------------------------')

        # Load crm_cust_info
        start_time = time.time()
        print('>> Loading Table: dwh_project.bronze.crm_cust_info')
        df = spark.read.csv('/Volumes/dwh_project/default/data_source/source_crm/cust_info.csv', header=True, inferSchema=True)
        df.write.format("delta").mode("overwrite").saveAsTable("dwh_project.bronze.crm_cust_info")
        end_time = time.time()
        print('>> Load Duration: ' + str(int(end_time - start_time)) + ' seconds')
        print('>> -------------')

        # Load crm_prd_info
        start_time = time.time()
        print('>> Loading Table: dwh_project.bronze.crm_prd_info')
        df = spark.read.csv('/Volumes/dwh_project/default/data_source/source_crm/prd_info.csv', header=True, inferSchema=True)
        df.write.format("delta").mode("overwrite").saveAsTable("dwh_project.bronze.crm_prd_info")
        end_time = time.time()
        print('>> Load Duration: ' + str(int(end_time - start_time)) + ' seconds')
        print('>> -------------')

        # Load crm_sales_details
        start_time = time.time()
        print('>> Loading Table: dwh_project.bronze.crm_sales_details')
        df = spark.read.csv('/Volumes/dwh_project/default/data_source/source_crm/sales_details.csv', header=True, inferSchema=True)
        df.write.format("delta").mode("overwrite").saveAsTable("dwh_project.bronze.crm_sales_details")
        end_time = time.time()
        print('>> Load Duration: ' + str(int(end_time - start_time)) + ' seconds')
        print('>> -------------')

        print('------------------------------------------------')
        print('Loading ERP Tables')
        print('------------------------------------------------')

        # Load erp_loc_a101
        start_time = time.time()
        print('>> Loading Table: dwh_project.bronze.erp_loc_a101')
        df = spark.read.csv('/Volumes/dwh_project/default/data_source/source_erp/LOC_A101.csv', header=True, inferSchema=True)
        df.write.format("delta").mode("overwrite").saveAsTable("dwh_project.bronze.erp_loc_a101")
        end_time = time.time()
        print('>> Load Duration: ' + str(int(end_time - start_time)) + ' seconds')
        print('>> -------------')

        # Load erp_cust_az12
        start_time = time.time()
        print('>> Loading Table: dwh_project.bronze.erp_cust_az12')
        df = spark.read.csv('/Volumes/dwh_project/default/data_source/source_erp/CUST_AZ12.csv', header=True, inferSchema=True)
        df.write.format("delta").mode("overwrite").saveAsTable("dwh_project.bronze.erp_cust_az12")
        end_time = time.time()
        print('>> Load Duration: ' + str(int(end_time - start_time)) + ' seconds')
        print('>> -------------')

        # Load erp_px_cat_g1v2
        start_time = time.time()
        print('>> Loading Table: dwh_project.bronze.erp_px_cat_g1v2')
        df = spark.read.csv('/Volumes/dwh_project/default/data_source/source_erp/PX_CAT_G1V2.csv', header=True, inferSchema=True)
        df.write.format("delta").mode("overwrite").saveAsTable("dwh_project.bronze.erp_px_cat_g1v2")
        end_time = time.time()
        print('>> Load Duration: ' + str(int(end_time - start_time)) + ' seconds')
        print('>> -------------')

        batch_end_time = time.time()
        print('==========================================')
        print('Loading Bronze Layer is Completed')
        print('   - Total Load Duration: ' + str(int(batch_end_time - batch_start_time)) + ' seconds')
        print('==========================================')

    except Exception as e:
        print('==========================================')
        print('ERROR OCCURRED DURING LOADING BRONZE LAYER')
        print('Error Message: ' + str(e))
        print('==========================================')


# Run the function
load_bronze()