# Databricks notebook source
# MAGIC %md
# MAGIC #### Bronze Layer Incremental Load Script
# MAGIC  
# MAGIC This script performs incremental loading of raw data into the bronze schema of the dwh_project catalog using Delta Lake MERGE operations.
# MAGIC
# MAGIC It processes incremental CSV files for three CRM tables (crm_cust_info, crm_prd_info, crm_sales_details) and three ERP tables (erp_loc_a101, erp_cust_az12, erp_px_cat_g1v2) from the /Volumes/dwh_project/default/data_source/incremental_data/ directory.
# MAGIC
# MAGIC The script uses MERGE to upsert data based on unique keys, updates based on merge_key, adds a load_timestamp for tracking incremental loads, logs durations, and includes error handling.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from delta.tables import DeltaTable
import time

# Ensure catalog and schema exist
spark.sql("CREATE CATALOG IF NOT EXISTS dwh_project")
spark.sql("CREATE SCHEMA IF NOT EXISTS dwh_project.bronze")

# COMMAND ----------

def load_bronze_incremental(new_data_path, table_name, merge_key, timestamp_col=None):
    """
    Load incremental data into a Bronze Delta table using MERGE, updating based on merge_key.
    
    Args:
        new_data_path (str): Path to incremental CSV file
        table_name (str): Target Bronze table name (e.g., 'crm_cust_info')
        merge_key (str): Column for MERGE condition (e.g., 'cst_id')
        timestamp_col (str, optional): Column for deduplication (e.g., 'cst_create_date'). If None, deduplicate using source load_timestamp.
    """
    start_time = time.time()
    print(f">> Loading incremental data for dwh_project.bronze.{table_name}")
    
    try:
        # Read incremental CSV and add load_timestamp
        df_new = spark.read.csv(new_data_path, header=True, inferSchema=True) \
                      .withColumn("load_timestamp", current_timestamp())
        
        # Deduplicate source data based on merge_key
        if timestamp_col:
            # Use timestamp_col for deduplication if provided
            window_spec = Window.partitionBy(merge_key).orderBy(col(timestamp_col).desc_nulls_last())
        else:
            # Use source load_timestamp for arbitrary deduplication
            window_spec = Window.partitionBy(merge_key).orderBy(col("load_timestamp").desc_nulls_last())
        
        df_deduped = df_new.withColumn("rn", row_number().over(window_spec)) \
                           .filter(col("rn") == 1) \
                           .drop("rn")
        
        # Check if target table exists
        if spark.catalog.tableExists(f"dwh_project.bronze.{table_name}"):
            # MERGE into existing Delta table
            delta_table = DeltaTable.forName(spark, f"dwh_project.bronze.{table_name}")
            # Match only on merge_key (target tables lack load_timestamp)
            merge_condition = f"target.{merge_key} = source.{merge_key}"
            
            delta_table.alias("target").merge(
                df_deduped.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            # Initial load (fallback if table doesn't exist)
            print(f">> Warning: Table dwh_project.bronze.{table_name} does not exist. Performing initial load.")
            df_deduped.write.format("delta").mode("overwrite").saveAsTable(f"dwh_project.bronze.{table_name}")
        
        end_time = time.time()
        print(f">> Load Duration: {int(end_time - start_time)} seconds")
        print('>> -------------')
    
    except Exception as e:
        print(f">> Error loading {table_name}: {str(e)}")
        print('>> -------------')

# COMMAND ----------

def load_bronze():
    """
    Main function to load incremental data for all Bronze tables.
    """
    batch_start_time = time.time()
    print('================================================')
    print('Loading Bronze Layer (Incremental)')
    print('================================================')
    
    try:
        print('------------------------------------------------')
        print('Loading CRM Tables')
        print('------------------------------------------------')
        
        # CRM Tables
        load_bronze_incremental(
            '/Volumes/dwh_project/default/data_source/incremental_data/cust_info.csv',
            'crm_cust_info',
            'cst_id',
            timestamp_col='cst_create_date'
        )
        load_bronze_incremental(
            '/Volumes/dwh_project/default/data_source/incremental_data/prd_info.csv',
            'crm_prd_info',
            'prd_key',
            timestamp_col='prd_start_dt'
        )
        load_bronze_incremental(
            '/Volumes/dwh_project/default/data_source/incremental_data/sales_details.csv',
            'crm_sales_details',
            'sls_ord_num',
            timestamp_col='sls_order_dt'
        )
        
        print('------------------------------------------------')
        print('Loading ERP Tables')
        print('------------------------------------------------')
        
        # ERP Tables
        load_bronze_incremental(
            '/Volumes/dwh_project/default/data_source/incremental_data/LOC_A101.csv',
            'erp_loc_a101',
            'CID',
            timestamp_col=None
        )
        load_bronze_incremental(
            '/Volumes/dwh_project/default/data_source/incremental_data/CUST_AZ12.csv',
            'erp_cust_az12',
            'CID',
            timestamp_col='BDATE'
        )
        load_bronze_incremental(
            '/Volumes/dwh_project/default/data_source/incremental_data/PX_CAT_G1V2.csv',
            'erp_px_cat_g1v2',
            'ID',
            timestamp_col=None
        )
        
        batch_end_time = time.time()
        print('==========================================')
        print('Loading Bronze Layer (Incremental) Completed')
        print(f'   - Total Load Duration: {int(batch_end_time - batch_start_time)} seconds')
        print('==========================================')
    
    except Exception as e:
        print('==========================================')
        print('ERROR OCCURRED DURING INCREMENTAL LOADING BRONZE LAYER')
        print(f'Error Message: {str(e)}')
        print('==========================================')

# Run the function
load_bronze()