# Databricks notebook source
# MAGIC %md
# MAGIC #### Bronze Layer Streaming Load Script
# MAGIC  
# MAGIC This script performs streaming ingestion of raw data into the bronze schema of the dwh_project catalog using PySpark Structured Streaming with Delta Lake in Databricks.
# MAGIC
# MAGIC It processes streaming CSV files from six directories for CRM tables (crm_cust_info, crm_prd_info, crm_sales_details) and ERP tables (erp_loc_a101, erp_cust_az12, erp_px_cat_g1v2) located in /Volumes/dwh_project/default/data_source/streaming_data/.
# MAGIC
# MAGIC The script uses Structured Streaming with explicit schemas, adds a load_timestamp, deduplicates within micro-batches using row_number, and upserts into Delta tables using foreachBatch with MERGE operations based on unique keys.
# MAGIC
# MAGIC Note: This script uses `trigger(once=True)` for compatibility with Databricks Community Edition, which does not support continuous streaming triggers. This processes all files in a single batch and stops. For continuous streaming, a paid Databricks cluster is required.
# MAGIC
# MAGIC The script creates a checkpoint volume (/Volumes/dwh_project/default/checkpoints/) to store streaming state. Data validation and enhanced logging ensure robustness.
# MAGIC
# MAGIC Target tables are dropped before writing to ensure clean schemas.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from delta.tables import DeltaTable
import time
from pyspark.sql.streaming import StreamingQueryException
import traceback

# Ensure catalog, schema, and checkpoint volume exist
spark.sql("CREATE CATALOG IF NOT EXISTS dwh_project")
spark.sql("CREATE SCHEMA IF NOT EXISTS dwh_project.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS dwh_project.default.checkpoints")

# Define schemas for each table
crm_cust_info_schema = StructType([
    StructField("cst_id", StringType(), False),
    StructField("cst_name", StringType(), True),
    StructField("cst_email", StringType(), True),
    StructField("cst_create_date", TimestampType(), True)
])

crm_prd_info_schema = StructType([
    StructField("prd_key", StringType(), True),
    StructField("prd_name", StringType(), True),
    StructField("prd_price", DoubleType(), True),
    StructField("prd_start_dt", TimestampType(), True)
])

crm_sales_details_schema = StructType([
    StructField("sls_ord_num", StringType(), False),
    StructField("cst_id", StringType(), True),
    StructField("prd_key", StringType(), True),
    StructField("sls_amount", DoubleType(), True),
    StructField("sls_order_dt", TimestampType(), True)
])

erp_loc_a101_schema = StructType([
    StructField("CID", StringType(), False),
    StructField("location_name", StringType(), True),
    StructField("region", StringType(), True)
])

erp_cust_az12_schema = StructType([
    StructField("CID", StringType(), False),
    StructField("cust_name", StringType(), True),
    StructField("BDATE", TimestampType(), True)
])

erp_px_cat_g1v2_schema = StructType([
    StructField("ID", StringType(), False),
    StructField("category_name", StringType(), True),
    StructField("price", DoubleType(), True)
])

def process_batch(batch_df, batch_id, table_name, merge_key, timestamp_col=None, debug_mode=False):
    """
    Process a micro-batch DataFrame for upsert into a Bronze Delta table using MERGE or direct write (debug mode).
    
    Args:
        batch_df: The micro-batch DataFrame
        batch_id: The batch ID (for logging)
        table_name (str): Target Bronze table name (e.g., 'crm_prd_info')
        merge_key (str): Column for MERGE condition (e.g., 'prd_key')
        timestamp_col (str, optional): Column for deduplication (e.g., 'prd_start_dt'). If None, use load_timestamp.
        debug_mode (bool): If True, write directly to Delta table without MERGE to avoid toPandas() issues.
    """
    print(f">> Processing batch {batch_id} for dwh_project.bronze.{table_name}")
    
    try:
        # Check if the batch DataFrame is empty
        row_count = batch_df.count()
        if row_count == 0:
            print(f">> Skipping batch {batch_id} for {table_name}: Empty DataFrame")
            return
        
        # Log schema and sample data for debugging
        print(f">> Batch {batch_id} schema: {batch_df.schema}")
        print(f">> Batch {batch_id} row count: {row_count}")
        print(f">> Batch {batch_id} sample data (first 5 rows):")
        batch_df.show(5, truncate=False)
        
        # Validate data: Filter out rows with NULL merge_key
        if merge_key:
            batch_df = batch_df.filter(col(merge_key).isNotNull())
            filtered_count = batch_df.count()
            if filtered_count == 0:
                print(f">> Skipping batch {batch_id} for {table_name}: All rows have NULL {merge_key}")
                return
            print(f">> Batch {batch_id} after filtering NULL {merge_key}: {filtered_count} rows")
        
        # Add load_timestamp if not already present
        if "load_timestamp" not in batch_df.columns:
            batch_df = batch_df.withColumn("load_timestamp", current_timestamp())
        
        # Deduplicate within the batch based on merge_key
        if timestamp_col:
            window_spec = Window.partitionBy(merge_key).orderBy(col(timestamp_col).desc_nulls_last())
        else:
            window_spec = Window.partitionBy(merge_key).orderBy(col("load_timestamp").desc_nulls_last())
        
        df_deduped = batch_df.withColumn("rn", row_number().over(window_spec)) \
                             .filter(col("rn") == 1) \
                             .drop("rn")
        
        # Check if target table exists; if not, create an empty Delta table
        if not spark.catalog.tableExists(f"dwh_project.bronze.{table_name}"):
            print(f">> Warning: Table dwh_project.bronze.{table_name} does not exist. Creating empty Delta table.")
            # Create an empty DataFrame with the appropriate schema including load_timestamp
            schema_with_timestamp = df_deduped.schema
            spark.createDataFrame([], schema_with_timestamp).write.format("delta").mode("overwrite").saveAsTable(f"dwh_project.bronze.{table_name}")
        
        # Debug mode: Write directly to Delta table
        if debug_mode:
            print(f">> Debug mode enabled: Writing batch {batch_id} directly to dwh_project.bronze.{table_name}")
            df_deduped.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"dwh_project.bronze.{table_name}")
        else:
            # MERGE into existing Delta table
            delta_table = DeltaTable.forName(spark, f"dwh_project.bronze.{table_name}")
            merge_condition = f"target.{merge_key} = source.{merge_key}"
            
            delta_table.alias("target").merge(
                df_deduped.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        
        print(f">> Batch {batch_id} processed successfully for {table_name}")
    
    except Exception as e:
        print(f">> Error processing batch {batch_id} for {table_name}: {str(e)}")
        print(f">> Stack trace: {traceback.format_exc()}")
        raise  # Re-raise to fail the batch

def start_bronze_stream(stream_path, table_name, merge_key, schema, timestamp_col=None, debug_mode=False, options={}):
    """
    Start a Structured Streaming query to load data from a directory into a Bronze Delta table.
    
    Args:
        stream_path (str): Directory path for streaming CSV files
        table_name (str): Target Bronze table name
        merge_key (str): Column for MERGE condition
        schema (StructType): Schema for the CSV files
        timestamp_col (str, optional): Column for deduplication
        debug_mode (bool): If True, use direct write instead of MERGE
        options (dict, optional): Additional readStream options
    
    Returns:
        StreamingQuery: The started streaming query or None if directory is invalid
    """
    print(f">> Starting stream for dwh_project.bronze.{table_name} from {stream_path}")
    
    # Pre-flight check: Validate CSV file schema
    try:
        files = dbutils.fs.ls(stream_path)
        print(f">> Files in {stream_path}: {[f.path for f in files]}")
        if not files:
            print(f">> Warning: No files found in {stream_path}. Stream for {table_name} may process no data.")
            return None
        
        # Read a sample file to validate schema
        sample_file = files[0].path
        sample_df = spark.read.csv(sample_file, header=True, schema=schema)
        print(f">> Sample file {sample_file} schema: {sample_df.schema}")
        print(f">> Sample file data (first 5 rows):")
        sample_df.show(5, truncate=False)
    except Exception as e:
        print(f">> Error validating {stream_path}: {str(e)}")
        return None
    
    # Read stream from directory with explicit schema
    df_stream = spark.readStream \
                     .format("csv") \
                     .option("header", "true") \
                     .schema(schema) \
                     .load(stream_path) \
                     .withColumn("load_timestamp", current_timestamp())
    
    # Use trigger(once=True) for Community Edition
    query = df_stream.writeStream \
                     .format("delta") \
                     .outputMode("append") \
                     .option("checkpointLocation", f"/Volumes/dwh_project/default/checkpoints/{table_name}_checkpoint") \
                     .trigger(once=True) \
                     .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, table_name, merge_key, timestamp_col, debug_mode)) \
                     .options(**options) \
                     .start()
    
    return query

def start_all_bronze_streams():
    """
    Main function to start streaming queries for all Bronze tables.
    Uses trigger(once=True) for Community Edition.
    """
    batch_start_time = time.time()
    print('================================================')
    print('Starting Bronze Layer Streaming Ingestion')
    print('================================================')
    
    try:
        # List of (path, table_name, merge_key, schema, timestamp_col, debug_mode) for each table
        streams_config = [
            # CRM Tables
            ('/Volumes/dwh_project/default/data_source/streaming_data/cust_info/', 'crm_cust_info', 'cst_id', crm_cust_info_schema, 'cst_create_date', False),
            ('/Volumes/dwh_project/default/data_source/streaming_data/prd_info/', 'crm_prd_info', 'prd_key', crm_prd_info_schema, 'prd_start_dt', False),
            ('/Volumes/dwh_project/default/data_source/streaming_data/sales_details/', 'crm_sales_details', 'sls_ord_num', crm_sales_details_schema, 'sls_order_dt', False),
            # ERP Tables
            ('/Volumes/dwh_project/default/data_source/streaming_data/LOC_A101/', 'erp_loc_a101', 'CID', erp_loc_a101_schema, None, False),
            ('/Volumes/dwh_project/default/data_source/streaming_data/CUST_AZ12/', 'erp_cust_az12', 'CID', erp_cust_az12_schema, 'BDATE', False),
            ('/Volumes/dwh_project/default/data_source/streaming_data/PX_CAT_G1V2/', 'erp_px_cat_g1v2', 'ID', erp_px_cat_g1v2_schema, None, False)
        ]
        
        # Start streaming queries
        queries = []
        for path, table_name, merge_key, schema, timestamp_col, debug_mode in streams_config:
            query = start_bronze_stream(path, table_name, merge_key, schema, timestamp_col, debug_mode)
            if query is not None:
                queries.append(query)
        
        print(f">> Started {len(queries)} streaming queries for Bronze tables.")
        print('================================================')
        print('Streams are running with trigger(once=True). They will process all available files in a single batch and stop.')
        print('Check the Databricks UI for progress or errors.')
        print('================================================')
        
        # Await termination for all queries
        for query in queries:
            try:
                query.awaitTermination()
            except StreamingQueryException as e:
                print(f">> Streaming query for {query.name or 'unknown'} failed: {str(e)}")
                query.stop()
        
        batch_end_time = time.time()
        print('==========================================')
        print('Bronze Layer Streaming Ingestion Completed')
        print(f'   - Total Duration: {int(batch_end_time - batch_start_time)} seconds')
        print('==========================================')
    
    except Exception as e:
        print('==========================================')
        print('ERROR OCCURRED DURING BRONZE STREAMING INGESTION')
        print(f'Error Message: {str(e)}')
        print('==========================================')

# Run the function to start streams
start_all_bronze_streams()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dwh_project.bronze.erp_loc_a101;