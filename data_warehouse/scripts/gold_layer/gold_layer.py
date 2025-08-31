# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer Views Creation Script
# MAGIC
# MAGIC ##### This script creates curated views in the gold schema of the dwh_project catalog, following a star schema design for analytical purposes. 
# MAGIC
# MAGIC ##### It defines three views: dim_customers, dim_products, and fact_sales. The script joins data from the silver schema tables (crm_cust_info, crm_prd_info, crm_sales_details, erp_cust_az12, erp_loc_a101, erp_px_cat_g1v2) to create dimension and fact tables, incorporating surrogate keys (customer_key, product_key) and relevant attributes.
# MAGIC
# MAGIC ##### It ensures the gold schema exists, logs creation durations for each view, and includes error handling for robust execution. A final SQL query is included to preview the fact_sales view.

# COMMAND ----------

from pyspark.sql import SparkSession
import time

def create_gold_views():
    batch_start_time = time.time()
    print('================================================')
    print('Creating Gold Layer Views')
    print('================================================')

    try:
        # Create the gold schema if it doesn't exist
        spark.sql("CREATE SCHEMA IF NOT EXISTS dwh_project.gold")

        print('------------------------------------------------')
        print('Creating Gold Layer Views')
        print('------------------------------------------------')

        # Create dim_customers view
        print('>> Creating View: dwh_project.gold.dim_customers')
        start_time = time.time()
        spark.sql("""
        CREATE OR REPLACE VIEW dwh_project.gold.dim_customers AS
        SELECT
            ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
            ci.cst_id AS customer_id,
            ci.cst_key AS customer_number,
            ci.cst_firstname AS first_name,
            ci.cst_lastname AS last_name,
            la.cntry AS country,
            ci.cst_marital_status AS marital_status,
            CASE 
                WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
                ELSE COALESCE(ca.gen, 'n/a')
            END AS gender,
            ca.bdate AS birthdate,
            ci.cst_create_date AS create_date
        FROM dwh_project.silver.crm_cust_info ci
        LEFT JOIN dwh_project.silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
        LEFT JOIN dwh_project.silver.erp_loc_a101 la ON ci.cst_key = la.cid
        """)
        end_time = time.time()
        print('>> Creation Duration: ' + str(int(end_time - start_time)) + ' seconds')
        print('>> -------------')

        # Create dim_products view
        print('>> Creating View: dwh_project.gold.dim_products')
        start_time = time.time()
        spark.sql("""
        CREATE OR REPLACE VIEW dwh_project.gold.dim_products AS
        SELECT
            ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
            pn.prd_id AS product_id,
            pn.prd_key AS product_number,
            pn.prd_nm AS product_name,
            pn.cat_id AS category_id,
            pc.cat AS category,
            pc.subcat AS subcategory,
            pc.maintenance,
            pn.prd_cost AS cost,
            pn.prd_line AS product_line,
            pn.prd_start_dt AS start_date
        FROM dwh_project.silver.crm_prd_info pn
        LEFT JOIN dwh_project.silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
        WHERE pn.prd_end_dt IS NULL
        """)
        end_time = time.time()
        print('>> Creation Duration: ' + str(int(end_time - start_time)) + ' seconds')
        print('>> -------------')

        # Create fact_sales view
        print('>> Creating View: dwh_project.gold.fact_sales')
        start_time = time.time()
        spark.sql("""
        CREATE OR REPLACE VIEW dwh_project.gold.fact_sales AS
        SELECT
            sd.sls_ord_num AS order_number,
            pr.product_key,
            cu.customer_key,
            sd.sls_order_dt AS order_date,
            sd.sls_ship_dt AS shipping_date,
            sd.sls_due_dt AS due_date,
            sd.sls_sales AS sales_amount,
            sd.sls_quantity AS quantity,
            sd.sls_price AS price
        FROM dwh_project.silver.crm_sales_details sd
        LEFT JOIN dwh_project.gold.dim_products pr ON sd.sls_prd_key = pr.product_number
        LEFT JOIN dwh_project.gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id
        """)
        end_time = time.time()
        print('>> Creation Duration: ' + str(int(end_time - start_time)) + ' seconds')
        print('>> -------------')

        batch_end_time = time.time()
        print('==========================================')
        print('Creating Gold Layer Views is Completed')
        print('   - Total Creation Duration: ' + str(int(batch_end_time - batch_start_time)) + ' seconds')
        print('==========================================')

    except Exception as e:
        print('==========================================')
        print('ERROR OCCURRED DURING CREATING GOLD LAYER VIEWS')
        print('Error Message: ' + str(e))
        print('==========================================')

# Create catalog if it doesn't exist
try:
    spark.sql("CREATE CATALOG IF NOT EXISTS dwh_project")
except Exception as e:
    print(f"Warning: Could not create catalog 'dwh_project'. Error: {str(e)}. Ensure catalog exists or use hive_metastore.")


create_gold_views()