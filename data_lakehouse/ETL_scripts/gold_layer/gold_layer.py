# Databricks notebook source
# MAGIC %md
# MAGIC #### Gold Layer Tables Creation
# MAGIC
# MAGIC #### Overview
# MAGIC This notebook creates curated **Delta tables** in the `gold` schema of the `dwh_project` catalog, following a **star schema** design optimized for analytics.
# MAGIC
# MAGIC The Gold layer is materialized as persistent **Delta tables**, ensuring improved performance, data consistency, and seamless integration with BI/reporting tools like Tableau. 
# MAGIC
# MAGIC #### Tables Created
# MAGIC
# MAGIC - **`dwh_project.gold.dim_customers`**  
# MAGIC   Contains customer attributes enriched from CRM and ERP sources, with a surrogate key `customer_key`.
# MAGIC
# MAGIC - **`dwh_project.gold.dim_products`**  
# MAGIC   Contains product information, including categories, subcategories, and costs, with a surrogate key `product_key`.
# MAGIC
# MAGIC - **`dwh_project.gold.fact_sales`**  
# MAGIC   Fact table capturing sales transactions, linked to `dim_customers` and `dim_products` via foreign keys for efficient querying.
# MAGIC
# MAGIC #### Data Cleansing
# MAGIC After performing joins, each DataFrame applies **`.dropna()`** and **dropDuplicates()** to remove records with null values and duplicates ensuring clean and consistent data in the Gold layer.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import time

# ------------------ Golden Layer Data Validation ------------------
class GoldenLayerDataValidation:
    def __init__(self, df: DataFrame):
        self.df = df

    def remove_nulls(self) -> DataFrame:
        """Remove all rows with any null values"""
        return self.df.na.drop()

    def dedup(self, partition_cols=None, order_col=None) -> DataFrame:
        """Deduplicate rows based on partition columns"""
        if partition_cols and order_col:
            window_spec = Window.partitionBy(partition_cols).orderBy(order_col)
            df_with_rownum = self.df.withColumn("row_num", row_number().over(window_spec))
            return df_with_rownum.filter(col("row_num") == 1).drop("row_num")
        else:
            return self.df.dropDuplicates()

# ------------------ Gold Layer Creation ------------------
def create_gold_tables():
    batch_start_time = time.time()
    print('================================================')
    print('Creating Gold Layer Tables (with nulls dropped & duplicates removed)')
    print('================================================')

    try:
        # Create the gold schema if it doesn't exist
        spark.sql("CREATE SCHEMA IF NOT EXISTS dwh_project.gold")

        # ------------------ DIM CUSTOMERS ------------------
        print('>> Creating Table: dwh_project.gold.dim_customers')
        start_time = time.time()
        dim_customers_df = spark.sql("""
            SELECT
                cst_id,
                cst_key,
                cst_firstname,
                cst_lastname,
                la.cntry AS country,
                cst_marital_status AS marital_status,
                CASE 
                    WHEN cst_gndr != 'n/a' THEN cst_gndr
                    ELSE COALESCE(ca.gen, 'n/a')
                END AS gender,
                ca.bdate AS birthdate,
                cst_create_date AS create_date
            FROM dwh_project.silver.crm_cust_info ci
            LEFT JOIN dwh_project.silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
            LEFT JOIN dwh_project.silver.erp_loc_a101 la ON ci.cst_key = la.cid
        """)

        # Clean & deduplicate
        validator = GoldenLayerDataValidation(dim_customers_df)
        dim_customers_df = validator.remove_nulls()
        dim_customers_df = validator.dedup(partition_cols=["cst_id"], order_col="cst_id")

        # Add surrogate key
        window_spec = Window.orderBy("cst_id")
        dim_customers_df = dim_customers_df.withColumn("customer_key", row_number().over(window_spec))

        # Align columns & rename for final schema
        dim_customers_df = dim_customers_df.select(
            "customer_key", "cst_id", "cst_key", "cst_firstname", "cst_lastname",
            "country", "marital_status", "gender", "birthdate", "create_date"
        ).withColumnRenamed("cst_id", "customer_id") \
         .withColumnRenamed("cst_key", "customer_number") \
         .withColumnRenamed("cst_firstname", "first_name") \
         .withColumnRenamed("cst_lastname", "last_name")

        dim_customers_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("dwh_project.gold.dim_customers")
        print(f">> Created dim_customers in {int(time.time() - start_time)} seconds")

        # ------------------ DIM PRODUCTS ------------------
        print('>> Creating Table: dwh_project.gold.dim_products')
        start_time = time.time()
        dim_products_df = spark.sql("""
            SELECT
                prd_id,
                prd_key,
                prd_nm AS product_name,
                cat_id,
                pc.cat AS category,
                pc.subcat AS subcategory,
                pc.maintenance,
                prd_cost AS cost,
                prd_line AS product_line,
                prd_start_dt AS start_date
            FROM dwh_project.silver.crm_prd_info pn
            LEFT JOIN dwh_project.silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
            WHERE pn.prd_end_dt IS NULL
        """)

        # Clean & deduplicate
        validator = GoldenLayerDataValidation(dim_products_df)
        dim_products_df = validator.remove_nulls()
        dim_products_df = validator.dedup(partition_cols=["prd_id"], order_col="start_date")

        # Add surrogate key using start_date and prd_key
        window_spec = Window.orderBy("start_date", "prd_key")
        dim_products_df = dim_products_df.withColumn("product_key", row_number().over(window_spec))

        # Align columns for final schema
        dim_products_df = dim_products_df.select(
            "product_key", "prd_id", "prd_key", "product_name", "cat_id",
            "category", "subcategory", "maintenance", "cost", "product_line", "start_date"
        ).withColumnRenamed("prd_id", "product_id") \
         .withColumnRenamed("prd_key", "product_number")

        dim_products_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("dwh_project.gold.dim_products")
        print(f">> Created dim_products in {int(time.time() - start_time)} seconds")

        # ------------------ FACT SALES ------------------
        print('>> Creating Table: dwh_project.gold.fact_sales')
        start_time = time.time()
        fact_sales_df = spark.sql("""
            SELECT
                sd.sls_ord_num AS order_number,
                sd.sls_prd_key AS product_number,
                sd.sls_cust_id AS customer_id,
                sd.sls_order_dt AS order_date,
                sd.sls_ship_dt AS shipping_date,
                sd.sls_due_dt AS due_date,
                sd.sls_sales AS sales_amount,
                sd.sls_quantity AS quantity,
                sd.sls_price AS price
            FROM dwh_project.silver.crm_sales_details sd
        """)

        # Join with gold dimension tables
        fact_sales_df = fact_sales_df.join(
            dim_products_df.select("product_key", "product_number"),
            on="product_number",
            how="left"
        ).join(
            dim_customers_df.select("customer_key", "customer_id"),
            on="customer_id",
            how="left"
        )

        # Clean & deduplicate
        validator = GoldenLayerDataValidation(fact_sales_df)
        fact_sales_df = validator.remove_nulls()
        fact_sales_df = validator.dedup(partition_cols=["order_number"], order_col="order_number")

        # Write fact table
        fact_sales_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("dwh_project.gold.fact_sales")
        print(f">> Created fact_sales in {int(time.time() - start_time)} seconds")

        # ------------------ Finished ------------------
        batch_end_time = time.time()
        print('==========================================')
        print('Creating Gold Layer Tables Completed')
        print(f'   - Total Duration: {int(batch_end_time - batch_start_time)} seconds')
        print('==========================================')

    except Exception as e:
        print('==========================================')
        print('ERROR OCCURRED DURING CREATING GOLD LAYER TABLES')
        print('Error Message:', str(e))
        print('==========================================')

# ------------------ Create Catalog if Not Exists ------------------
try:
    spark.sql("CREATE CATALOG IF NOT EXISTS dwh_project")
except Exception as e:
    print(f"Warning: Could not create catalog 'dwh_project'. Error: {str(e)}. Ensure catalog exists or use hive_metastore.")

# ------------------ Run Gold Layer Creation ------------------
create_gold_tables()


# COMMAND ----------

# MAGIC %md
# MAGIC