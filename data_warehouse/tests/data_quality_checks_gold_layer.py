# Databricks notebook source
from pyspark.sql import SparkSession

# Initialize SparkSession (assumed to be already created as 'spark')
# spark = SparkSession.builder.appName("GoldLayerQualityChecks").getOrCreate()

def run_gold_quality_checks():
    """
    Performs quality assurance checks on the Gold Layer tables to ensure data integrity,
    consistency, and readiness for analytical use. Checks include surrogate key uniqueness
    and referential integrity between fact and dimension tables.
    """
    print("=" * 50)
    print("Starting Gold Layer Quality Checks")
    print("=" * 50)

    # Check 1: Uniqueness of Customer Key in gold.dim_customers
    print("\n-- Checking 'gold.dim_customers' for Uniqueness of Customer Key")
    print("-- Expectation: No Results")
    duplicate_customers = spark.sql("""
        SELECT 
            customer_key,
            COUNT(*) AS duplicate_count
        FROM dwh_project.gold.dim_customers
        GROUP BY customer_key
        HAVING COUNT(*) > 1
    """)
    duplicate_customers.show(truncate=False)
    print(f"Duplicate Customer Keys Count: {duplicate_customers.count()}")

    # Check 2: Uniqueness of Product Key in gold.dim_products
    print("\n-- Checking 'gold.dim_products' for Uniqueness of Product Key")
    print("-- Expectation: No Results")
    duplicate_products = spark.sql("""
        SELECT 
            product_key,
            COUNT(*) AS duplicate_count
        FROM dwh_project.gold.dim_products
        GROUP BY product_key
        HAVING COUNT(*) > 1
    """)
    duplicate_products.show(truncate=False)
    print(f"Duplicate Product Keys Count: {duplicate_products.count()}")

    # Check 3: Referential Integrity in gold.fact_sales
    print("\n-- Checking 'gold.fact_sales' for Data Model Connectivity")
    print("-- Expectation: No Unmatched Keys")
    unmatched_keys = spark.sql("""
        SELECT *
        FROM dwh_project.gold.fact_sales f
        LEFT JOIN dwh_project.gold.dim_customers c ON c.customer_key = f.customer_key
        LEFT JOIN dwh_project.gold.dim_products p ON p.product_key = f.product_key
        WHERE p.product_key IS NULL OR c.customer_key IS NULL
    """)
    unmatched_keys.show(truncate=False)
    print(f"Unmatched Keys Count: {unmatched_keys.count()}")

    print("\n" + "=" * 50)
    print("Gold Layer Quality Checks Completed")
    print("=" * 50)

# Execute the quality checks
run_gold_quality_checks()