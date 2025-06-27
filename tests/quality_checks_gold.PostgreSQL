/*
===============================================================================
Gold Layer Quality Checks
===============================================================================
Purpose:
    This script performs critical quality checks on the Gold Layer to ensure 
    data integrity, consistency, and readiness for analytical use. The checks include:

    - Ensuring surrogate keys in dimension tables are unique.
    - Verifying referential integrity between fact and dimension tables.
    - Validating the correctness of relationships defined in the data model.

Usage:
    - Run this script as part of the validation process after populating the Gold Layer.
    - Review and investigate any issues identified during these checks.
    - Address discrepancies to maintain a high-quality, reliable data model for analysis.

===============================================================================
*/


-- Checking 'gold.dim_customers'
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results

SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

==================================================

-- Checking 'gold.product_key'
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 

SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

==================================================

-- Checking 'gold.fact_sales'
-- Check the data model connectivity between fact and dimensions

SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  








