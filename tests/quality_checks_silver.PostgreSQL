/*
===============================================================================
Quality Checks Overview
===============================================================================
Purpose:
    This script is designed to perform a series of quality assurance checks on 
    the 'silver' data layer to ensure data consistency, accuracy, and adherence 
    to defined standards. The checks performed include:

    - Detection of null or duplicate primary key values.
    - Identification of unwanted leading or trailing spaces in string fields.
    - Validation of data standardization and consistency across columns.
    - Verification of valid date ranges and proper chronological order.
    - Consistency checks between related or dependent fields.

Usage:
    - Execute this script after loading data into the Silver Layer.
    - Carefully review any discrepancies or issues flagged by the checks.
    - Investigate and resolve any data quality concerns before proceeding 
      to downstream processes.

===============================================================================
*/



==================================================

-- Checking 'silver.crm_sales_details'
-- Check for Invalid Dates
-- Expectation: No Invalid Dates

SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LENGTH(CAST(sls_due_dt AS VARCHAR)) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

==================================================

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results

SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;


==================================================

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results

SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


==================================================

-- Checking 'silver.erp_cust_az12'
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today

SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > CURRENT_DATE;

==================================================

-- Data Standardization & Consistency

SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

==================================================

-- Checking 'silver.erp_loc_a101'
-- Data Standardization & Consistency

SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

==================================================

-- Checking 'silver.erp_px_cat_g1v2'
-- Check for Unwanted Spaces
-- Expectation: No Results

SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

==================================================

-- Data Standardization & Consistency

SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
