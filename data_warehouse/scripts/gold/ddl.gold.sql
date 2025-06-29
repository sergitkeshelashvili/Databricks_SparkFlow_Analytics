# Gold Layer Views: `gold.dim_customers`, `gold.dim_products`, `gold.fact_sales`

## Overview
This SQL script creates three PostgreSQL views in the `gold` schema: `gold.dim_customers`, `gold.dim_products`, and `gold.fact_sales`. These views transform and integrate data from the `silver` schema tables to create a structured, analytics-ready data model for customer, product, and sales data. The views are designed to support reporting and analysis in a data warehouse environment.

## Purpose
The views serve the following purposes:
- **`gold.dim_customers`**: Provides a clean, consolidated view of customer data, including unique keys, demographic details, and derived attributes.
- **`gold.dim_products`**: Delivers a curated view of active product data with category details and costs.
- **`gold.fact_sales`**: Aggregates sales transaction data with links to customer and product dimensions for analytical queries.

These views are part of a gold layer in a data warehouse, optimized for business intelligence and reporting.

## Schema
The views are created in the `gold` schema and rely on tables from the `silver` schema:
- **Customer-related tables**:
  - `silver.crm_cust_info`: Contains customer information (`cst_id`, `cst_key`, `cst_firstname`, `cst_lastname`, `cst_gndr`, `cst_marital_status`, `cst_create_date`).
  - `silver.erp_cust_az12`: Provides additional customer attributes (`cid`, `gen`, `bdate`).
  - `silver.erp_loc_a101`: Contains location data (`cid`, `cntry`).
- **Product-related tables**:
  - `silver.crm_prd_info`: Contains product details (`prd_id`, `prd_key`, `prd_nm`, `cat_id`, `prd_cost`, `prd_line`, `prd_start_dt`, `prd_end_dt`).
  - `silver.erp_px_cat_g1v2`: Provides product category details (`id`, `cat`, `subcat`, `maintenance`).
- **Sales-related table**:
  - `silver.crm_sales_details`: Contains sales transaction data (`sls_ord_num`, `sls_prd_key`, `sls_cust_id`, `sls_order_dt`, `sls_ship_dt`, `sls_due_dt`, `sls_sales`, `sls_quantity`, `sls_price`).




=======================================================================================

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master for gender information
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON  ci.cst_key = la.cid

=======================================================================================


CREATE VIEW gold.dim_products AS
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
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- filter out all historical data


=======================================================================================


CREATE VIEW gold.fact_sales AS
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
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id = cu.customer_id





