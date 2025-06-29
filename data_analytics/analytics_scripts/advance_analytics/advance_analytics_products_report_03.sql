# gold.report_products View

## Overview
This SQL script creates a PostgreSQL view named `gold.report_products` in the `gold` schema. The view aggregates product data from the `gold.fact_sales` and `gold.dim_products` tables to provide a detailed report on product performance, including sales metrics, customer reach, and segmentation.

## Purpose
The `gold.report_products` view is designed to:
- Summarize product-level metrics such as total orders, total sales, and customer counts.
- Segment products based on total sales (High-Performer, Mid-Range, Low-Performer).
- Calculate derived metrics like average selling price, average order revenue (AOR), and average monthly revenue.
- Provide insights into product recency and lifespan for inventory and sales analysis.

## Schema
The view is created in the `gold` schema and relies on the following tables:
- `gold.fact_sales`: Contains sales transaction data (`order_number`, `order_date`, `customer_key`, `sales_amount`, `quantity`, `product_key`).
- `gold.dim_products`: Contains product details (`product_key`, `product_name`, `category`, `subcategory`, `cost`).

## Structure
The view is built using two Common Table Expressions (CTEs) and a final SELECT statement:

### 1. `base_query` CTE
- **Purpose**: Retrieves core columns from `gold.fact_sales` and `gold.dim_products`.
- **Joins**: Left joins `fact_sales` with `dim_products` on `product_key`.
- **Filters**: Excludes rows where `order_date` is NULL.
- **Columns**:
  - `order_number`: Unique identifier for each order.
  - `order_date`: Date of the order.
  - `customer_key`: Unique identifier for the customer.
  - `sales_amount`: Total sales amount for the order.
  - `quantity`: Number of items in the order.
  - `product_key`: Unique identifier for the product.
  - `product_name`: Name of the product.
  - `category`: Product category.
  - `subcategory`: Product subcategory.
  - `cost`: Cost of the product.

### 2. `product_aggregations` CTE
- **Purpose**: Aggregates data at the product level.
- **Grouping**: Groups by `product_key`, `product_name`, `category`, `subcategory`, and `cost`.
- **Metrics**:
  - `lifespan`: Months between first and last sale, calculated using `EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date)))`.
  - `last_sale_date`: Most recent sale date.
  - `total_orders`: Count of distinct orders.
  - `total_customers`: Count of distinct customers.
  - `total_sales`: Sum of sales amounts.
  - `total_quantity`: Sum of quantities sold.
  - `avg_selling_price`: Average sales amount per unit, rounded to 1 decimal place, using `ROUND(AVG(CAST(sales_amount AS NUMERIC) / NULLIF(quantity, 0)), 1)`.

### 3. Final SELECT
- **Purpose**: Builds the final report with additional derived fields and product segmentation.
- **Columns**:
  - `product_key`, `product_name`, `category`, `subcategory`, `cost`: Direct from `product_aggregations`.
  - `last_sale_date`: Most recent sale date.
  - `recency_in_months`: Months since the last sale, calculated using `EXTRACT(MONTH FROM AGE(CURRENT_DATE, last_sale_date))`.
  - `product_segment`: Segments products based on total sales:
    - `High-Performer`: Total sales > 50,000.
    - `Mid-Range`: Total sales ≥ 10,000 and ≤ 50,000.
    - `Low-Performer`: Total sales < 10,000.
  - `lifespan`, `total_orders`, `total_sales`, `total_quantity`, `total_customers`, `avg_selling_price`: Direct from `product_aggregations`.
  - `avg_order_revenue`: Total sales divided by total orders (returns 0 if `total_orders` is 0).
  - `avg_monthly_revenue`: Total sales divided by lifespan (returns `total_sales` if `lifespan` is 0).


==================================================================================
==================================================================================
==================================================================================


DROP VIEW IF EXISTS gold.report_products;

CREATE VIEW gold.report_products AS

WITH base_query AS (
    /*---------------------------------------------------------------------------
    1) Base Query: Retrieves core columns from fact_sales and dim_products
    ---------------------------------------------------------------------------*/
    SELECT
        f.order_number,
        f.order_date,
        f.customer_key,
        f.sales_amount,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE order_date IS NOT NULL  -- only consider valid sales dates
),

product_aggregations AS (
    /*---------------------------------------------------------------------------
    2) Product Aggregations: Summarizes key metrics at the product level
    ---------------------------------------------------------------------------*/
    SELECT
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan,
        MAX(order_date) AS last_sale_date,
        COUNT(DISTINCT order_number) AS total_orders,
        COUNT(DISTINCT customer_key) AS total_customers,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        ROUND(AVG(CAST(sales_amount AS NUMERIC) / NULLIF(quantity, 0)), 1) AS avg_selling_price
    FROM base_query
    GROUP BY
        product_key,
        product_name,
        category,
        subcategory,
        cost
)

/*---------------------------------------------------------------------------
3) Final Query: Combines all product results into one output
---------------------------------------------------------------------------*/
SELECT 
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_sale_date,
    EXTRACT(MONTH FROM AGE(CURRENT_DATE, last_sale_date)) AS recency_in_months,
    CASE
        WHEN total_sales > 50000 THEN 'High-Performer'
        WHEN total_sales >= 10000 THEN 'Mid-Range'
        ELSE 'Low-Performer'
    END AS product_segment,
    lifespan,
    total_orders,
    total_sales,
    total_quantity,
    total_customers,
    avg_selling_price,
    -- Average Order Revenue (AOR)
    CASE 
        WHEN total_orders = 0 THEN 0
        ELSE total_sales / total_orders
    END AS avg_order_revenue,
    -- Average Monthly Revenue
    CASE
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales / lifespan
    END AS avg_monthly_revenue
FROM product_aggregations;
