# gold.report_customers View

## Overview
This SQL script creates a PostgreSQL view named `gold.report_customers` in the `gold` schema. The view aggregates customer data from the `gold.fact_sales` and `gold.dim_customers` tables to provide a comprehensive report on customer metrics, including order history, sales, and segmentation.

## Purpose
The `gold.report_customers` view is designed to:
- Summarize customer-level metrics such as total orders, total sales, and product diversity.
- Segment customers based on age and purchase behavior (VIP, Regular, New).
- Calculate derived metrics like average order value (AOV) and average monthly spend.
- Provide insights into customer recency and lifespan for business analysis.

## Schema
The view is created in the `gold` schema and relies on the following tables:
- `gold.fact_sales`: Contains sales transaction data (order_number, product_key, order_date, sales_amount, quantity, customer_key).
- `gold.dim_customers`: Contains customer details (customer_key, customer_number, first_name, last_name, birthdate).

## Structure
The view is built using two Common Table Expressions (CTEs) and a final SELECT statement:

### 1. `base_query` CTE
- **Purpose**: Retrieves core columns from `gold.fact_sales` and `gold.dim_customers`.
- **Joins**: Left joins `fact_sales` with `dim_customers` on `customer_key`.
- **Filters**: Excludes rows where `order_date` is NULL.
- **Columns**:
  - `order_number`: Unique identifier for each order.
  - `product_key`: Identifier for products.
  - `order_date`: Date of the order.
  - `sales_amount`: Total sales amount for the order.
  - `quantity`: Number of items in the order.
  - `customer_key`: Unique identifier for the customer.
  - `customer_number`: Customer identifier.
  - `customer_name`: Concatenated first and last names.
  - `age`: Calculated as the difference in years between the current date and the customer's birthdate using `EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate))`.

### 2. `customer_aggregation` CTE
- **Purpose**: Aggregates data at the customer level.
- **Grouping**: Groups by `customer_key`, `customer_number`, `customer_name`, and `age`.
- **Metrics**:
  - `total_orders`: Count of distinct orders.
  - `total_sales`: Sum of sales amounts.
  - `total_quantity`: Sum of quantities ordered.
  - `total_products`: Count of distinct products purchased.
  - `last_order_date`: Most recent order date.
  - `lifespan`: Number of months between the first and last order, calculated using `EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date)))`.

### 3. Final SELECT
- **Purpose**: Builds the final report with additional derived fields and customer segmentation.
- **Columns**:
  - `customer_key`, `customer_number`, `customer_name`, `age`: Direct from `customer_aggregation`.
  - `age_group`: Categorizes customers into age bands:
    - Under 20
    - 20-29
    - 30-39
    - 40-49
    - 50 and above
  - `customer_segment`: Segments customers based on lifespan and total sales:
    - `VIP`: Lifespan ≥ 12 months and total sales > 5000.
    - `Regular`: Lifespan ≥ 12 months and total sales ≤ 5000.
    - `New`: Lifespan < 12 months.
  - `last_order_date`: Most recent order date.
  - `recency`: Months since the last order, calculated using `EXTRACT(MONTH FROM AGE(CURRENT_DATE, last_order_date))`.
  - `total_orders`, `total_sales`, `total_quantity`, `total_products`, `lifespan`: Direct from `customer_aggregation`.
  - `avg_order_value`: Total sales divided by total orders (returns 0 if `total_orders` is 0 to avoid division by zero).
  - `avg_monthly_spend`: Total sales divided by lifespan (returns `total_sales` if `lifespan` is 0).

======================================================================================

-- Create Report: gold.report_customers

DROP VIEW IF EXISTS gold.report_customers;

======================================================================================

CREATE VIEW gold.report_customers AS

WITH base_query AS (
    /*---------------------------------------------------------------------------
    1) Base Query: Retrieves core columns from tables
    ---------------------------------------------------------------------------*/
    SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.birthdate)) AS age
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON c.customer_key = f.customer_key
    WHERE order_date IS NOT NULL
),

customer_aggregation AS (
    /*---------------------------------------------------------------------------
    2) Customer Aggregations: Summarizes key metrics at the customer level
    ---------------------------------------------------------------------------*/
    SELECT 
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order_date,
        EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan
    FROM base_query
    GROUP BY 
        customer_key,
        customer_number,
        customer_name,
        age
)
SELECT
    customer_key,
    customer_number,
    customer_name,
    age,
    CASE 
        WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        ELSE '50 and above'
    END AS age_group,
    CASE 
        WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
    last_order_date,
    EXTRACT(MONTH FROM AGE(CURRENT_DATE, last_order_date)) AS recency,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    lifespan,
    -- Compute average order value (AVO)
    CASE 
        WHEN total_orders = 0 THEN 0
        ELSE total_sales / total_orders
    END AS avg_order_value,
    -- Compute average monthly spend
    CASE 
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales / lifespan
    END AS avg_monthly_spend
FROM customer_aggregation;





