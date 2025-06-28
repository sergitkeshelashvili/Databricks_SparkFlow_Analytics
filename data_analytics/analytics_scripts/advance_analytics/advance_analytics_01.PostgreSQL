# SQL Sales Performance Analysis

## Overview
This project contains a set of SQL queries focused on analyzing sales performance over time for a sales database. The queries explore temporal trends, product performance, customer segmentation, and category contributions, providing actionable insights into business operations.

## Features
- **Temporal Sales Analysis**:
  - Aggregate sales, customer counts, and quantities by year and month using `EXTRACT`, `DATE_TRUNC`, and `TO_CHAR` for flexible date formatting.
  - Calculate running totals and moving averages for yearly sales and prices.
- **Product Performance**:
  - Analyze yearly product sales, comparing current sales to historical averages and year-over-year performance using window functions (`AVG`, `LAG`).
  - Segment products into cost ranges (e.g., Below 100, 100-500) to understand cost distribution.
- **Customer Segmentation**:
  - Categorize customers into VIP, Regular, and New segments based on spending behavior (€5,000 threshold) and lifespan (12+ months).
  - Count customers in each segment to assess customer base composition.
- **Category Contribution**:
  - Identify top-performing product categories by total sales and their percentage contribution to overall sales.

## Database Schema
The queries assume a database with the following key tables in the `gold` schema:
- `dim_customers`: Contains customer information (e.g., `customer_key`).
- `dim_products`: Contains product details (e.g., `product_key`, `product_name`, `category`, `cost`).
- `fact_sales`: Contains sales transactions (e.g., `order_date`, `customer_key`, `product_key`, `sales_amount`, `quantity`, `price`).


## Key Queries
1. **Sales Over Time**: Aggregate sales, customers, and quantities by year/month using different date grouping methods.
2. **Running Totals and Averages**: Calculate cumulative sales and moving average prices per year.
3. **Yearly Product Performance**: Compare product sales to their historical averages and previous year's sales.
4. **Product Cost Segmentation**: Group products by cost ranges and count products in each range.
5. **Customer Segmentation**: Classify customers into VIP, Regular, and New based on spending and lifespan.
6. **Category Sales Contribution**: Rank categories by sales and calculate their percentage of total sales.

## Notes
- Queries use `LEFT JOIN` to handle potential missing relationships and `WHERE order_date IS NOT NULL` to ensure valid date data.
- PostgreSQL-specific functions (`EXTRACT`, `DATE_TRUNC`, `TO_CHAR`) may need adjustment for other databases (e.g., MySQL, SQL Server).
- Ensure data integrity (e.g., no null values in critical columns like `order_date`, `sales_amount`) for accurate results.

## License
This project is licensed under the MIT License. 



======================================================================================

-- Analyse sales performance over time

SELECT
    EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date)
ORDER BY EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date);


======================================================================================

-- DATE_TRUNC()

SELECT
    DATE_TRUNC('month', order_date) AS order_date,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY DATE_TRUNC('month', order_date);


======================================================================================

-- TO_CHAR()

SELECT
    TO_CHAR(order_date, 'YYYY-MON') AS order_date,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY TO_CHAR(order_date, 'YYYY-MON')
ORDER BY TO_CHAR(order_date, 'YYYY-MON');

-- Calculate the total sales per month 
-- and the running total of sales over time 

SELECT
    order_date,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
    AVG(avg_price) OVER (ORDER BY order_date) AS moving_average_price
FROM
(
    SELECT 
        DATE_TRUNC('year', order_date) AS order_date,
        SUM(sales_amount) AS total_sales,
        AVG(price) AS avg_price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATE_TRUNC('year', order_date)
) t;


======================================================================================

/* Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous year's sales */

WITH yearly_product_sales AS (
    SELECT
        EXTRACT(YEAR FROM f.order_date) AS order_year,
        p.product_name,
        SUM(f.sales_amount) AS current_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY 
        EXTRACT(YEAR FROM f.order_date),
        p.product_name
)
SELECT
    order_year,
    product_name,
    current_sales,
    AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS difference_avg,
    CASE 
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change,
    -- Year-over-Year Analysis
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS previous_year_sales,
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS difference_previous_year,
    CASE 
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
        ELSE 'No Change'
    END AS previous_year_change
FROM yearly_product_sales
ORDER BY product_name, order_year;


======================================================================================

/*Segment products into cost ranges and 
count how many products fall into each segment*/

WITH product_segments AS (
    SELECT
        product_key,
        product_name,
        cost,
        CASE 
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)
SELECT 
    cost_range,
    COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;


======================================================================================

/*Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than €5,000.
	- Regular: Customers with at least 12 months of history but spending €5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/

WITH customer_spending AS (
    SELECT
        c.customer_key,
        SUM(f.sales_amount) AS total_spending,
        MIN(f.order_date) AS first_order,
        MAX(f.order_date) AS last_order,
        EXTRACT(YEAR FROM AGE(MAX(f.order_date), MIN(f.order_date))) * 12 + 
        EXTRACT(MONTH FROM AGE(MAX(f.order_date), MIN(f.order_date))) AS lifespan
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
)
SELECT 
    customer_segment,
    COUNT(customer_key) AS total_customers
FROM (
    SELECT 
        customer_key,
        CASE 
            WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
            WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
    FROM customer_spending
) AS segmented_customers
GROUP BY customer_segment
ORDER BY total_customers DESC;


======================================================================================

-- Categories contribute the most to overall sales

WITH category_sales AS (
    SELECT
        p.category,
        SUM(f.sales_amount) AS total_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    GROUP BY p.category
)
SELECT
    category,
    total_sales,
    SUM(total_sales) OVER () AS overall_sales,
    ROUND((total_sales::NUMERIC / SUM(total_sales) OVER ()) * 100, 2) AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC;


