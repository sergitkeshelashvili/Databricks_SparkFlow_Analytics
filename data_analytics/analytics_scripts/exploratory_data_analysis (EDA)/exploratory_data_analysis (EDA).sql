# SQL Exploratory Data Analysis (EDA) Project

## Overview
This project contains a collection of SQL queries designed to perform exploratory data analysis (EDA) on a sales database. The queries aim to uncover key insights about customers, products, sales, and orders, providing a comprehensive understanding of the business's performance and trends.

## Features
- **Database Schema Exploration**: Queries to retrieve all tables and columns from the database using `INFORMATION_SCHEMA`.
- **Customer Analysis**:
  - Identify unique countries of customers.
  - Calculate the total number of customers and their distribution by country and gender.
  - Determine the youngest and oldest customers based on birthdate.
- **Product Analysis**:
  - List all product categories, subcategories, and product names.
  - Calculate the total number of products and their distribution by category.
  - Compute average product costs per category.
- **Sales Analysis**:
  - Determine the date range of sales and the number of years covered.
  - Calculate total sales, total quantity sold, average selling price, and total orders.
  - Analyze revenue by product category and customer.
  - Identify the distribution of sold items across countries.
- **Rankings and Performance**:
  - Rank the top 5 products by revenue using both simple and window function-based approaches.
  - Identify the 5 worst-performing products by sales.
  - List the top 10 customers by revenue and the 3 customers with the fewest orders.

## Database Schema
The queries assume a database with the following key tables in the `gold` schema:
- `dim_customers`: Contains customer information (e.g., customer_key, first_name, last_name, country, gender, birthdate).
- `dim_products`: Contains product details (e.g., product_key, product_name, category, subcategory, cost).
- `fact_sales`: Contains sales transactions (e.g., order_number, order_date, customer_key, product_key, sales_amount, quantity, price).

## Usage
- The queries are written in standard SQL and can be executed on any SQL database (e.g., PostgreSQL, MySQL, SQL Server) that supports the `INFORMATION_SCHEMA` and standard SQL functions like `EXTRACT`, `SUM`, `COUNT`, `AVG`, and window functions (`RANK`).
- Ensure the database has the required tables (`gold.dim_customers`, `gold.dim_products`, `gold.fact_sales`) with the specified columns.
- Copy and paste the queries into your SQL client or editor to execute them against your database.

## Key Queries
1. **Schema Exploration**: Retrieve all tables and columns in the database.
2. **Customer Insights**: Analyze customer demographics (countries, gender, age).
3. **Product Insights**: Explore product categories, subcategories, and costs.
4. **Sales Metrics**: Calculate total sales, quantities, average prices, and order counts.
5. **Revenue Analysis**: Break down revenue by category, customer, and country.
6. **Performance Rankings**: Identify top and bottom-performing products and customers.

## Notes
- The queries are designed for a clean and normalized database. Ensure data integrity (e.g., no missing or null values in key columns) for accurate results.
- Some queries use `LEFT JOIN` to handle potential missing relationships between tables.
- The `EXTRACT` function is used for date calculations, which is specific to databases like PostgreSQL. Adjust for other databases (e.g., use `DATEDIFF` for SQL Server).

## License
This project is licensed under the MIT License. Feel free to use, modify, and distribute the queries as needed.


=================================================================================================================
=================================================================================================================



-- Explore All object in the DATABASE

SELECT * FROM INFORMATION_SCHEMA.TABLES;

======================================================

-- Explore All Columns in the Database

SELECT * FROM INFORMATION_SCHEMA.COLUMNS;

======================================================

-- Dimensions Exploration
-- Explore All Countries Our customers come from

SELECT DISTINCT country FROM gold.dim_customers;

======================================================

-- Explore All Categories & subcategories & products

SELECT DISTINCT category, subcategory, product_name FROM gold.dim_products
ORDER BY 1,2,3;

======================================================

-- Date Exploration
-- Find the date of the first and last order
-- How many years of sales are avaiable

SELECT
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) AS order_range_years
FROM gold.fact_sales;

======================================================

-- Find the youngest and the oldest customer

SELECT
    MIN(birthdate) AS oldest_birthdate,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, MIN(birthdate))) AS oldest_age,
    MAX(birthdate) AS youngest_birthdate,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, MAX(birthdate))) AS youngest_age
FROM gold.dim_customers;

======================================================

-- Find the Total sales

SELECT 
	SUM(sales_amount) AS total_sales
FROM gold.fact_sales;

======================================================

-- Find how many items are sold

SELECT 
	SUM(quantity) AS total_quantity
FROM  gold.fact_sales;

======================================================

-- Find the avarage selling price

SELECT
	AVG(price) AS avg_price
FROM  gold.fact_sales;

======================================================

-- Find the total number of orders

SELECT 
	COUNT(DISTINCT order_number) AS total_orders
FROM  gold.fact_sales;

======================================================

-- Find the total numbers of products

SELECT 
	COUNT(product_name) AS total_products
FROM gold.dim_products;

SELECT 
	COUNT(DISTINCT product_name) AS total_products
FROM gold.dim_products;

======================================================

-- Find the total number of customers

SELECT 
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers;

======================================================

-- Find the total number of customers that has placed an order

SELECT 
	COUNT(DISTINCT customer_key) AS total_customers
FROM gold.fact_sales;

======================================================

-- Generate a Report that shows all key metrics of the business

SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Products', COUNT(DISTINCT product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Customers', COUNT(customer_key) FROM gold.dim_customers;

======================================================

-- Find total customers by countries

SELECT
    country,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

======================================================

-- Find total customers by gender

SELECT
    gender,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;

======================================================

-- Find total products by category

SELECT
    category,
    COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;

======================================================

-- Average costs in each category

SELECT
    category,
    AVG(cost) AS avg_cost
FROM gold.dim_products
GROUP BY category
ORDER BY avg_cost DESC;

======================================================

-- Total revenue generated for each category

SELECT
    p.category,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

======================================================

-- Total revenue generated by each customer

SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC;

======================================================

-- The distribution of sold items across countries

SELECT
    c.country,
    SUM(f.quantity) AS total_sold_items
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY total_sold_items DESC;

======================================================

-- Which 5 products Generating the Highest Revenue?
-- Simple Ranking

SELECT
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC
Limit 5;

======================================================

-- Complex but Flexibly Ranking Using Window Functions

SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(f.sales_amount) AS total_revenue,
        RANK() OVER (ORDER BY SUM(f.sales_amount) DESC) AS rank_products
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    GROUP BY p.product_name
) AS ranked_products
WHERE rank_products <= 5;

======================================================

-- What are the 5 worst-performing products in terms of sales?

SELECT
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue
LIMIT 5;

======================================================

-- Find the top 10 customers who have generated the highest revenue

SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC
LIMIT 10;

======================================================

-- The 3 customers with the fewest orders placed

SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_orders
LIMIT 3;
