# Databricks SparkFlow Analytics 📊💸  

![Databricks](https://img.shields.io/badge/Platform-Databricks-orange?logo=databricks)  
![PySpark](https://img.shields.io/badge/PySpark-ETL-blue?logo=apachespark)  
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Storage-brightgreen)  
![Spark SQL](https://img.shields.io/badge/Spark%20SQL-Analytics-purple?logo=apachespark)  
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

A **modern data lakehouse & analytics solution** built on **Databricks**, powered by **PySpark, Spark SQL, and Delta Lake** with **automated workflows** for scalable and reliable pipeline execution.


This project demonstrates end-to-end **data engineering and BI practices**:  

✅ Scalable **data lakehouse** design  
✅ Automated **ETL pipelines** with **Databricks Workflows**
✅ **Star schema modeling** for analytics  
✅ **Actionable insights** through **Spark SQL** based reporting  

---

## 🖥 Data Architecture – Medallion Approach  

The project adopts the **Medallion Architecture** with three layers:  

| Layer   | Purpose |
|---------|---------|
| 🥉 **Bronze** | Raw ingested data from ERP/CRM CSV files. |
| 🥈 **Silver** | Cleaned & standardized data ensuring quality. |
| 🥇 **Gold**   | Business-ready, star schema data for BI & reporting. |


## 🏗️ Medallion Architecture Diagram


![Medallion Architecture](./data_lakehouse/schema_documentation/data_lakehouse_project_architecture.png)


---

## Pipeline Automation 🚀

This project includes **automated workflows** in **Databricks** to orchestrate the **Medallion Architecture ETL pipeline**, **data quality checks**, and **advanced analytics**. The workflow ensures seamless execution from raw data ingestion (Bronze) to business-ready insights (Gold), with error handling and logging.


![Databricks Workflow Automation](./data_lakehouse/schema_documentation/databricks_workflow_automation.png)

### Workflow Details
- **Bronze Layer**: Ingests raw CSV data.
- **Silver Layer**: Cleans and transforms data, followed by quality checks.
- **Gold Layer**: Creates star schema views, with quality validation.
- **Analytics**: Runs exploratory and advanced analytics.


This automation enhances scalability and reliability, aligning with production-grade **data engineering** practices.

---

## 📖 Project Highlights  

- 🪙 **Data Architecture** – Medallion layers with **Delta Lake storage**.  
- 🪙 **Automated ETL Pipelines** – Built in **PySpark & Spark SQL**.  
- 🪙 **Data Modeling** – Fact & dimension tables in a **star schema**.  
- 🪙 **Analytics & BI** – **Spark SQL** queries that generate insights for business stakeholders.  

---

## 🗂 Repository Structure  

📂 **data_lakehouse**/

┣ 📂 **datasource**/ → Raw ERP & CRM CSV files

┣ 📂 **schema_documentation**/ → Data model & schema documentations

┣ 📂 **ETL_scripts**/ → ETL code (bronze_layer.py, silver_layer.py, gold_layer.py)

┣ 📂 **data_quality_checks**/ → Data quality & pipeline validation


📂 **data_analytics**/

┣ 📂 **analytics_scripts** / → exploratory_data_analysis (EDA) & advance_analytics

┣ 📂 **analytics_data_source** / → (gold.dim_customers.csv, gold.dim_products.csv, gold.fact_sales.csv)

┣ 📂 **data_analytics_roadmap** / → data_analytics_roadmap


---

## 🎯 Target Audience  

This project is designed for **data engineers, analysts, and students** showcasing expertise in:  

- 🐍 **PySpark & Spark SQL Development**  
- 🏗️ **Data Lakehouse with Medallion Architecture**  
- ⚙️ **Automated ETL Pipeline Engineering** 
- ⭐ **Star Schema Data Modeling** 
- 📊 **Data Analytics & BI**  

---

## 📊 Business Insights  

Analytics & reporting focus on:  

- 👥 **Customer Behavior** – Segmentation (VIP, Regular, New), retention, churn.  
- 📦 **Product Performance** – Category contribution, sales vs. costs.  
- 📅 **Sales Trends** – Seasonal patterns, regional metrics, growth tracking.  

These insights support **strategic business decisions**.  

---

## 🛠 Technologies Used  

- 🔧 **Databricks** – Unified data platform  
- 🐍 **PySpark** – Scalable automated ETL pipelines  
- 📜 **Spark SQL** – Transformations & analytics  
- 💾 **Delta Lake** – Reliable, versioned storage  

---

## 🛡️ License  

Licensed under the **MIT License**.  

---

✨ With SparkFlow Analytics, raw ERP & CRM data is transformed into a **scalable, analytics-ready lakehouse** that powers **data-driven business insights**.  












