# Data Warehouse Implementation Project

## 📖 Overview
This project implements a **Sales Data Warehouse** in **PostgreSQL**, designed to support business intelligence, reporting, and multi-dimensional analysis.  
The system integrates raw sales data into a structured warehouse through **ETL (Extract, Transform, Load) pipelines** that ensure consistency, accuracy, and analytical readiness.

## 🚀 Features
- **Transformation Modules**
  - Data mapping across heterogeneous sources
  - Matching and deduplication of records
  - Versioning for maintaining historical accuracy
  - Data cleansing to ensure standardized and high-quality entries  

- **Dimension Modeling**
  - Time dimension (year, quarter, month, day)
  - Location dimension (province, city/municipality)
  - Product dimension (categories, variants, hierarchy)

- **Loading Modules**
  - Automated loading of dimension hierarchies
  - Fact table population for sales transactions
  - Support for Slowly Changing Dimensions (SCD)

- **Analytics-Ready Outputs**
  - Pre-aggregated **data cubes**
  - Metrics for sales, customers, and product performance
  - Multi-dimensional analysis (time, location, product)

## 🛠️ Tech Stack
- **Database:** PostgreSQL  
- **ETL / Data Processing:** SQL (CTEs, CASE WHEN mappings, transformation scripts)  
- **Modeling Approach:** Star Schema with fact and dimension tables  
- **Analytics:** OLAP-style cubes for drill-down analysis  
