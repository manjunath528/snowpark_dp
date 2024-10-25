# Amazon Sales Data Processing with Snowflake and Snowpark

This project demonstrates the processing of Amazon sales data from multiple regions (India, USA, France) using Snowflake's Snowpark library. The objective is to create a structured data warehouse with a star schema for use in further analysis, visualization, and machine learning tasks. The `Sales` table serves as the fact table, connected to various dimension tables providing additional details such as dates, customers, payments, products, promotional codes, and regions.

## Table of Contents

- [Overview](#overview)
- [Data Sources](#data-sources)
- [Project Structure](#project-structure)
- [Data Processing](#data-processing)
- [Database Design](#database-design)
- [Future Steps](#future-steps)

## Overview

This repository provides a data engineering workflow utilizing Snowflake's Snowpark API for the processing and transformation of Amazon sales data. The resulting star schema design allows efficient data querying, optimized for visualization and data science applications.

## Data Sources

The project utilizes three directories, each containing sales data for a specific region:
- `data/india/`: Contains CSV files of India sales data.
- `data/usa/`: Contains Parquet files of USA sales data.
- `data/france/`: Contains JSON files of France sales data.

Each directory includes information on individual sales transactions, including customer details, payment methods, product information, and promotional codes.

## Project Structure

```plaintext
.
├── data/
│   ├── india/            # Contains CSV files
│   ├── usa/              # Contains Parquet files
│   └── france/           # Contains JSON files
├── python/               # Contains Python scripts for data processing
│   ├── 1_connectivity.py               # Establishes connection to Snowflake
│   ├── 2_stg_to_source.py               # Loads staging data to source tables
│   ├── 3_currency_stg.py                # Handles currency staging processes
│   ├── 4_currency_stg_to_source.py      # Moves currency data to source tables
│   ├── 5_currency_processing.py          # Processes currency data
│   ├── 6_source_to_curated.py            # Moves data from source to curated layer
│   ├── 7_source_to_curated_fr.py         # Moves French data to curated layer
│   ├── 8_source_to_curated_us.py         # Moves USA data to curated layer
│   └── _final_process.py                 # Final processing script
├── sql/                   # Contains SQL scripts for table creation and queries
│   ├── 1_initial_creations_sf.sql         # Initial table and schema creations
│   ├── 2_file_formats.sql                 # SQL for managing different file formats
│   ├── 3_exchange_rate_processing.sql     # Processing exchange rates
│   ├── 4_data_ingestion_source.sql        # Data ingestion SQL for source tables
│   ├── 5_curated_layer.sql                # SQL for creating curated data layer
│   ├── 6_consumption_layer.sql            # SQL for creating consumption layer
│   └── 7_stg_local.sql                    # Local staging table creation
├── README.md
└── requirements.txt
.
```


## Data Processing

Using Snowflake and the Snowpark library, the data processing workflow involves several steps:

- Data Ingestion: Import data from the three folders (CSV, Parquet, and JSON) into Snowflake.
- Cleaning and Transformation:
   - Format dates, standardize data types, and remove duplicates.
   - Handle missing values.
- Creating Dimension Tables:
   - Date Dimension: Includes fields for date, day, week, month, quarter, and year.
   - Customer Details Dimension: Contains customer information, including ID, name, email, and contact.
   - Payment Details Dimension: Holds payment method details.
   - Product Details Dimension: Includes product ID, name, category, and price.
   - Promo Code Details Dimension: Contains promo codes and their discount values.
   - Region Details Dimension: Includes region-based information.
- Creating Fact Table:
   - The Sales Fact Table links each transaction to the associated dimension tables, including transaction-specific information like sales amount, product quantity, and promo codes.
- Loading Data: Populate each table with the cleaned data.
  
## Database Design

This project follows a star schema design, with a central Sales fact table linked to various dimension tables. This structure facilitates efficient querying for both visualization and data science applications.
                
                 
  # Star Schema Diagram
```plaintext
                 +---------------+
                 |    Date       |
                 +---------------+
                 | Date_ID       |<------------------+
                 | Day           |                   |
                 | Week          |                   |
                 | Month         |                   |
                 | Quarter       |                   |
                 | Year          |                   |
                 +---------------+                   |
                        |                              |
                        |                              |
                 +---------------+                     |
                 |   Customer    |                     |
                 +---------------+                     |
                 | Customer_ID   |                     |
                 | Name          |                     |
                 | Email         |                     |
                 | Contact       |                     |
                 +---------------+                     |
                        |                              |
                        |                              |
                 +---------------+                     |
                 |    Sales      |                     |
                 +---------------+                     |
                 | Sales_ID      |                     |
                 | Date_ID       |-------------------+
                 | Customer_ID   |
                 | Amount        |
                 | Quantity      |
                 | Promo_Code_ID |
                 | Product_ID    |
                 | Payment_ID    |
                 | Region_ID     |
                 +---------------+
                        |
                        |
      +------------+--------+-----------+
      |            |        |           |
      |            |        |           |
 +------+    +---------+  +---------+  +---------+
 |Region|    | Product |  | Payment |  | Promo   |
 +------+    +---------+  +---------+  +---------+
 | Region_ID   | Product_ID | Payment_ID | Promo_Code_ID |
 | Region_Name | Name       | Method      | Discount      |
 | ...         | Category   | ...        | ...           |
 +------+    +---------+  +---------+  +---------+

```

- Fact Table: Sales
- Dimension Tables: Date, Customer, Payment, Product, Promo, Region



## Future Steps

- Data Visualization: Use tools like Tableau, PowerBI, or Snowflake's native features for visualization.
- Data Science Applications: Implement predictive modeling, customer segmentation, and trend analysis using this structured data.
- Optimization: Refine SQL queries and Snowpark transformations for enhanced performance.






