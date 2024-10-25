# Amazon Sales Data Processing with Snowflake and Snowpark

This project involves processing Amazon sales data from multiple regions (India, USA, France) using Snowflake's Snowpark library. The goal is to create a clean, organized data warehouse with a star schema structure, which can be used for further analysis, visualization, and machine learning tasks. The fact table is the `Sales` table, and the associated dimension tables provide details for various aspects such as dates, customers, payments, products, promotional codes, and regions.

## Table of Contents

- [Overview](#overview)
- [Data Sources](#data-sources)
- [Project Structure](#project-structure)
- [Data Processing](#data-processing)
- [Database Design](#database-design)
- [Getting Started](#getting-started)
- [Future Steps](#future-steps)
- [License](#license)

## Overview

This repository demonstrates a data engineering workflow using Snowflake's Snowpark API to process and transform sales data from Amazon. By following a star schema, it enables efficient querying and analysis of large datasets. This setup is optimized for downstream tasks like visualization and data science applications.

## Data Sources

The project utilizes three folders, each containing sales data for a specific region:
- `india/` (India - folder of CSV files)
- `usa/` (USA - folder of Parquet files)
- `france/` (France - folder of JSON files)

Each folder contains files with information on individual sales transactions, including customer details, payment methods, product information, and promotional codes used.

## Project Structure

```plaintext
.
├── data/
│   ├── india/
│   │   ├── file1.csv
│   │   ├── file2.csv
│   │   └── ...
│   ├── usa/
│   │   ├── file1.parquet
│   │   ├── file2.parquet
│   │   └── ...
│   └── france/
│       ├── file1.json
│       ├── file2.json
│       └── ...
├── scripts/
│   ├── data_processing.py
│   └── create_tables.sql
├── README.md
└── requirements.txt

##Data Processing

Using Snowflake and the Snowpark library, the data processing workflow involves several steps:

1. Data Ingestion: Import data from the three folders (CSV, Parquet, and JSON) into Snowflake.
2. Cleaning and Transformation:
 * Format dates, standardize data types, and remove duplicates.
 * Handle missing values.
3. Creating Dimension Tables:
 * Date Dimension: Includes date, day, week, month, quarter, and year fields.Customer Details Dimension: Contains customer information such as ID, name, email, and contact.
 * Payment Details Dimension: Holds payment method details.
 * Product Details Dimension: Includes product ID, name, category, and price.
 * Promo Code Details Dimension: Contains promo codes and their discount values.
 * Region Details Dimension: Includes region-based information.
4. Creating Fact Table:
 * The Sales Fact Table is created to link each transaction to the associated dimension tables. It includes transaction-specific information like sales amount, product quantity, promo codes, etc.
5. Loading Data: Populate each table with the cleaned data.
