# Medallion-Architecture-Patterns-for-Data-Engineering
Hands-On Data Engineering Labs with Medallion Architecture
---
## Overview
The Medallion Architecture is a data design pattern that organizes data into three layers:


- **Bronze Layer** – Raw, unfiltered data ingested directly from source systems.
- **Silver Layer** – Cleaned, standardized, and enriched data prepared for analytics.
- **Gold Layer** – Aggregated, business-level data optimized for reporting and advanced analytics.

This approach improves data quality, reliability, and enables scalable data processing.

## Getting Started:

- Put your enviroment in Dark Mode.
- Turn on cell line numbers.
- Please run the 00_seed_bronze file in the launch folder to initialize your environment.
- The 01_example_load_bronze file provides examples of how to extract data from the Bronze layer.
## Goals for Landing Bronze Data and Processing to Silver Layer
- **Ingest Raw Data (Bronze Layer):**

  - Collect raw, unprocessed data from landing volume.
  - Store data in its original format for traceability and auditing.
  - Ensure the following:
    - That the data has the correct datatype via schema inferance.
    - All columns are lowercase.
    - Add a metadata column with file name and file modification timestamp.
    - Add a ingested timestamp.
- **Clean and Normalize Data (Silver Layer):**

  - Silver Layer must be built using a job or pipeline.
  - Remove duplicates and correct data types.
  - Standardize formats and apply business rules.
    - Pep 8
    - Leading Commas ;)
  - Prepare data for analytics and downstream processing.
- **Enable Reliable Data Pipelines:**

  - Ensure data quality and consistency between layers.
  - Facilitate scalable and maintainable ETL workflows.
## Stretch Goals - This is entirely optional but should provide a fun challenge.
These advanced objectives focus on optimizing inventory management and supply chain operations using the Gold Layer of the Medallion Architecture. The Gold Layer aggregates business-level data, enabling strategic planning and analytics.

- **Supply Chain Chaos (Gold Layer):**
  - Model inventory burn rate based off of historic demand to predict future needs.
  - Determine MOQ (minimum order quantity) of each SKU per week for the next year to the factory, ensuring efficient ordering and production.
  - Build a shipping schedule for each SKU from warehouses to stores, optimizing logistics and delivery timelines.
  - Ensure that all stores have at least 3 weeks of stock at any time to prevent stockouts and maintain customer satisfaction.
  -  for warehouse lead times to store of 1 week to accurately plan replenishments.
  - Account for factory lead times to warehouse of 4 weeks to synchronize production and distribution.
  - Account for stores only being able to receive cases from the factory, which affects order quantities and packaging.
## Burn Rate

- Burn Rate is the rate at which inventory is consumed. For example, if you sell 10 widgets a week, then your burn rate for widgets is 10/week. Understanding burn rate helps forecast inventory needs and avoid shortages.
## Notes
- To complete this assignment you will need to create the following:
  - a calendar table.
  - complex algorithms that may span multiple tables or workbooks.
