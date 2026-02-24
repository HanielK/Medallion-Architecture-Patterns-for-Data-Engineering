# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # DA Track Training Labs — Bronze → Silver → Gold (Weather + Outages + Work Orders)
# MAGIC
# MAGIC Welcome! This training track teaches a clean Medallion-style architecture in Databricks using realistic “messy” operational datasets.
# MAGIC
# MAGIC You will:
# MAGIC - Ingest raw (messy) data into **Bronze**
# MAGIC - Clean + normalize into **Silver**
# MAGIC - Build analytics-ready **Gold facts + dims**
# MAGIC - (Bonus) Perform an **hourly-nearest weather join** for advanced analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Folder / Notebook Order (Recommended Run Path)
# MAGIC
# MAGIC 1. **01_DA_TRACK_SEED_Bronze**  
# MAGIC    Creates schemas/volumes and loads raw Bronze tables.
# MAGIC
# MAGIC 2. **02_DA_TRACK_SILVER_CLEANING**  
# MAGIC    Cleans + standardizes messy outages + work orders into Silver.
# MAGIC
# MAGIC 3. **03_DA_TRACK_GOLD_AGGREGATIONS**  
# MAGIC    Builds Gold dims + daily facts for dashboards and analysis.
# MAGIC
# MAGIC 4. **99_DA_TRACK_RESET_AND_RELOAD** *(optional)*  
# MAGIC    Resets & reloads the pipeline by calling 01 → 02 → 03 in order.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives (By End of Track)
# MAGIC
# MAGIC You will be able to:
# MAGIC - Apply a **Bronze/Silver/Gold** architecture in Databricks
# MAGIC - Build **robust timestamp parsing** using `try_to_timestamp()` + `COALESCE()`
# MAGIC - Standardize messy text fields using `TRIM`, `UPPER`, regex cleansing
# MAGIC - Implement **data quality flags** (bad timestamps, negative durations, invalid IDs)
# MAGIC - Create **Gold facts + dims** suitable for BI dashboards
# MAGIC - Join datasets to a **daily weather calendar** using UTC date logic
# MAGIC - (Bonus) Join events to **hourly weather** using nearest timestamp logic
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datasets in This Track
# MAGIC
# MAGIC ### 1) Weather (Open-Meteo API)
# MAGIC - Stored as daily (and optionally hourly) weather tables
# MAGIC - Calendar date is based on **UTC date (default)**
# MAGIC
# MAGIC ### 2) Outages (messy operational feed)
# MAGIC Typical messy issues:
# MAGIC - Mixed timestamp formats
# MAGIC - Empty or malformed text fields
# MAGIC - Numeric values embedded in strings (e.g., "1,200 customers")
# MAGIC
# MAGIC Silver goals:
# MAGIC - Parse timestamps reliably
# MAGIC - Standardize cause/equipment/severity categories
# MAGIC - Compute duration when missing
# MAGIC - Add helpful data quality flags
# MAGIC
# MAGIC ### 3) Work Orders (messy operational feed)
# MAGIC Typical messy issues:
# MAGIC - Mixed timestamp formats and missing seconds
# MAGIC - “Y/N/Yes/No/1/0” boolean signals
# MAGIC - Crew IDs containing punctuation
# MAGIC
# MAGIC Silver goals:
# MAGIC - Parse timestamps reliably
# MAGIC - Normalize crew_id to uppercase alphanumeric
# MAGIC - Standardize priority / type
# MAGIC - Compute SLA flag (completed within 48 hours)
# MAGIC - Add data quality flags
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture: Tables by Layer
# MAGIC
# MAGIC ### Bronze (Raw)
# MAGIC - `bronze_layer.weather_raw` *(or similar)*
# MAGIC - `bronze_layer.outages_raw`
# MAGIC - `bronze_layer.work_orders_raw`
# MAGIC
# MAGIC Bronze is: **raw + messy + minimal transformations**
# MAGIC
# MAGIC ### Silver (Cleaned)
# MAGIC - `silver_layer.weather_daily` *(or similar daily output)*
# MAGIC - `silver_layer.outages`
# MAGIC - `silver_layer.work_orders`
# MAGIC
# MAGIC Silver is: **cleaned + normalized + standardized types**
# MAGIC
# MAGIC ### Gold (Analytics Ready)
# MAGIC **Dimensions**
# MAGIC - `gold_layer.dim_location` *(built from locations.py list in Python)*
# MAGIC - `gold_layer.dim_date`
# MAGIC
# MAGIC **Facts**
# MAGIC - `gold_layer.fact_weather_daily`
# MAGIC - `gold_layer.fact_outages_daily`
# MAGIC - `gold_layer.fact_work_orders_daily`
# MAGIC
# MAGIC Gold is: **metrics-ready + join-friendly + dashboard-ready**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Files & Bronze Volume Setup
# MAGIC
# MAGIC This training includes **sample CSV files** used for Bronze-layer ingestion.
# MAGIC
# MAGIC ### Included Files
# MAGIC The following files are provided in the training package:
# MAGIC
# MAGIC - data/ bronze_outages_raw_messy.csv
# MAGIC - data/ bronze_work_orders_raw_messy.csv
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ⚠️ Important — Complete This Step Before Starting the Labs
# MAGIC
# MAGIC Before starting **any lab**, you must ensure the **Bronze raw volume** exists.  
# MAGIC All data ingestion and downstream labs depend on this volume.
# MAGIC
# MAGIC ### Step 1: Create the Bronze Raw Volume
# MAGIC
# MAGIC Run the following SQL **once** in Databricks (or confirm it already exists):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE VOLUME IF NOT EXISTS bronze_layer.raw;
# MAGIC This creates the managed Databricks Volume used to store raw CSV files for Bronze ingestion
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###CSV File Placement in Volumes
# MAGIC
# MAGIC When you run the **01_DA_TRACK_SEED_Bronze notebook**, it will:
# MAGIC
# MAGIC - Create the Bronze volume (if it does not already exist)
# MAGIC
# MAGIC - Copy the CSV files into the following Databricks Volume paths:
# MAGIC
# MAGIC - **/Volumes/workspace/bronze_layer/raw/outages/** bronze_outages_raw_messy.csv
# MAGIC
# MAGIC - **/Volumes/workspace/bronze_layer/raw/work_orders/** bronze_work_orders_raw_messy.csv
# MAGIC
# MAGIC
# MAGIC These volume paths are then used to load data into the Bronze tables:
# MAGIC
# MAGIC - bronze_layer.outages_raw
# MAGIC
# MAGIC - bronze_layer.work_orders_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ###How to Load the CSV Data
# MAGIC
# MAGIC - Import the full training folder into your Databricks workspace
# MAGIC
# MAGIC - Do not move or rename the CSV files inside /data/bronze/
# MAGIC
# MAGIC - Open and run the notebook:
# MAGIC
# MAGIC **01_DA_TRACK_SEED_Bronze**
# MAGIC
# MAGIC
# MAGIC This notebook handles:
# MAGIC
# MAGIC - Volume creation
# MAGIC
# MAGIC - File copy into volumes
# MAGIC
# MAGIC - Bronze table creation and data loading

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab 1 — Bronze Ingestion (01_DA_TRACK_SEED_Bronze)
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC - Understand Bronze layer purpose and structure
# MAGIC - Load raw files into tables with minimal assumptions
# MAGIC - Validate raw row counts and schema shape
# MAGIC
# MAGIC ### Key Tasks
# MAGIC - Run the notebook end-to-end
# MAGIC - Confirm the following tables exist and contain data:
# MAGIC   - `bronze_layer.outages_raw`
# MAGIC   - `bronze_layer.work_orders_raw`
# MAGIC - Run quick checks:
# MAGIC   - row counts
# MAGIC   - sample records
# MAGIC   - inspect timestamp format variety
# MAGIC
# MAGIC ### Checks
# MAGIC ```sql
# MAGIC SELECT COUNT(*) FROM bronze_layer.outages_raw;
# MAGIC SELECT COUNT(*) FROM bronze_layer.work_orders_raw;
# MAGIC
# MAGIC SELECT * FROM bronze_layer.outages_raw LIMIT 10;
# MAGIC SELECT * FROM bronze_layer.work_orders_raw LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Lab 2 — Silver Cleaning (02_DA_TRACK_SILVER_CLEANING)
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC - Clean messy timestamp formats safely with `try_to_timestamp()`
# MAGIC - Normalize text fields consistently (case/trim/regex)
# MAGIC - Create data quality flags that support troubleshooting & governance
# MAGIC
# MAGIC ### Key Tasks
# MAGIC - Run the notebook end-to-end
# MAGIC - Validate parsing success rates:
# MAGIC   - start_time not null
# MAGIC   - created_ts not null
# MAGIC - Confirm location_id filters are correct (LOC### format)
# MAGIC - Validate row counts are non-zero after filters
# MAGIC
# MAGIC ### Checks
# MAGIC ```sql
# MAGIC SELECT COUNT(*) FROM silver_layer.outages;
# MAGIC SELECT COUNT(*) FROM silver_layer.work_orders;
# MAGIC
# MAGIC SELECT * FROM silver_layer.outages LIMIT 10;
# MAGIC SELECT * FROM silver_layer.work_orders LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Lab 3 — Gold Facts + Dims (03_DA_TRACK_GOLD_AGGREGATIONS)
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC - Build a clean daily analytics schema (facts + dims)
# MAGIC - Join outages/work orders to daily weather by:
# MAGIC   - `location_id + date (UTC calendar)`
# MAGIC - Produce dashboard-ready daily metrics
# MAGIC
# MAGIC ### Key Outputs
# MAGIC - `gold_layer.dim_location` *(from locations.py list)*
# MAGIC - `gold_layer.dim_date`
# MAGIC - `gold_layer.fact_weather_daily`
# MAGIC - `gold_layer.fact_outages_daily`
# MAGIC - `gold_layer.fact_work_orders_daily`
# MAGIC
# MAGIC ### Checks
# MAGIC ```sql
# MAGIC SELECT COUNT(*) FROM gold_layer.fact_weather_daily;
# MAGIC SELECT COUNT(*) FROM gold_layer.fact_outages_daily;
# MAGIC SELECT COUNT(*) FROM gold_layer.fact_work_orders_daily;
# MAGIC
# MAGIC SELECT * FROM gold_layer.fact_outages_daily ORDER BY date DESC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Lab 4 — Build an Analytics Dashboard in Databricks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ###Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC
# MAGIC - Build a Databricks SQL Dashboard using Gold-layer tables
# MAGIC - Design KPI, trend, and breakdown visualizations
# MAGIC - Select appropriate chart types for operational analytics
# MAGIC - Use clean Gold tables for BI without re-cleaning data
# MAGIC - Apply dashboard filters for slicing metrics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ###Business Scenario
# MAGIC
# MAGIC You are an analytics engineer supporting grid operations leadership.
# MAGIC
# MAGIC Your goal is to create an **Operational & Weather Impact Dashboard** that answers:
# MAGIC
# MAGIC - How many outages occur per day?
# MAGIC - How many customers are impacted?
# MAGIC - Are outages correlated with weather severity?
# MAGIC - Are work orders meeting SLA targets?
# MAGIC - Which locations experience the most disruption?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ###Data Sources (Gold Layer Only)
# MAGIC
# MAGIC | Table | Description |
# MAGIC |-----|-------------|
# MAGIC | `gold_layer.fact_outages_daily` | Daily outage metrics |
# MAGIC | `gold_layer.fact_work_orders_daily` | Daily work order metrics |
# MAGIC | `gold_layer.fact_weather_daily` | Daily weather metrics |
# MAGIC | `gold_layer.dim_date` | Calendar dimension |
# MAGIC | `gold_layer.dim_location` | Location dimension |
# MAGIC
# MAGIC > ⚠️ Dashboards should always query **Gold tables**, not Bronze or Silver.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ###Recommended Dashboard Layout
# MAGIC
# MAGIC ### Section 1 — KPI Tiles
# MAGIC - Total Outages
# MAGIC - Customers Affected
# MAGIC - Work Order SLA %
# MAGIC - Days with Severe Weather
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Section 2 — Trends Over Time
# MAGIC - Outages per Day
# MAGIC - Customers Affected per Day
# MAGIC - Work Orders per Day
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Section 3 — Breakdown Analysis
# MAGIC - Outages by Weather Severity
# MAGIC - SLA Compliance by Priority
# MAGIC - Top Locations by Outage Count
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ###Dashboard SQL Queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### KPI 1— Total Outages
# MAGIC
# MAGIC SELECT
# MAGIC   SUM(outage_count) AS total_outages
# MAGIC FROM gold_layer.fact_outages_daily;
# MAGIC
# MAGIC Visualization: KPI / Single Val
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### KPI 2— Total Customers Affected
# MAGIC SELECT
# MAGIC   SUM(customers_affected) AS total_customers_affected
# MAGIC FROM gold_layer.fact_outages_daily;
# MAGIC
# MAGIC
# MAGIC Visualization: KPI / Single Value
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### KPI 3 — Work Order SLA %
# MAGIC SELECT
# MAGIC   ROUND(
# MAGIC     100.0 * SUM(CASE WHEN sla_met_flag THEN work_order_count ELSE 0 END)
# MAGIC     / NULLIF(SUM(work_order_count), 0),
# MAGIC     1
# MAGIC   ) AS sla_percentage
# MAGIC FROM gold_layer.fact_work_orders_daily;
# MAGIC
# MAGIC Visualization: KPI / Gauge
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### kpi 4-Outages Over Time
# MAGIC SELECT
# MAGIC   date,
# MAGIC   SUM(outage_count) AS outages
# MAGIC FROM gold_layer.fact_outages_daily
# MAGIC GROUP BY date
# MAGIC ORDER BY date;
# MAGIC
# MAGIC Visualization: Line Chart
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### KPI 5- Customers Affected Over Time
# MAGIC SELECT
# MAGIC   date,
# MAGIC   SUM(customers_affected) AS customers_affected
# MAGIC FROM gold_layer.fact_outages_daily
# MAGIC GROUP BY date
# MAGIC ORDER BY date;
# MAGIC
# MAGIC Visualization: Line or Area Chart
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ###  KPI 6- Outages by Weather Severity
# MAGIC SELECT
# MAGIC   weather_severity,
# MAGIC   SUM(outage_count) AS outages
# MAGIC FROM gold_layer.fact_outages_daily
# MAGIC GROUP BY weather_severity
# MAGIC ORDER BY outages DESC;
# MAGIC
# MAGIC Visualization: Bar Chart
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### KPI 7- Work Orders by Priority
# MAGIC SELECT
# MAGIC   priority,
# MAGIC   SUM(work_order_count) AS work_orders
# MAGIC FROM gold_layer.fact_work_orders_daily
# MAGIC GROUP BY priority
# MAGIC ORDER BY work_orders DESC;
# MAGIC
# MAGIC Visualization: Bar Chart
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ###  KPI 8-SLA Compliance by Priority
# MAGIC SELECT
# MAGIC   priority,
# MAGIC   ROUND(
# MAGIC     100.0 * SUM(CASE WHEN sla_met_flag THEN work_order_count ELSE 0 END)
# MAGIC     / NULLIF(SUM(work_order_count), 0),
# MAGIC     1
# MAGIC   ) AS sla_pct
# MAGIC FROM gold_layer.fact_work_orders_daily
# MAGIC GROUP BY priority;
# MAGIC
# MAGIC Visualization: Bar Chart (Percentage)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### KPI 9-Top 10 Locations by Outages
# MAGIC SELECT
# MAGIC   l.location_name,
# MAGIC   SUM(f.outage_count) AS outages
# MAGIC FROM gold_layer.fact_outages_daily f
# MAGIC JOIN gold_layer.dim_location l
# MAGIC   ON f.location_id = l.location_id
# MAGIC GROUP BY l.location_name
# MAGIC ORDER BY outages DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC Visualization: Horizontal Bar Chart
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Optional Dashboard Filters
# MAGIC
# MAGIC Add dashboard-level filters for:
# MAGIC
# MAGIC - Date range
# MAGIC
# MAGIC - Location
# MAGIC
# MAGIC - Weather severity
# MAGIC
# MAGIC These filters allow business users to explore outage and work order trends interactively without modifying SQL.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bonus Lab — Hourly-Nearest Weather Join (Optional / Advanced)
# MAGIC
# MAGIC ### Learning Objective
# MAGIC Join event timestamps (outage start, work order created) to the nearest hourly weather record.
# MAGIC
# MAGIC ### Why This Matters
# MAGIC Daily joins are easier and great for dashboards.
# MAGIC Hourly-nearest joins allow more accurate causality analysis (e.g., weather at the moment of an outage).
# MAGIC
# MAGIC ### What You Will Learn
# MAGIC - Window functions
# MAGIC - nearest timestamp matching
# MAGIC - tolerance windows (±1–3 hours)
# MAGIC
# MAGIC If your training includes this, run the “04_DA_TRACK_BONUS_HOURLY_WEATHER_JOIN" notebook.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset / Reload (99_DA_TRACK_RESET_AND_RELOAD)
# MAGIC
# MAGIC This notebook is optional and intended for:
# MAGIC - Instructor resets
# MAGIC - Re-running from scratch
# MAGIC - Fixing tables after adjustments
# MAGIC
# MAGIC ### Best Practice
# MAGIC `99_...` should **run 01 → 02 → 03** in order (recommended training flow).
# MAGIC
# MAGIC ### Instructor Tip
# MAGIC Use this notebook to quickly restore a clean working state for a new cohort.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Quick Guide
# MAGIC
# MAGIC ### 1) Silver table returns 0 rows
# MAGIC Most common cause: location_id regex is too strict or wrong.
# MAGIC Use:
# MAGIC - `^LOC[0-9]{3}$` for `LOC001` style IDs
# MAGIC
# MAGIC ### 2) Timestamp parsing errors
# MAGIC Use `try_to_timestamp()` wrapped in `COALESCE()` with multiple patterns.
# MAGIC Never use `to_timestamp()` directly on messy raw strings.
# MAGIC
# MAGIC ### 3) Validate the raw data quickly
# MAGIC ```sql
# MAGIC SELECT location_id, COUNT(*) 
# MAGIC FROM bronze_layer.work_orders_raw
# MAGIC GROUP BY location_id
# MAGIC ORDER BY COUNT(*) DESC;
# MAGIC
