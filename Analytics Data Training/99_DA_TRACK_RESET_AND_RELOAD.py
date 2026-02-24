# Databricks notebook source
# MAGIC %md
# MAGIC ####🎯 Purpose of This Notebook
# MAGIC
# MAGIC This notebook provides a full reset of the DA Track Medallion Lakehouse:
# MAGIC
# MAGIC 🗑️ Clears Gold, Silver, and Bronze tables
# MAGIC
# MAGIC 🔁 Reloads Bronze data (Outages, Work Orders) from CSV files
# MAGIC
# MAGIC 🧹 Rebuilds Silver cleaned tables
# MAGIC
# MAGIC 📊 Rebuilds Gold fact tables + combined analytics view
# MAGIC
# MAGIC This notebook is designed so you can rerun labs multiple times, or quickly fix any broken state during training.
# MAGIC
# MAGIC 🧠 Assumptions:
# MAGIC
# MAGIC - Your CSVs are already in:
# MAGIC - /Volumes/bronze_layer/raw/outages/
# MAGIC - /Volumes/bronze_layer/raw/work_orders/
# MAGIC - Weather Bronze (bronze_layer.weather) is populated by a separate Python/API notebook. Weather reload can be slow (API calls). I’ll mark that as optional reset. Use the toggle below.

# COMMAND ----------

# MAGIC %md
# MAGIC ####🧪 How to Use This in Training
# MAGIC
# MAGIC Use if:
# MAGIC
# MAGIC - You want to restart the lab from scratch
# MAGIC - Your data tables got corrupted or overwritten
# MAGIC - You want to validate the full pipeline end-to-end
# MAGIC - “If anything breaks or gets messy, just run 99_DA_TRACK_RESET_AND_RELOAD top-to-bottom.”
# MAGIC - It gives them a clean, deterministic reset of:
# MAGIC
# MAGIC   - Bronze (Outages + Work Orders)
# MAGIC   - Silver (cleaned)
# MAGIC   - Gold (facts + view

# COMMAND ----------

#Toggle: Reload weather from API?
RELOAD_WEATHER = False   # set True if you want to reload bronze_layer.weather

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 0 — Drop dependent objects (Gold → Silver → Bronze)
# MAGIC #####We drop in this order to avoid dependency errors (views depend on tables).

# COMMAND ----------



# COMMAND ----------

# (Optional) Set catalog & schema if using Unity Catalog
#USE CATALOG your_catalog_name;
# USE SCHEMA default;

########################################################
#  STEP 0: OVERVIEW
#  We drop in this order to avoid dependency errors (views depend on tables).
#  This notebook:
#   0) Drops / truncates Gold, Silver, and Bronze tables
#   1) Reloads Bronze outages & work orders from CSV
#   2) Optional: Reload Bronze Weather data via API
#   3) Rebuilds Silver tables
#   4) Rebuilds Gold tables and view
#   5) Perform Quick validation checks
########################################################

drop_statements = [
    "DROP VIEW IF EXISTS gold_layer.v_operational_weather_view",
    "DROP TABLE IF EXISTS gold_layer.fact_work_orders_daily",
    "DROP TABLE IF EXISTS gold_layer.fact_outages_daily",
    "DROP TABLE IF EXISTS gold_layer.fact_weather_daily",
    "DROP TABLE IF EXISTS gold_layer.dim_date",
    "DROP TABLE IF EXISTS silver_layer.work_orders",
    "DROP TABLE IF EXISTS silver_layer.outages",
    "DROP TABLE IF EXISTS silver_layer.weather_hourly",
    # "DROP TABLE IF EXISTS bronze_layer.weather",  # drop weather bronze based on toggle (handled later)
    "DROP TABLE IF EXISTS bronze_layer.work_orders_raw",
    "DROP TABLE IF EXISTS bronze_layer.outages_raw"
]

for stmt in drop_statements:
    spark.sql(stmt)

print("✅ Reset complete: dropped Gold/Silver tables + Bronze raw tables (weather kept unless reloaded).")



# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 — Run the Bronze Seed notebook (01)
# MAGIC ######This recreates schemas/volumes, Bronze raw tables, and reloads CSV data into Bronze.

# COMMAND ----------

# MAGIC %run "./01_DA_TRACK_SEED_Bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 — Optional: Reload Bronze Weather (API)
# MAGIC #####Only enable this if you're intentionally refreshing weather data.

# COMMAND ----------

if RELOAD_WEATHER:
    print("🌦️ Reloading weather data...")
    spark.sql("DROP TABLE IF EXISTS bronze_layer.weather")
%run "./open_meteo_weather_import"
else:
    print("⏭️ Skipping weather reload (RELOAD_WEATHER=False). Using existing bronze_layer.weather if present.")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 — Build Silver layer (02)
# MAGIC ##### Cleans and normalizes weather, outages, and work orders.

# COMMAND ----------

# MAGIC %run "./02_DA_TRACK_SILVER_CLEANING"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 — Build Gold layer (03)
# MAGIC #####Creates dim_date, daily fact tables, and the combined operational view.

# COMMAND ----------

# MAGIC %run "./03_DA_TRACK_GOLD_ANALYTICS"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5 — Quick validation checks

# COMMAND ----------



# COMMAND ----------

display(spark.sql("SELECT COUNT(*) AS bronze_outages_raw FROM bronze_layer.outages_raw"))
display(spark.sql("SELECT COUNT(*) AS bronze_work_orders_raw FROM bronze_layer.work_orders_raw"))

display(spark.sql("SELECT COUNT(*) AS silver_outages FROM silver_layer.outages"))
display(spark.sql("SELECT COUNT(*) AS silver_work_orders FROM silver_layer.work_orders"))
display(spark.sql("SELECT COUNT(*) AS silver_weather_hourly FROM silver_layer.weather_hourly"))

display(spark.sql("SELECT COUNT(*) AS gold_fact_weather_daily FROM gold_layer.fact_weather_daily"))
display(spark.sql("SELECT COUNT(*) AS gold_fact_outages_daily FROM gold_layer.fact_outages_daily"))
display(spark.sql("SELECT COUNT(*) AS gold_fact_work_orders_daily FROM gold_layer.fact_work_orders_daily"))

display(spark.sql("SELECT * FROM gold_layer.v_operational_weather_daily LIMIT 10"))

print("Pipeline reload complete.")
