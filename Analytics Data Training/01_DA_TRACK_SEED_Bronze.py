# Databricks notebook source
# MAGIC %md
# MAGIC #SEED
# MAGIC Run this notebook to seed your Data Analytics resources:
# MAGIC
# MAGIC Purpose: Setup + Bronze Ingestion
# MAGIC
# MAGIC Includes:
# MAGIC
# MAGIC | Step                    |
# MAGIC |-------------------------|
# MAGIC | ✔ Create UC Schemas     |
# MAGIC | ✔ Create Volumes        |
# MAGIC | ✔ Create Bronze tables  |
# MAGIC | ✔ COPY INTO Bronze - Ingests messy CSV     |
# MAGIC | ✔ Stops. |

# COMMAND ----------

# MAGIC %md
# MAGIC ####Schemas and Volumes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze_layer;
# MAGIC CREATE VOLUME IF NOT EXISTS bronze_layer.landing;
# MAGIC CREATE VOLUME IF NOT EXISTS bronze_layer.raw;

# COMMAND ----------

# DBTITLE 1,SILVER
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_layer;
# MAGIC CREATE VOLUME IF NOT EXISTS silver_layer.currated;

# COMMAND ----------

# DBTITLE 1,GOLD
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold_layer;
# MAGIC CREATE VOLUME IF NOT EXISTS gold_layer.aggeragated; 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Bronze tables (EXPLICIT definitions)

# COMMAND ----------

# MAGIC %sql
# MAGIC -----------------------------------------
# MAGIC -- Bronze Outages (messy raw ingestion)
# MAGIC -----------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS bronze_layer.outages_raw (
# MAGIC   outage_id              STRING,
# MAGIC   location_id            STRING,
# MAGIC   start_time_raw         STRING,
# MAGIC   end_time_raw           STRING,
# MAGIC   duration_minutes_raw   STRING,
# MAGIC   cause_code_raw         STRING,
# MAGIC   equipment_type_raw     STRING,
# MAGIC   weather_severity_raw   STRING,
# MAGIC   customers_affected_raw STRING,
# MAGIC   _ingest_ts             STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -----------------------------------------
# MAGIC -- Bronze Work Orders (messy raw ingestion)
# MAGIC -----------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS bronze_layer.work_orders_raw (
# MAGIC   wo_id               STRING,
# MAGIC   location_id         STRING,
# MAGIC   created_ts_raw      STRING,
# MAGIC   scheduled_ts_raw    STRING,
# MAGIC   completed_ts_raw    STRING,
# MAGIC   crew_id_raw         STRING,
# MAGIC   wo_type_raw         STRING,
# MAGIC   weather_related_raw STRING,
# MAGIC   priority_raw        STRING,
# MAGIC   _ingest_ts          STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####COPY INTO commands (still in Bronze) - Add these after the CREATE TABLE to populate with data from Volumes

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronze_layer.outages_raw
# MAGIC FROM '/Volumes/workspace/bronze_layer/raw/outages/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true');
# MAGIC
# MAGIC COPY INTO bronze_layer.work_orders_raw
# MAGIC FROM '/Volumes/workspace/bronze_layer/raw/work_orders/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COMMAND ----------
# MAGIC -- Quick sanity checks
# MAGIC SELECT COUNT(*) AS outage_row_count FROM bronze_layer.outages_raw;
# MAGIC
# MAGIC -- COMMAND ----------
# MAGIC SELECT COUNT(*) AS work_order_row_count FROM bronze_layer.work_orders_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Generator Function Derived Table

# COMMAND ----------

# MAGIC %md
# MAGIC ##Notebook Call

# COMMAND ----------

# DBTITLE 1,Open Meteo
# MAGIC %run ./open_meteo_weather_import
