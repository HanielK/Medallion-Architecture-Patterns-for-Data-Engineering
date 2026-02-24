# Databricks notebook source
# MAGIC %md
# MAGIC ## 04A — BONUS LAB (Challenge): Hourly-Nearest Weather Join (UTC)
# MAGIC
# MAGIC ####**Goal:** Build an enrichment join that matches each event to the *closest hourly weather observation*.
# MAGIC
# MAGIC
# MAGIC #### Learning Objectives
# MAGIC ##### 1. Use `try_to_timestamp` / consistent timestamp columns for joins.
# MAGIC #####2. Compute time distance in seconds/hours.
# MAGIC ##### 3. Use window functions (`ROW_NUMBER`) to pick the closest match.
# MAGIC ##### 4. Apply a tolerance window (e.g., within ±2 hours).
# MAGIC #####5. Validate results with match-rate metrics.
# MAGIC
# MAGIC
# MAGIC #### Rules
# MAGIC #####Use UTC timestamps
# MAGIC #####Join by `location_id`
# MAGIC ##### Pick the nearest weather hour per event
# MAGIC #####Add `diff_seconds`, `diff_hours`, and tolerance filtering
# MAGIC
# MAGIC
# MAGIC #### Tables
# MAGIC #####`silver_layer.weather_hourly`
# MAGIC #####`silver_layer.outages`
# MAGIC #####`silver_layer.work_orders`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0) Setup: choose your weather timestamp column
# MAGIC #####Update this if your weather table uses a different timestamp field.

# COMMAND ----------



# COMMAND ----------

WEATHER_TS_COL = "timestamp"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) Task: Create a normalized hourly weather temp view
# MAGIC #### MAGIC Create:
# MAGIC ####`v_weather_hourly_norm`
# MAGIC
# MAGIC #### Requirements:
# MAGIC #### - Must output: `location_id`, `weather_ts_utc_norm`, and at least 3 measures (temp, precip, wind).
# MAGIC ####- Filter to valid location ids with `RLIKE '^LOC[0-9]{3}$'`
# MAGIC
# MAGIC #### **Hint:** `CAST({WEATHER_TS_COL} AS TIMESTAMP)` as `weather_ts_utc_norm`

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC #TO DO: Create v_weather_hourly_norm
# MAGIC # CREATE OR REPLACE TEMP VIEW v_weather_hourly_norm AS
# MAGIC # SELECT
# MAGIC # location_id,
# MAGIC # CAST(<<WEATHER_TS_COL>> AS TIMESTAMP) AS weather_ts_utc_norm,
# MAGIC # <<temperature column>> AS temperature_2m,
# MAGIC # <<precip column>>      AS precipitation,
# MAGIC # <<wind column>>        AS windspeed_10m
# MAGIC # FROM silver_layer.weather_hourly
# MAGIC # WHERE location_id RLIKE '^LOC[0-9]{3}$'
# MAGIC # AND <<WEATHER_TS_COL>> IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check your view works

# COMMAND ----------



# COMMAND ----------

display(spark.sql("SELECT * FROM v_weather_hourly_norm LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2) Task: Outages → nearest hourly weather
# MAGIC #### Requirements:
# MAGIC ##### Candidate join: outages ↔ hourly weather by `location_id`
# MAGIC ###### Compute `diff_seconds = ABS(unix_timestamp(weather_ts) - unix_timestamp(start_time))`
# MAGIC ##### Use `ROW_NUMBER()` partitioned by `outage_id` ordered by smallest diff
# MAGIC ##### Keep only the best match
# MAGIC
# MAGIC #### **Bonus:** Add a tie-breaker on `weather_ts_utc_norm ASC

# COMMAND ----------



# COMMAND ----------

# MAGIC  %sql
# MAGIC # TO DO: Create v_outages_nearest_weather
# MAGIC # CREATE OR REPLACE TEMP VIEW v_outages_nearest_weather AS
# MAGIC # WITH candidates AS (
# MAGIC #   SELECT
# MAGIC #     o.outage_id,
# MAGIC #     o.location_id,
# MAGIC #     o.start_time,
# MAGIC #     w.weather_ts_utc_norm,
# MAGIC #     w.temperature_2m,
# MAGIC #     w.precipitation,
# MAGIC #     w.windspeed_10m,
# MAGIC #     ABS(unix_timestamp(w.weather_ts_utc_norm) - unix_timestamp(o.start_time)) AS diff_seconds
# MAGIC #   FROM silver_layer.outages o
# MAGIC #   JOIN v_weather_hourly_norm w
# MAGIC #     ON o.location_id = w.location_id
# MAGIC #   WHERE o.start_time IS NOT NULL
# MAGIC # ),
# MAGIC # ranked AS (
# MAGIC #   SELECT *,
# MAGIC #          ROW_NUMBER() OVER (
# MAGIC #            PARTITION BY outage_id
# MAGIC #            ORDER BY diff_seconds ASC, weather_ts_utc_norm ASC
# MAGIC #          ) AS rn
# MAGIC #   FROM candidates
# MAGIC # )
# MAGIC # SELECT
# MAGIC #   outage_id, location_id, start_time,
# MAGIC #   weather_ts_utc_norm AS matched_weather_hour_utc,
# MAGIC #   diff_seconds,
# MAGIC #   diff_seconds/3600.0 AS diff_hours,
# MAGIC #   temperature_2m, precipitation, windspeed_10m
# MAGIC # FROM ranked
# MAGIC # WHERE rn = 1;
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

display(spark.sql("SELECT * FROM v_outages_nearest_weather LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3) Task: Apply a tolerance window to outages
# MAGIC ####Create:
# MAGIC #####`v_outages_nearest_weather_tolerant`
# MAGIC
# MAGIC ####Requirement:
# MAGIC #####Keep only matches within ±`TOLERANCE_HOURS`

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC # TO DO: tolerance-filtered outages view
# MAGIC # CREATE OR REPLACE TEMP VIEW v_outages_nearest_weather_tolerant AS
# MAGIC # SELECT *
# MAGIC # FROM v_outages_nearest_weather
# MAGIC # WHERE diff_seconds <= ${TOLERANCE_HOURS} * 3600;

# COMMAND ----------



# COMMAND ----------

display(spark.sql("SELECT * FROM v_outages_nearest_weather_tolerant LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4) Task: Work Orders → nearest hourly weather
# MAGIC ####Create:
# MAGIC ##### `v_work_orders_nearest_weather`
# MAGIC #####`v_work_orders_nearest_weather_tolerant`
# MAGIC
# MAGIC ####Requirements:
# MAGIC ##### Use `created_ts` as the event timestamp
# MAGIC #####Partition by `wo_id`

# COMMAND ----------

 

# COMMAND ----------

# MAGIC  %sql
# MAGIC # TO DO: Create v_work_orders_nearest_weather (same pattern as outages)
# MAGIC # (1) candidates: join by location_id, compute diff_seconds to created_ts
# MAGIC # (2) ranked: row_number partition by wo_id
# MAGIC # (3) select rn = 1
# MAGIC
# MAGIC # CREATE OR REPLACE TEMP VIEW v_work_orders_nearest_weather AS

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC # TO DO: Create v_work_orders_nearest_weather_tolerant
# MAGIC # CREATE OR REPLACE TEMP VIEW v_work_orders_nearest_weather_tolerant AS
# MAGIC # SELECT *
# MAGIC # FROM v_work_orders_nearest_weather
# MAGIC # WHERE diff_seconds <= ${TOLERANCE_HOURS} * 3600;

# COMMAND ----------

display(spark.sql("SELECT * FROM v_work_orders_nearest_weather LIMIT 20"))
display(spark.sql("SELECT * FROM v_work_orders_nearest_weather_tolerant LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5) Task: DQ metrics (match rate)
# MAGIC ####Write two queries:
# MAGIC #####outages match rate
# MAGIC ##### work orders match rate
# MAGIC
# MAGIC ####Requirements:
# MAGIC ##### total rows
# MAGIC ##### matched rows within tolerance
# MAGIC ##### match_rate_pct
# MAGIC ##### avg diff_hours

# COMMAND ----------



# COMMAND ----------

# MAGIC  %sql
# MAGIC # TO DO: DQ metrics for outages
# MAGIC # SELECT ... FROM v_outages_nearest_weather;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC # TO DO: DQ metrics for work orders
# MAGIC # SELECT ... FROM v_work_orders_nearest_weather;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6) Optional Challenge: Persist to Gold
# MAGIC #####If your environment supports it, persist:
# MAGIC #####`gold_layer.fact_outages_hourly_enriched`
# MAGIC #####`gold_layer.fact_work_orders_hourly_enriched`
# MAGIC
# MAGIC ####Keep only tolerant matches

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC # TO DO: Persist tolerant views to gold tables (optional)
# MAGIC # CREATE OR REPLACE TABLE gold_layer.fact_outages_hourly_enriched AS
# MAGIC # SELECT * FROM v_outages_nearest_weather_tolerant;
# MAGIC
# MAGIC # CREATE OR REPLACE TABLE gold_layer.fact_work_orders_hourly_enriched AS
# MAGIC # SELECT * FROM v_work_orders_nearest_weather_tolerant;
