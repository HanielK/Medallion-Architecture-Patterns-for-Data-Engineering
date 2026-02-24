# Databricks notebook source
# MAGIC %md
# MAGIC Purpose: All Silver SQL
# MAGIC
# MAGIC Includes:
# MAGIC
# MAGIC | Step                                                      | Description                |
# MAGIC |-----------------------------------------------------------|----------------------------|
# MAGIC | ✔ CREATE OR REPLACE TABLE silver_layer.weather_hourly     | arrays_zip + explode       |
# MAGIC | ✔ CREATE OR REPLACE TABLE silver_layer.outages            |                            |
# MAGIC | ✔ CREATE OR REPLACE TABLE silver_layer.work_orders        |                            |
# MAGIC | ✔ Data quality filtering                                  |                            |

# COMMAND ----------

# MAGIC %md
# MAGIC ####Clean & normalize messy raw outages and turn messy work order feed into a clean, standardized event tables suitable for Gold aggregations & dashboards.
# MAGIC
# MAGIC - Reads Bronze
# MAGIC - Cleans, normalizes
# MAGIC - Creates Silver tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COMMAND ----------
# MAGIC -- SILVER: Weather (explode arrays → hourly rows)
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_layer.weather_hourly AS
# MAGIC SELECT
# MAGIC    location_id
# MAGIC   ,timestamp
# MAGIC   ,CAST(timestamp AS DATE) AS date
# MAGIC   ,relative_humidity_2m
# MAGIC   ,temperature_2m
# MAGIC   ,apparent_temperature
# MAGIC   ,surface_pressure
# MAGIC   ,wind_speed_10m
# MAGIC   ,precipitation
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC      location_id
# MAGIC     ,date_trunc('HOUR', to_timestamp(data.`hourly.time`)) AS timestamp
# MAGIC     ,CAST(
# MAGIC         to_number(lpad(data.`hourly.relative_humidity_2m`, 3, '0'), '000')
# MAGIC         AS DOUBLE
# MAGIC      ) AS relative_humidity_2m
# MAGIC     ,data.`hourly.temperature_2m`       AS temperature_2m
# MAGIC     ,data.`hourly.apparent_temperature` AS apparent_temperature
# MAGIC     ,data.`hourly.surface_pressure`     AS surface_pressure
# MAGIC     ,data.`hourly.wind_speed_10m`       AS wind_speed_10m
# MAGIC     ,data.`hourly.precipitation`        AS precipitation
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       location_id,
# MAGIC       explode(
# MAGIC         arrays_zip(
# MAGIC            weather.`hourly.time`,
# MAGIC            weather.`hourly.relative_humidity_2m`,
# MAGIC            weather.`hourly.temperature_2m`,
# MAGIC            weather.`hourly.apparent_temperature`,
# MAGIC            weather.`hourly.surface_pressure`,
# MAGIC            weather.`hourly.wind_speed_10m`,
# MAGIC            weather.`hourly.precipitation`
# MAGIC         )
# MAGIC       ) AS data
# MAGIC     FROM bronze_layer.weather AS weather
# MAGIC   ) AS exploded_data
# MAGIC ) AS aliased_data
# MAGIC WHERE temperature_2m BETWEEN -80 AND 60
# MAGIC   AND surface_pressure > 0
# MAGIC   AND wind_speed_10m >= 0;

# COMMAND ----------



# COMMAND ----------

# Quick preview of Silver weather_hourly
spark.sql(
    """
    OPTIMIZE silver_layer.weather_hourly
    ZORDER BY (location_id, date)
    """
)
#Partition by date if you expect more years later.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COMMAND ----------
# MAGIC -- Quick preview of Silver Weather
# MAGIC SELECT * FROM silver_layer.weather_hourly
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COMMAND ----------
# MAGIC -- SILVER: Outages (cleaned & normalized)
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_layer.outages AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     outage_id,
# MAGIC
# MAGIC     -- Normalize location_id (trim, uppercase, remove spaces)
# MAGIC     UPPER(regexp_replace(TRIM(location_id), '\\s+', '')) AS location_id_norm,
# MAGIC
# MAGIC     start_time_raw,
# MAGIC     end_time_raw,
# MAGIC     duration_minutes_raw,
# MAGIC     cause_code_raw,
# MAGIC     equipment_type_raw,
# MAGIC     weather_severity_raw,
# MAGIC     customers_affected_raw,
# MAGIC     _ingest_ts
# MAGIC   FROM bronze_layer.outages_raw
# MAGIC ),
# MAGIC
# MAGIC parsed AS (
# MAGIC   SELECT
# MAGIC     outage_id,
# MAGIC     location_id_norm AS location_id,
# MAGIC
# MAGIC     COALESCE(
# MAGIC       try_to_timestamp(start_time_raw, 'yyyy-MM-dd HH:mm:ss'),
# MAGIC       try_to_timestamp(start_time_raw, 'yyyy-MM-dd HH:mm'),
# MAGIC       try_to_timestamp(start_time_raw, 'yyyy/MM/dd HH:mm:ss'),
# MAGIC       try_to_timestamp(start_time_raw, 'yyyy/MM/dd HH:mm'),
# MAGIC       try_to_timestamp(start_time_raw, 'MM/dd/yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(start_time_raw, 'MM/dd/yyyy HH:mm'),
# MAGIC       try_to_timestamp(start_time_raw, 'dd-MM-yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(start_time_raw, 'dd-MM-yyyy HH:mm')
# MAGIC     ) AS start_time,
# MAGIC
# MAGIC     COALESCE(
# MAGIC       try_to_timestamp(end_time_raw, 'yyyy-MM-dd HH:mm:ss'),
# MAGIC       try_to_timestamp(end_time_raw, 'yyyy-MM-dd HH:mm'),
# MAGIC       try_to_timestamp(end_time_raw, 'yyyy/MM/dd HH:mm:ss'),
# MAGIC       try_to_timestamp(end_time_raw, 'yyyy/MM/dd HH:mm'),
# MAGIC       try_to_timestamp(end_time_raw, 'MM/dd/yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(end_time_raw, 'MM/dd/yyyy HH:mm'),
# MAGIC       try_to_timestamp(end_time_raw, 'dd-MM-yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(end_time_raw, 'dd-MM-yyyy HH:mm')
# MAGIC     ) AS end_time,
# MAGIC
# MAGIC     duration_minutes_raw,
# MAGIC     cause_code_raw,
# MAGIC     equipment_type_raw,
# MAGIC     weather_severity_raw,
# MAGIC     customers_affected_raw,
# MAGIC
# MAGIC     COALESCE(
# MAGIC       try_to_timestamp(_ingest_ts, 'yyyy-MM-dd HH:mm:ss'),
# MAGIC       try_to_timestamp(_ingest_ts, 'yyyy/MM/dd HH:mm:ss'),
# MAGIC       try_to_timestamp(_ingest_ts, 'MM/dd/yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(_ingest_ts, 'MM/dd/yyyy HH:mm')
# MAGIC     ) AS _ingest_ts
# MAGIC   FROM base
# MAGIC ),
# MAGIC
# MAGIC final AS (
# MAGIC   SELECT
# MAGIC     outage_id,
# MAGIC     location_id,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN duration_minutes_raw RLIKE '^[0-9]+$' THEN CAST(duration_minutes_raw AS INT)
# MAGIC       WHEN start_time IS NOT NULL AND end_time IS NOT NULL
# MAGIC         THEN CAST((unix_timestamp(end_time) - unix_timestamp(start_time))/60 AS INT)
# MAGIC       ELSE NULL
# MAGIC     END AS duration_minutes,
# MAGIC
# MAGIC     COALESCE(NULLIF(UPPER(TRIM(cause_code_raw)), ''), 'UNKNOWN') AS cause_code_norm,
# MAGIC     COALESCE(NULLIF(UPPER(TRIM(equipment_type_raw)), ''), 'UNKNOWN') AS equipment_type,
# MAGIC
# MAGIC     CASE
# MAGIC       WHEN UPPER(weather_severity_raw) IN ('HIGH','H','HI') THEN 'HIGH'
# MAGIC       WHEN UPPER(weather_severity_raw) IN ('MED','M','MEDIUM') THEN 'MEDIUM'
# MAGIC       WHEN UPPER(weather_severity_raw) IN ('LOW','L') THEN 'LOW'
# MAGIC       ELSE 'UNKNOWN'
# MAGIC     END AS weather_severity,
# MAGIC
# MAGIC     CAST(NULLIF(regexp_replace(customers_affected_raw, '[^0-9]', ''), '') AS INT) AS customers_affected,
# MAGIC
# MAGIC     CAST(start_time AS DATE) AS date,
# MAGIC     _ingest_ts,
# MAGIC
# MAGIC     -- DQ flags
# MAGIC     CASE WHEN start_time IS NULL THEN TRUE ELSE FALSE END AS dq_bad_start_ts,
# MAGIC     CASE WHEN end_time   IS NULL THEN TRUE ELSE FALSE END AS dq_bad_end_ts,
# MAGIC     CASE WHEN start_time IS NOT NULL AND end_time IS NOT NULL AND end_time < start_time THEN TRUE ELSE FALSE END AS dq_end_before_start,
# MAGIC     CASE WHEN duration_minutes IS NOT NULL AND duration_minutes < 0 THEN TRUE ELSE FALSE END AS dq_negative_duration,
# MAGIC
# MAGIC     -- NEW: location format check (don’t filter it out; keep as flag for training)
# MAGIC     CASE WHEN location_id RLIKE '^LOC[0-9]{3}$' THEN TRUE ELSE FALSE END AS dq_bad_location_id
# MAGIC   FROM parsed
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM final;
# MAGIC

# COMMAND ----------



# COMMAND ----------

# Quick preview of Silver Outages
result = spark.sql("SELECT COUNT(*) AS silver_cnt FROM silver_layer.outages")
display(result)

result2 = spark.sql("SELECT * FROM silver_layer.outages LIMIT 15")
display(result2)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- COMMAND ----------
# MAGIC -- SILVER: Work Orders (cleaned & normalized)
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_layer.work_orders AS
# MAGIC WITH cleaned AS (
# MAGIC   SELECT
# MAGIC     wo_id,
# MAGIC     TRIM(location_id) AS location_id,           -- normalize location_id
# MAGIC     created_ts_raw,
# MAGIC     scheduled_ts_raw,
# MAGIC     completed_ts_raw,
# MAGIC     crew_id_raw,
# MAGIC     wo_type_raw,
# MAGIC     weather_related_raw,
# MAGIC     priority_raw,
# MAGIC
# MAGIC     -- Parse timestamps (robust)
# MAGIC     COALESCE(
# MAGIC       try_to_timestamp(created_ts_raw,   'yyyy/MM/dd HH:mm:ss'),
# MAGIC       try_to_timestamp(created_ts_raw,   'yyyy-MM-dd HH:mm:ss'),
# MAGIC       try_to_timestamp(created_ts_raw,   'yyyy-MM-dd HH:mm'),
# MAGIC       try_to_timestamp(created_ts_raw,   'MM/dd/yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(created_ts_raw,   'MM/dd/yyyy HH:mm'),
# MAGIC       try_to_timestamp(created_ts_raw,   'dd-MM-yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(created_ts_raw,   'dd-MM-yyyy HH:mm')
# MAGIC     ) AS created_ts,
# MAGIC
# MAGIC     COALESCE(
# MAGIC       try_to_timestamp(scheduled_ts_raw, 'yyyy/MM/dd HH:mm:ss'),
# MAGIC       try_to_timestamp(scheduled_ts_raw, 'yyyy-MM-dd HH:mm:ss'),
# MAGIC       try_to_timestamp(scheduled_ts_raw, 'yyyy-MM-dd HH:mm'),
# MAGIC       try_to_timestamp(scheduled_ts_raw, 'MM/dd/yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(scheduled_ts_raw, 'MM/dd/yyyy HH:mm'),
# MAGIC       try_to_timestamp(scheduled_ts_raw, 'dd-MM-yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(scheduled_ts_raw, 'dd-MM-yyyy HH:mm')
# MAGIC     ) AS scheduled_ts,
# MAGIC
# MAGIC     COALESCE(
# MAGIC       try_to_timestamp(completed_ts_raw, 'yyyy/MM/dd HH:mm:ss'),
# MAGIC       try_to_timestamp(completed_ts_raw, 'yyyy-MM-dd HH:mm:ss'),
# MAGIC       try_to_timestamp(completed_ts_raw, 'yyyy-MM-dd HH:mm'),
# MAGIC       try_to_timestamp(completed_ts_raw, 'MM/dd/yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(completed_ts_raw, 'MM/dd/yyyy HH:mm'),
# MAGIC       try_to_timestamp(completed_ts_raw, 'dd-MM-yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(completed_ts_raw, 'dd-MM-yyyy HH:mm')
# MAGIC     ) AS completed_ts,
# MAGIC
# MAGIC     -- Normalize crew_id (remove non-alphanumeric, uppercase)
# MAGIC     UPPER(regexp_replace(crew_id_raw, '[^A-Z0-9]', '')) AS crew_id,
# MAGIC
# MAGIC     -- Normalize work order type
# MAGIC     COALESCE(NULLIF(UPPER(TRIM(wo_type_raw)), ''), 'UNKNOWN') AS wo_type,
# MAGIC
# MAGIC     -- Weather related: allow NULL for unknown (recommended for training/data quality)
# MAGIC     CASE
# MAGIC       WHEN UPPER(weather_related_raw) IN ('Y','YES','1','TRUE') THEN TRUE
# MAGIC       WHEN UPPER(weather_related_raw) IN ('N','NO','0','FALSE') THEN FALSE
# MAGIC       ELSE NULL
# MAGIC     END AS weather_related_flag,
# MAGIC
# MAGIC     -- Normalize priority
# MAGIC     CASE
# MAGIC       WHEN UPPER(priority_raw) IN ('H','HIGH','P1','1') THEN 'HIGH'
# MAGIC       WHEN UPPER(priority_raw) IN ('M','MED','MEDIUM','P2','2') THEN 'MEDIUM'
# MAGIC       WHEN UPPER(priority_raw) IN ('L','LOW','P3','3') THEN 'LOW'
# MAGIC       ELSE 'UNKNOWN'
# MAGIC     END AS priority,
# MAGIC
# MAGIC     -- Normalize ingest timestamp too (robust)
# MAGIC     COALESCE(
# MAGIC       try_to_timestamp(_ingest_ts, 'yyyy-MM-dd HH:mm:ss'),
# MAGIC       try_to_timestamp(_ingest_ts, 'yyyy/MM/dd HH:mm:ss'),
# MAGIC       try_to_timestamp(_ingest_ts, 'MM/dd/yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(_ingest_ts, 'MM/dd/yyyy HH:mm'),
# MAGIC       try_to_timestamp(_ingest_ts, 'dd-MM-yyyy HH:mm:ss'),
# MAGIC       try_to_timestamp(_ingest_ts, 'dd-MM-yyyy HH:mm')
# MAGIC     ) AS _ingest_ts
# MAGIC
# MAGIC   FROM bronze_layer.work_orders_raw
# MAGIC )
# MAGIC SELECT
# MAGIC   wo_id,
# MAGIC   location_id,
# MAGIC   created_ts,
# MAGIC   scheduled_ts,
# MAGIC   completed_ts,
# MAGIC   crew_id,
# MAGIC   wo_type,
# MAGIC   weather_related_flag,
# MAGIC   priority,
# MAGIC
# MAGIC   -- SLA: completed within 48 hours of created
# MAGIC   CASE
# MAGIC     WHEN completed_ts IS NOT NULL AND created_ts IS NOT NULL
# MAGIC      AND (unix_timestamp(completed_ts) - unix_timestamp(created_ts)) <= 48 * 3600
# MAGIC     THEN TRUE ELSE FALSE
# MAGIC   END AS sla_met_flag,
# MAGIC
# MAGIC   CAST(created_ts AS DATE) AS date,
# MAGIC   _ingest_ts,
# MAGIC
# MAGIC   -- DQ flags
# MAGIC   CASE WHEN created_ts IS NULL THEN TRUE ELSE FALSE END AS dq_bad_created_ts,
# MAGIC   CASE WHEN scheduled_ts IS NULL THEN TRUE ELSE FALSE END AS dq_bad_scheduled_ts,
# MAGIC   CASE WHEN completed_ts IS NULL AND completed_ts_raw IS NOT NULL THEN TRUE ELSE FALSE END AS dq_bad_completed_ts
# MAGIC
# MAGIC FROM cleaned
# MAGIC WHERE TRIM(location_id) RLIKE '^LOC[0-9]{3}$';
# MAGIC

# COMMAND ----------



# COMMAND ----------

# Preview Silver Work Orders
result = spark.sql("SELECT COUNT(*) AS work_orders_cnt FROM silver_layer.work_orders")
display(result)

result2 = spark.sql("SELECT * FROM silver_layer.work_orders LIMIT 15")
display(result2)
