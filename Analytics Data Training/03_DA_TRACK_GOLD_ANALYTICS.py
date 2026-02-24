# Databricks notebook source
# MAGIC %md
# MAGIC #### This notebook is the backbone for your Databricks SQL dashboards and all your guided analytics labs.
# MAGIC
# MAGIC - Reads Silver
# MAGIC - Builds daily aggregates
# MAGIC - Creates fact tables + views

# COMMAND ----------

# MAGIC %md
# MAGIC Purpose: Gold Facts + Dimensions + Views
# MAGIC
# MAGIC Includes:
# MAGIC
# MAGIC | Item |
# MAGIC |------|
# MAGIC | ✔ gold_layer.dim_date |
# MAGIC | ✔ fact_weather_daily |
# MAGIC | ✔ fact_outages_daily |
# MAGIC | ✔ fact_work_orders_daily |
# MAGIC | ✔ v_operational_weather_view |
# MAGIC | ✔ Lab prompts: |
# MAGIC | “Find stormiest days” |
# MAGIC | “Which locations have highest outage sensitivity?” |
# MAGIC | “Compute SLA trends by month” |

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 — Create gold_layer.dim_location from the Python list
# MAGIC

# COMMAND ----------



# COMMAND ----------

# GOLD: dim_location (from locations.py list)

from pyspark.sql import Row

# If locations.py is in the same folder, you can %run it:
# %run "./locations"

# Otherwise, import it (depends how your repo/notebook path is set up)
from locations import Location  # <-- adjust if your module name/path differs

locs = Location(random_100=True).location_list  # or False if you want all

rows = []
for loc in locs:
    # Support both styles:
    # (A) if loc is namedtuple("location", ["Name","Latitude","Longitude"])
    # (B) if loc is an object/record with Id/Name/Latitude/Longitude
    loc_id = getattr(loc, "Id", None) or getattr(loc, "id", None)
    name   = getattr(loc, "Name", None) or getattr(loc, "name", None)
    lat    = getattr(loc, "Latitude", None) or getattr(loc, "latitude", None)
    lon    = getattr(loc, "Longitude", None) or getattr(loc, "longitude", None)

    # If no Id exists, generate one
    if loc_id is None:
        # fallback deterministic-ish ID from name order
        # (better is updating locations.py to always provide Id)
        loc_id = name

    rows.append(Row(location_id=str(loc_id), location_name=str(name), latitude=float(lat), longitude=float(lon)))

df = spark.createDataFrame(rows)

df.write.mode("overwrite").saveAsTable("gold_layer.dim_location")

display(df.limit(10))
print(f"gold_layer.dim_location created with {df.count()} locations")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 — Gold Daily Schema (Dims + Facts) SQL
# MAGIC #####Dynamically detect the date boundaries from Silver tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COMMAND ----------
# MAGIC -- GOLD: Date Dimension 
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold_layer.dim_date AS
# MAGIC WITH bounds AS (
# MAGIC   SELECT
# MAGIC     LEAST(
# MAGIC       (SELECT min(date) FROM silver_layer.weather_hourly),
# MAGIC       (SELECT min(date) FROM silver_layer.outages),
# MAGIC       (SELECT min(date) FROM silver_layer.work_orders)
# MAGIC     ) AS min_date,
# MAGIC     GREATEST(
# MAGIC       (SELECT max(date) FROM silver_layer.weather_hourly),
# MAGIC       (SELECT max(date) FROM silver_layer.outages),
# MAGIC       (SELECT max(date) FROM silver_layer.work_orders)
# MAGIC     ) AS max_date
# MAGIC ),
# MAGIC dates AS (
# MAGIC   SELECT explode(sequence(min_date, max_date, interval 1 day)) AS date
# MAGIC   FROM bounds
# MAGIC )
# MAGIC SELECT
# MAGIC   date,
# MAGIC   year(date)                  AS year,
# MAGIC   month(date)                 AS month,
# MAGIC   date_format(date,'MMM')     AS month_name,
# MAGIC   day(date)                   AS day,
# MAGIC   date_format(date,'E')       AS day_of_week,
# MAGIC   CASE WHEN date_format(date,'E') IN ('Sat','Sun') THEN TRUE ELSE FALSE END AS is_weekend
# MAGIC FROM dates;
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# Preview Gold Dim_Date
result = spark.sql("SELECT COUNT(*) AS dim_cnt FROM gold_layer.dim_date")
display(result)

result2 = spark.sql("SELECT * FROM gold_layer.dim_date LIMIT 15")
display(result2)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COMMAND ----------
# MAGIC -- GOLD: Fact Weather Daily
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold_layer.fact_weather_daily AS
# MAGIC SELECT
# MAGIC   location_id,
# MAGIC   CAST(timestamp AS DATE) AS date,
# MAGIC
# MAGIC   AVG(temperature_2m)          AS avg_temp_c,
# MAGIC   MAX(temperature_2m)          AS max_temp_c,
# MAGIC   MIN(temperature_2m)          AS min_temp_c,
# MAGIC
# MAGIC   AVG(apparent_temperature)    AS avg_apparent_temp_c,
# MAGIC   AVG(relative_humidity_2m)    AS avg_humidity_pct,
# MAGIC   AVG(wind_speed_10m)          AS avg_wind_speed,
# MAGIC
# MAGIC   SUM(precipitation)           AS total_precipitation,
# MAGIC   MAX(precipitation)           AS max_hourly_precipitation
# MAGIC
# MAGIC FROM silver_layer.weather_hourly
# MAGIC GROUP BY location_id, CAST(timestamp AS DATE);

# COMMAND ----------



# COMMAND ----------

# Preview Gold Fact Weather Daily
result = spark.sql("SELECT COUNT(*) AS weather_daily_cnt FROM gold_layer.fact_weather_daily")
display(result)

result2 = spark.sql("SELECT * FROM gold_layer.fact_weather_daily LIMIT 15")
display(result2)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COMMAND ----------
# MAGIC -- GOLD: Fact Outages Daily
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold_layer.fact_outages_daily AS
# MAGIC SELECT
# MAGIC   location_id,
# MAGIC   date,
# MAGIC
# MAGIC   COUNT(*)                         AS outage_count,
# MAGIC   SUM(customers_affected)          AS customers_affected_total,
# MAGIC   AVG(duration_minutes)            AS avg_duration_minutes,
# MAGIC   MAX(duration_minutes)            AS max_duration_minutes,
# MAGIC
# MAGIC   SUM(CASE WHEN weather_severity = 'HIGH' THEN 1 ELSE 0 END)    AS high_severity_outages,
# MAGIC   SUM(CASE WHEN weather_severity = 'MEDIUM' THEN 1 ELSE 0 END)  AS medium_severity_outages,
# MAGIC   SUM(CASE WHEN weather_severity = 'LOW' THEN 1 ELSE 0 END)     AS low_severity_outages,
# MAGIC   SUM(CASE WHEN weather_severity = 'UNKNOWN' THEN 1 ELSE 0 END) AS unknown_severity_outages
# MAGIC
# MAGIC FROM silver_layer.outages
# MAGIC GROUP BY location_id, date;

# COMMAND ----------



# COMMAND ----------

# Preview Gold Fact Outages Daily
result = spark.sql("SELECT COUNT(*) AS outages_daily_cnt FROM gold_layer.fact_outages_daily")
display(result)

result2 = spark.sql("SELECT * FROM gold_layer.fact_outages_daily LIMIT 15")
display(result2)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COMMAND ----------
# MAGIC -- GOLD: Fact Work Orders Daily
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold_layer.fact_work_orders_daily AS
# MAGIC SELECT
# MAGIC   location_id,
# MAGIC   date,
# MAGIC
# MAGIC   COUNT(*) AS work_order_count,
# MAGIC
# MAGIC   SUM(CASE WHEN priority = 'HIGH' THEN 1 ELSE 0 END)    AS high_priority_count,
# MAGIC   SUM(CASE WHEN priority = 'MEDIUM' THEN 1 ELSE 0 END)  AS medium_priority_count,
# MAGIC   SUM(CASE WHEN priority = 'LOW' THEN 1 ELSE 0 END)     AS low_priority_count,
# MAGIC   SUM(CASE WHEN priority = 'UNKNOWN' THEN 1 ELSE 0 END) AS unknown_priority_count,
# MAGIC
# MAGIC   AVG(CASE WHEN sla_met_flag THEN 1 ELSE 0 END) AS pct_sla_met,
# MAGIC
# MAGIC   SUM(CASE WHEN weather_related_flag IS TRUE THEN 1 ELSE 0 END)  AS weather_related_true,
# MAGIC   SUM(CASE WHEN weather_related_flag IS FALSE THEN 1 ELSE 0 END) AS weather_related_false,
# MAGIC   SUM(CASE WHEN weather_related_flag IS NULL THEN 1 ELSE 0 END)  AS weather_related_unknown
# MAGIC
# MAGIC FROM silver_layer.work_orders
# MAGIC GROUP BY location_id, date;

# COMMAND ----------



# COMMAND ----------

# Preview Gold Fact Work Orders Daily
result = spark.sql("SELECT COUNT(*) AS work_orders_daily_cnt FROM gold_layer.fact_work_orders_daily")
display(result)

result2 = spark.sql("SELECT * FROM gold_layer.fact_work_orders_daily LIMIT 15")
display(result2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Dashboard View

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COMMAND ----------
# MAGIC -- GOLD: Combined Operational View for Dashboards
# MAGIC
# MAGIC CREATE OR REPLACE VIEW gold_layer.v_operational_weather_daily AS
# MAGIC SELECT
# MAGIC   d.date,
# MAGIC   l.location_id,
# MAGIC   l.location_name,
# MAGIC
# MAGIC   w.avg_temp_c,
# MAGIC   w.total_precipitation,
# MAGIC   w.avg_wind_speed,
# MAGIC
# MAGIC   o.outage_count,
# MAGIC   o.customers_affected_total,
# MAGIC   o.avg_duration_minutes,
# MAGIC
# MAGIC   wo.work_order_count,
# MAGIC   wo.high_priority_count,
# MAGIC   wo.pct_sla_met,
# MAGIC   wo.weather_related_true,
# MAGIC   wo.weather_related_unknown
# MAGIC
# MAGIC FROM gold_layer.dim_date d
# MAGIC CROSS JOIN gold_layer.dim_location l
# MAGIC LEFT JOIN gold_layer.fact_weather_daily w
# MAGIC   ON w.location_id = l.location_id AND w.date = d.date
# MAGIC LEFT JOIN gold_layer.fact_outages_daily o
# MAGIC   ON o.location_id = l.location_id AND o.date = d.date
# MAGIC LEFT JOIN gold_layer.fact_work_orders_daily wo
# MAGIC   ON wo.location_id = l.location_id AND wo.date = d.date;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# Preview Gold Combined Operational View

display(
    spark.sql(
        "SELECT * FROM gold_layer.v_operational_weather_daily LIMIT 10"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Note:
# MAGIC > A more advanced hourly weather join is available in
# MAGIC > `04_DA_TRACK_BONUS_HOURLY_WEATHER_JOIN`.
# MAGIC > This Gold model intentionally uses daily weather for BI stability.
# MAGIC

# COMMAND ----------


