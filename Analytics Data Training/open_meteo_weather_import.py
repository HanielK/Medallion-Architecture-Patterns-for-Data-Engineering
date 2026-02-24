# Databricks notebook source
from api_handler import api_handler
from locations import Location
import json
from pandas import json_normalize
from functools import reduce
from pyspark.sql import functions as F


# COMMAND ----------

# Use a subset for speed in training (e.g., 50 or 100 locations)
locations = Location(random_100=True).location_list

print(locations)

# COMMAND ----------

api_base = (
    "https://archive-api.open-meteo.com/v1/archive"
    "?start_date=2020-01-01"
    "&end_date=2020-01-31"
    "&hourly=temperature_2m,surface_pressure,wind_speed_10m,"
    "wind_direction_10m,wind_gusts_10m,relative_humidity_2m,"
    "apparent_temperature,precipitation,rain,pressure_msl"
)

# COMMAND ----------

# Instantiate the handler correctly
handler = api_handler()

spark_dfs = []

for loc in locations:
    print(
        f"Fetching weather for {loc.Id} - {loc.Name} ({loc.Latitude}, {loc.Longitude})"
    )

    api_url = (
        f"{api_base}"
        f"&latitude={loc.Latitude}"
        f"&longitude={loc.Longitude}"
    )

    handler.make_api_call(api_url=api_url)
    response = handler.get_response()  # Get the response object

    json_value = response.json()  # Use the response object

    # Flatten JSON using pandas
    pdf = json_normalize(json_value)

    # Convert to Spark DataFrame
    sdf = spark.createDataFrame(pdf)

    # Attach location metadata
    sdf = (
        sdf.withColumn("location_id", F.lit(loc.Id))
           .withColumn("location_name", F.lit(loc.Name))
           .withColumn("latitude", F.lit(loc.Latitude))
           .withColumn("longitude", F.lit(loc.Longitude))
    )

    spark_dfs.append(sdf)

# COMMAND ----------

# Union all per-location DataFrames
if spark_dfs:
    df_weather_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), spark_dfs)
else:
    df_weather_all = spark.createDataFrame([], schema="location_id string")

# COMMAND ----------

# Write to Bronze
df_weather_all.write.mode("overwrite").saveAsTable("bronze_layer.weather")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM bronze_layer.weather
