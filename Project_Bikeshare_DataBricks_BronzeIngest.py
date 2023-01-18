# Databricks notebook source
# MAGIC %md
# MAGIC # Extract (Bronze)
# MAGIC Extract information from CSV files stored in Databricks and write it to the Delta file system.

# COMMAND ----------

# load csv
file_location = "/FileStore/bikeshare/riders.csv"
file_type = "csv"

infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#set columns
df =  df.selectExpr(
    '_c0 AS rider_id',
    '_c1 AS first',
    '_c2 AS last',
    '_c3 AS address',
    '_c4 AS birthday',
    '_c5 AS account_start_date',
    '_c6 AS account_end_date',
    '_c7 AS is_member',
)

#write to bronze delta
df.write.format("delta").mode("overwrite").save("/delta/bronze/bronze_riders")

#TODO: is this named properly?  shows up without extension within delta\bronze folder, just looks like a folder with parquet files within
#TODO: why more than one file in bronze_riders when mode = overwrite?  learn how parquet / delta data is stored

# COMMAND ----------

# load csv
file_location = "/FileStore/bikeshare/stations.csv"
file_type = "csv"

infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#set columns
df =  df.selectExpr(
    '_c0 AS station_id',
    '_c1 AS name',
    '_c2 AS latitude',
    '_c3 AS longitude',
)

#write to bronze delta
df.write.format("delta").mode("overwrite").save("/delta/bronze/bronze_stations")


# COMMAND ----------

# load csv
file_location = "/FileStore/bikeshare/payments.csv"
file_type = "csv"

infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#set columns
df =  df.selectExpr(
    '_c0 AS payment_id',
    '_c1 AS date',
    '_c2 AS amount',
    '_c3 AS rider_id',
)

#write to bronze delta
df.write.format("delta").mode("overwrite").save("/delta/bronze/bronze_payments")

# COMMAND ----------

# load csv
file_location = "/FileStore/bikeshare/trips.csv"
file_type = "csv"

infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

#set columns
df =  df.selectExpr(
    '_c0 AS trip_id',
    '_c1 AS rideable_type',
    '_c2 AS start_at',
    '_c3 AS ended_at',
    '_c4 AS start_station_id',
    '_c5 AS end_station_id',
    '_c6 AS rider_id',
)

#write to bronze delta
df.write.format("delta").mode("overwrite").save("/delta/bronze/bronze_trips")
