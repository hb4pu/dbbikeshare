# Databricks notebook source
# MAGIC %md
# MAGIC ## 03 - Silver Load
# MAGIC Load from Bronze delta to Silver (delta or spark table?) 
# MAGIC 
# MAGIC Doing both delta and spark table to start

# COMMAND ----------

#Establish silver storage for riders
from pyspark.sql.functions import sha2

dfbronzeriders = spark.read.format("delta") \
    .load("/delta/bronze/bronze_riders")

dfbronzeriders = dfbronzeriders.withColumn("row_sha2", sha2("rider_id", 256))

#append data so that delta is updated with changes
#save in delta format
dfbronzeriders.write.format("delta").mode("overwrite") \
    .save("/delta/silver/silver_riders")

#TODO: Should i be writing silver using saveastable so it shows up in Data > Database Tables or continue to store in DBFS?
#TODO: what is behavior of overwrite vs append.  Can those be used for merging data (upsert & delete)?

#save in spark table
dfbronzeriders.write.format("delta").mode("overwrite") \
    .saveAsTable("silver_riders")

# COMMAND ----------

from pyspark.sql.functions import sha2

dfbronze = spark.read.format("delta") \
    .load("/delta/bronze/bronze_stations")

dfbronze = dfbronze.withColumn("row_sha2", sha2("station_id", 256))

#save in delta format
dfbronze.write.format("delta").mode("overwrite") \
    .save("/delta/silver/silver_stations")

#save in spark table
dfbronze.write.format("delta").mode("overwrite") \
    .saveAsTable("silver_stations")

# COMMAND ----------

from pyspark.sql.functions import sha2

dfbronze = spark.read.format("delta") \
    .load("/delta/bronze/bronze_payments")

dfbronze = dfbronze.withColumn("row_sha2", sha2("payment_id", 256))

#save in delta format
dfbronze.write.format("delta").mode("overwrite") \
    .save("/delta/silver/silver_payments")

#save in spark table
dfbronze.write.format("delta").mode("overwrite") \
    .saveAsTable("silver_payments")

# COMMAND ----------

from pyspark.sql.functions import sha2

dfbronze = spark.read.format("delta") \
    .load("/delta/bronze/bronze_trips")

dfbronze = dfbronze.withColumn("row_sha2", sha2("trip_id", 256))

#save in delta format
dfbronze.write.format("delta").mode("overwrite") \
    .save("/delta/silver/silver_trips")

#save in spark table
dfbronze.write.format("delta").mode("overwrite") \
    .saveAsTable("silver_trips")
