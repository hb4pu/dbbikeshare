# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 - Use this notebook to clean all items and start over

# COMMAND ----------

#delete delta files
dbutils.fs.rm("/delta/bronze",recurse=True)
dbutils.fs.rm("/delta/gold",recurse=True)
dbutils.fs.rm("/delta/silver",recurse=True)
#dbutils.fs.rm("/Filestore/bikeshare",recurse=True)

# COMMAND ----------

#drop spark tables

spark.sql("drop table if exists silver_riders")
spark.sql("drop table if exists silver_payments")
spark.sql("drop table if exists silver_stations")
spark.sql("drop table if exists silver_trips")
spark.sql("drop table if exists gold_dates")

spark.sql("DROP TABLE IF EXISTS Fact.Trip")
spark.sql("DROP TABLE IF EXISTS Dim.Dates")
spark.sql("DROP TABLE IF EXISTS Fact.Payment")
spark.sql("DROP TABLE IF EXISTS Dim.Station")
spark.sql("DROP TABLE IF EXISTS Dim.Rider")


# COMMAND ----------

spark.sql("DROP SCHEMA IF EXISTS Dim")
spark.sql("DROP SCHEMA IF EXISTS Fact")
