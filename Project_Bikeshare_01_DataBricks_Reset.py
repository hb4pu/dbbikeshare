# Databricks notebook source
#TIPS AND TRICKS, Notes, other
#> dbutils.fs.rm("/delta/bronze_riders",recurse=True)

#used this command to delete 'spark table'
#> spark.sql("drop table if exists silver_riders2")

#TODO: learn how to handle OLTP systems that have updates to rows.  Attempt to use hash of columns that are the key for the row
#      is this hash value used for knowing when to update particular row in delta table, or am i using it the wrong way?
#TODO: desire to learn how to handle deleted rows from OLTP systems
#TODO: for databricks / sql to delta lake, nvarchar not supported.  what to use for double byte chars in delta lake?
#TODO: for databricks tables, how to create primary key identity (1,1) for dimensional tables
#TODO: when defining tables for databrick sql tables, NULL not an option but NOT NULL is.  why?

#need attribution for #https://www.bluegranite.com/blog/generate-a-calendar-dimension-in-spark

# COMMAND ----------

dbutils.fs.rm("/delta/bronze",recurse=True)
dbutils.fs.rm("/delta/gold",recurse=True)
dbutils.fs.rm("/delta/silver",recurse=True)
dbutils.fs.rm("/Filestore/bikeshare",recurse=True)

# COMMAND ----------

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
