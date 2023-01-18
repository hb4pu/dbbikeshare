# Databricks notebook source
#TIPS AND TRICKS, Notes, other
#> dbutils.fs.rm("/delta/bronze_riders",recurse=True)

#used this command to delete 'spark table'
#> spark.sql("drop table if exists silver_riders2")

#TODO: learn how to handle OLTP systems that have updates to rows.  Attempt to use hash of columns that are the key for the row
#      is this hash value used for knowing when to update particular row in delta table, or am i using it the wrong way?
#TODO: desire to learn how to handle deleted rows from OLTP systems

# COMMAND ----------

dbutils.fs.rm("/delta/bronze",recurse=True)

# COMMAND ----------

spark.sql("drop table if exists silver_riders")
spark.sql("drop table if exists silver_payment")
spark.sql("drop table if exists silver_station")
spark.sql("drop table if exists silver_trip")

# COMMAND ----------

#attribution: 
# how to connect to azure synapse in azure databricks  https://joeho.xyz/blog-posts/how-to-connect-to-azure-synapse-in-azure-databricks/

