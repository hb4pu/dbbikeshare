# Databricks notebook source
# MAGIC %md
# MAGIC # Other Notes
# MAGIC 
# MAGIC ## Random notes within
# MAGIC * Workign with Azure Synapse SQL Dedicated Pool (read and write to tables)
# MAGIC   * HAD TO CREATE A MASTER ENCRYPTION KEY IN AZURE SYNAPSE SQL DED POOL:
# MAGIC   * CREATE MASTER KEY ENCRYPTION BY PASSWORD = '23113ASJ#KL45641nl0DBd';
# MAGIC * Did not use Azure Synapse since the rubric stated to 'should write to delta'
# MAGIC * Get history info on delta table
# MAGIC 
# MAGIC ##Tips
# MAGIC * TO use PBI, open partner connect -> Power BI -> download connection file
# MAGIC * Had to turn off ZScaler Internet security, got further still received load on failure

# COMMAND ----------

#How to load table into spark df and some basic data frame commands.  more info can be found here https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/
dfsilverriders = spark.table("silver_riders")
display(dfsilverriders)

dfsilverriders = spark.read.format("delta") \
    .load("/delta/silver/silver_riders")

dfsilverriders.printSchema()
dfsilverriders.count()
dfsilverriders.describe().show()
dfsilverriders.crosstab('rider_id', 'is_member').show()


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY '/delta/silver/silver_riders/'          -- get the full history of the table
# MAGIC 
# MAGIC DESCRIBE HISTORY delta.`/data/events/`
# MAGIC 
# MAGIC DESCRIBE HISTORY '/data/events/' LIMIT 1  -- get the last operation only
# MAGIC 
# MAGIC DESCRIBE HISTORY eventsTable

# COMMAND ----------

#attribution: 
# how to connect to azure synapse in azure databricks  https://joeho.xyz/blog-posts/how-to-connect-to-azure-synapse-in-azure-databricks/

#Read table from Azure Synapse SQL Dedicated Pool
tempDir = "abfss://dlg2filesys@dlg2account.dfs.core.windows.net/tempspace"
#attempt to read table THIS WORKED do not change!!!

#Azure Synapse related settings
dwDatabase = "dedsqlpoolbikeshare"
dwServer = "synapse-udacitybikeshare2.sql.azuresynapse.net"
dwUser = "sqladminuser"
dwPass = "SqlAU$3r"
dwJdbcPort =  "1433"
dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
sqlDwUrl = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass + ";$dwJdbcExtraOptions"
sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass
# Azure Data Lake Gen 2
tempDir = "abfss://dlg2filesys@dlg2account.dfs.core.windows.net/tempspace"

spark.conf.set(
    "spark.sql.parquet.writeLegacyFormat",
    "true")

spark.conf.set(
  "fs.azure.account.key.dlg2account.dfs.core.windows.net",
  "9QX9fJIKdmf50MuxbSAoe/GdOiCLN3kLuYIOUFRT29SOulOK3WfPOByKz4Y61Vc/RtQ0k7bSR+Jm+AStltjmFA==")

# Get some data from an Azure Synapse table.
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sqlDwUrlSmall) \
  .option("tempDir", tempDir) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "Numbers") \
  .load()

display(df)

# COMMAND ----------

#Read table from Azure Synapse SQL Dedicated Pool
#Write to table!  this worked !!

spark.conf.set(
  "fs.azure.account.key.dlg2account.dfs.core.windows.net",
  "9QX9fJIKdmf50MuxbSAoe/GdOiCLN3kLuYIOUFRT29SOulOK3WfPOByKz4Y61Vc/RtQ0k7bSR+Jm+AStltjmFA==")

#Azure Synapse related settings
dwDatabase = "dedsqlpoolbikeshare"
dwServer = "synapse-udacitybikeshare2.sql.azuresynapse.net"
dwUser = "sqladminuser"
dwPass = "SqlAU$3r"
dwJdbcPort =  "1433"
dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
sqlDwUrl = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass + ";$dwJdbcExtraOptions"
sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass
# Azure Data Lake Gen 2
tempDir = "abfss://dlg2filesys@dlg2account.dfs.core.windows.net/tempspace"

spark.conf.set(
    "spark.sql.parquet.writeLegacyFormat",
    "true")

dfbronzeriders = spark.read.format("delta") \
    .load("/delta/bronze/bronze_riders")
display(dfbronzeriders)
dfbronzeriders.write.format("com.databricks.spark.sqldw").option("url", sqlDwUrlSmall).option("dbtable", "SampleTable").option( "forward_spark_azure_storage_credentials","True").option("tempdir", tempDir).mode("append").save()
