# Databricks notebook source
# MAGIC %md
# MAGIC # Other Notes
# MAGIC 
# MAGIC ## Random notes within
# MAGIC * Workign with Azyre Synapse SQL Dedicated Pool (read and write to tables)
# MAGIC   * HAD TO CREATE A MASTER ENCRYPTION KEY IN AZURE SYNAPSE SQL DED POOL:
# MAGIC   * CREATE MASTER KEY ENCRYPTION BY PASSWORD = '23113ASJ#KL45641nl0DBd';
# MAGIC * Did not use Azure Synapse since the rubric stated to 'should write to delta'

# COMMAND ----------

#How to load table into spark df
dfgoldriders = spark.table("silver_riders")
display(dfgoldriders)

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
