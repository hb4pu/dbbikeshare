# Databricks notebook source
#attempting per article: https://joeho.xyz/blog-posts/how-to-connect-to-azure-synapse-in-azure-databricks/

# Python

# Azure Synapse Connection Configuration
dwDatabase = "dedsqlpoolbikeshare"
dwServer = "synapse-udacitybikeshare2.sql.azuresynapse.net"
dwUser = "sqladminuser"
dwPass = "SqlAU$3r"
#TODO: How to properly security password? check this article: https://learn.microsoft.com/en-us/azure/databricks/scenarios/databricks-extract-load-sql-data-warehouse
dwJdbcPort =  "1433"
dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
sqlDwUrl = f"jdbc:sqlserver://{dwServer}:{dwJdbcPort};database={dwDatabase};user={dwUser};password={dwPass};${dwJdbcExtraOptions}"
  
# Staging Storage Configuration
# Azure Blob Storage
# tempDir = "wasbs://<<container>>@<<your-storage-account-name>>.blob.core.windows.net/<<folder-for-temporary-data>>"

# Azure Data Lake Gen 2
tempDir = "abfss://bikeshareblobc@dlg2account.dfs.core.windows.net/tempspace"

# Azure Synapse Table
tableNameDimRider = "Numbers"

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sqlDwUrl) \
  .option("tempDir", tempDir) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", tableNameDimRider) \
  .load()

display(df)

# COMMAND ----------

#Azure Synapse related settings
dwDatabase = "dedsqlpoolbikeshare"
dwServer = "synapse-udacitybikeshare2.sql.azuresynapse.net"
dwUser = "sqladminuser"
dwPass = "SqlAU$3r"
dwJdbcPort =  "1433"
dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
sqlDwUrl = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass + ";$dwJdbcExtraOptions"
sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

spark.conf.set(
    "spark.sql.parquet.writeLegacyFormat",
    "true")

dfbronzeriders = spark.read.format("delta") \
    .load("/delta/bronze/bronze_riders")
display(dfbronzeriders)
dfbronzeriders.write.format("com.databricks.spark.sqldw").option("url", sqlDwUrlSmall).option("dbtable", "SampleTable").option( "forward_spark_azure_storage_credentials","True").option("tempdir", tempDir).mode("overwrite").save()

# COMMAND ----------


