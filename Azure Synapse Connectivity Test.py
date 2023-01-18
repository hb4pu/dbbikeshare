# Databricks notebook source
#attempting per article: https://joeho.xyz/blog-posts/how-to-connect-to-azure-synapse-in-azure-databricks/

# Python

# Azure Synapse Connection Configuration
dwDatabase = "dedsqlpoolbikeshare"
dwServer = "synapse-udacitybikeshare2.sql.azuresynapse.net"
dwUser = "sqladminuser"
dwPass = "SqlAU$3r"
#TODO: How to properly security password?
dwJdbcPort =  "1433"
dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
sqlDwUrl = f"jdbc:sqlserver://{dwServer}:{dwJdbcPort};database={dwDatabase};user={dwUser};password={dwPass};${dwJdbcExtraOptions}"
  
# Staging Storage Configuration
# Azure Blob Storage
# tempDir = "wasbs://<<container>>@<<your-storage-account-name>>.blob.core.windows.net/<<folder-for-temporary-data>>"

# Azure Data Lake Gen 2
tempDir = "abfss://<<container >>@<<your-storage-account-name>>.dfs.core.windows.net/<<folder-for-temporary-data>>"

# Azure Synapse Table
tableNameDimRider = "Dim.Rider"

df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sqlDwUrl) \
  .option("tempDir", tempDir) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", tableNameDimRider) \
  .load()

display(df)
