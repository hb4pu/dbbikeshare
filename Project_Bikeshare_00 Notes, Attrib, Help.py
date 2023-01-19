# Databricks notebook source
# DBTITLE 1,NOTES
# MAGIC %md
# MAGIC ## Comments about project
# MAGIC * I may have misread the requirements within the rubric. I wasn't sure if I should store the end results with Databricks/SQL or if I should be writing to Azure Synapse/SQL Dedicated.  Based on the requirements, I read that as store in Databricks/SQL Delta.
# MAGIC * I liked using jupyter notebook with the visualization tools to answer the questions for the project. If I was to store the star schema results in Azure Synapse, not sure if I could do the same (how to read data from Databricks SQL code from Azure SQL)
# MAGIC 
# MAGIC ## Attribution
# MAGIC * Used this to learn about databricks to synapse read write of tables: https://joeho.xyz/blog-posts/how-to-connect-to-azure-synapse-in-azure-databricks/
# MAGIC * Used this to generate calendar dimension within spark #https://www.bluegranite.com/blog/generate-a-calendar-dimension-in-spark
# MAGIC 
# MAGIC ## Areas for Improvement and Questions
# MAGIC * Best way to generate Keys in Dimensional Tables?
# MAGIC * Update queries to Merge statements (Upserts). Can't do that without better way to create Keys for Dims.
# MAGIC * Learn how to handle OLTP systems that have updates to rows.  
# MAGIC   * Attempt to use hash of columns that are the key for the row within silver rated delta files
# MAGIC   * Able to upsert into silver delta
# MAGIC   * Able to delete from silver delta?
# MAGIC * For databricks tables, how to create primary key identity (1,1) for dimensional tables.  Once that is figured out, how to handle Upsert/Delete merge statements?
# MAGIC * Within notebook 'Project_Bikeshare_03_DataBricks_SilverLoad' I am reusing dfbronze reference.  anything else i should do, cleanup mem usage, garbage cleanup, dispose, other?
# MAGIC * Could not create nvarchar within databricks / sql table.  How to support double byte characters
# MAGIC * Could not set column as nullable within databricks / sql table.  Are columns nullable by default. Was able to set NOT NULL constraint.
