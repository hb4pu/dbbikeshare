# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## 06 Transform Data - Load Dimensions
# MAGIC 
# MAGIC Manufacture Keys using ROW_NUMBER function
# MAGIC 
# MAGIC Can pull from delta lake or from data tables (Since I could not decide best plact for the data):
# MAGIC 
# MAGIC delta lake: select * from delta.``/delta/silver/silver_riders`` limit 10;
# MAGIC 
# MAGIC data table select * from silver_riders
# MAGIC 
# MAGIC Attribution: This documentation was helpful for learning functions available within databricks/sql: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/datediff3
# MAGIC 
# MAGIC Areas for improvement: 
# MAGIC 1. Best way to generate Keys in Dimensional Tables?
# MAGIC 2. Update queries to Merge statements (Upserts). Can't do that without better way to create Keys for Dims. 

# COMMAND ----------

# MAGIC %sql
# MAGIC --POPULATE DATE DIM
# MAGIC TRUNCATE TABLE Dim.Dates;
# MAGIC 
# MAGIC INSERT INTO Dim.Dates(DateKey
# MAGIC   , CalendarDate
# MAGIC   , CalendarYear
# MAGIC   , CalendarMonth
# MAGIC   , MonthOfYear
# MAGIC   , CalendarDay
# MAGIC   , DayOfWeek
# MAGIC   , DayOfMonth
# MAGIC   , DayOfYear
# MAGIC   , QuarterOfYear)
# MAGIC SELECT DateKey
# MAGIC   , CalendarDate
# MAGIC   , CalendarYear
# MAGIC   , CalendarMonth
# MAGIC   , MonthOfYear
# MAGIC   , CalendarDay
# MAGIC   , DayOfWeek
# MAGIC   , DayOfMonth
# MAGIC   , DayOfYear
# MAGIC   , QuarterOfYear
# MAGIC FROM delta.`/delta/silver/silver_dates`;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE Dim.Rider;
# MAGIC 
# MAGIC INSERT INTO Dim.Rider(
# MAGIC   RemoteSystemRiderID
# MAGIC   ,FirstName
# MAGIC   ,LastName
# MAGIC   ,BirthDateKey
# MAGIC   ,AccountStartDateKey
# MAGIC   ,AccountEndDateKey
# MAGIC   ,LivingAgeYears
# MAGIC   ,AccountStartAgeYears
# MAGIC   ,IsMember
# MAGIC   ,IsActiveAccount
# MAGIC   )
# MAGIC SELECT 
# MAGIC   r.rider_id RemoteSystemRiderID
# MAGIC   , r.first FirstName
# MAGIC   , r.last LastName
# MAGIC   , date_format(r.birthday, 'yyyyMMdd') BirthDateKey
# MAGIC   , date_format(r.account_start_date, 'yyyyMMdd') AccountStartDateKey
# MAGIC   , date_format(r.account_end_date, 'yyyyMMdd') AccountEndDateKey
# MAGIC   , floor(datediff(r.account_start_date, r.birthday)/365.0, 0) LivingAgeYears
# MAGIC   , floor(datediff(r.account_start_date, r.birthday)/365.0, 0) AccountStartAgeYears
# MAGIC   , CASE WHEN r.is_member = 'True' THEN 'Y' ELSE 'N' END IsMember
# MAGIC   , CASE WHEN r.account_end_date IS NULL THEN 'Y' ELSE 'N' END IsActiveAccount
# MAGIC FROM silver_riders r;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE Dim.Station;
# MAGIC 
# MAGIC INSERT INTO Dim.Station(
# MAGIC   RemoteSystemStationID
# MAGIC   , StationName
# MAGIC   , Latitude
# MAGIC   , Longitude
# MAGIC   )
# MAGIC SELECT 
# MAGIC   s.station_id RemoteSystemStationID
# MAGIC   , s.name StationName
# MAGIC   , s.latitude   Latitude
# MAGIC   , s.longitude   Longitude
# MAGIC FROM silver_stations s;
