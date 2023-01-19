-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC #Transform Data - Load Dimensions
-- MAGIC 
-- MAGIC Manufacture Keys using ROW_NUMBER function
-- MAGIC 
-- MAGIC Can pull from delta lake or from data tables (Since I could not decide best plact for the data):
-- MAGIC 
-- MAGIC delta lake: select * from delta.``/delta/silver/silver_riders`` limit 10;
-- MAGIC 
-- MAGIC data table select * from silver_riders
-- MAGIC 
-- MAGIC Attribution: This documentation was helpful for learning functions available within databricks/sql: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/datediff3
-- MAGIC 
-- MAGIC Areas for improvement: 
-- MAGIC 1. Best way to generate Keys in Dimensional Tables?
-- MAGIC 2. Update queries to Merge statements (Upserts). Can't do that without better way to create Keys for Dims. 

-- COMMAND ----------

--POPULATE DATE DIM
TRUNCATE TABLE Dim.Dates;

INSERT INTO Dim.Dates(DateKey
  , CalendarDate
  , CalendarYear
  , CalendarMonth
  , MonthOfYear
  , CalendarDay
  , DayOfWeek
  , DayOfMonth
  , DayOfYear
  , QuarterOfYear)
SELECT DateKey
  , CalendarDate
  , CalendarYear
  , CalendarMonth
  , MonthOfYear
  , CalendarDay
  , DayOfWeek
  , DayOfMonth
  , DayOfYear
  , QuarterOfYear
FROM delta.`/delta/gold/gold_dates`;

-- COMMAND ----------

TRUNCATE TABLE Dim.Rider;

INSERT INTO Dim.Rider(
  RiderKey
  ,RemoteSystemRiderID
  ,FirstName
  ,LastName
  ,BirthDateKey
  ,AccountStartDateKey
  ,AccountEndDateKey
  ,LivingAgeYears
  ,AccountStartAgeYears
  ,IsMember
  ,IsActiveAccount
  )
SELECT 
  ROW_NUMBER() over (order by rider_id) RiderKey
  , r.rider_id RemoteSystemRiderID
  , r.first FirstName
  , r.last LastName
  , date_format(r.birthday, 'yyyyMMdd') BirthDateKey
  , date_format(r.account_start_date, 'yyyyMMdd') AccountStartDateKey
  , date_format(r.account_end_date, 'yyyyMMdd') AccountEndDateKey
  , floor(datediff(now(), r.birthday)/365.0, 0) LivingAgeYears
  , floor(datediff(r.account_start_date, r.birthday)/365.0, 0) AccountStartAgeYears
  , CASE WHEN r.is_member = 'True' THEN 'Y' ELSE 'N' END IsMember
  , CASE WHEN r.account_end_date IS NULL THEN 'Y' ELSE 'N' END IsActiveAccount
FROM silver_riders r;


-- COMMAND ----------

TRUNCATE TABLE Dim.Station;

INSERT INTO Dim.Station(
  StationKey
  , RemoteSystemStationID
  , StationName
  , Latitude
  , Longitude
  )
SELECT 
  ROW_NUMBER() OVER (ORDER BY s.station_id) StationKey
  , s.station_id RemoteSystemStationID
  , s.name StationName
  , s.latitude   Latitude
  , s.longitude   Longitude
FROM silver_stations s;

