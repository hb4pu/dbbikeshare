-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Business Outcomes Designing For
-- MAGIC 
-- MAGIC ## Analyze how much time is spent per ride
-- MAGIC * Based on date and time factors such as day of week and time of day
-- MAGIC * Based on which station is the starting and / or ending station (Need hour column or time on fact)
-- MAGIC * Based on age of the rider at time of the ride (Done)
-- MAGIC * Based on whether the rider is a member or a casual rider (Done)
-- MAGIC 
-- MAGIC ## Analyze how much money is spent
-- MAGIC * Per month, quarter, year (Done)
-- MAGIC * Per member, based on the age of the rider at account start
-- MAGIC 
-- MAGIC ##EXTRA CREDIT - Analyze how much money is spent per member
-- MAGIC * Based on how many rides the rider averages per month
-- MAGIC * Based on how many minutes the rider spends on a bike per month

-- COMMAND ----------

select dd.CalendarDay
  , dd.DayOfWeek
  , SUM(ft.ElapsedTimeSeconds) TotalElapsedTimeSeconds
  , COUNT(ft.TripKey) TripCount
  , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
from fact.trip ft
inner join dim.dates dd
  on ft.TripStartDateKey = dd.DateKey
group by dd.CalendarDay, dd.DayOfWeek
order by dd.DayOfWeek;

-- COMMAND ----------

select ft.TripStartHourOfDay
  , SUM(ft.ElapsedTimeSeconds) TotalElapsedTimeSeconds
  , COUNT(ft.TripKey) TripCount
  , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
from fact.trip ft
group by ft.TripStartHourOfDay
order by ft.TripStartHourOfDay;

-- COMMAND ----------

select 
  dsstart.StationName StartStation
  , dsend.StationName EndStation
  , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
  , ROW_NUMBER() OVER (ORDER BY SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) DESC) Ranking
from fact.trip ft
inner join dim.station dsstart
  on ft.StartStationKey = dsstart.StationKey
inner join dim.station dsend
  on ft.EndStationKey = dsend.StationKey
group by dsstart.StationName
  , dsend.StationName
order by 3 desc

-- COMMAND ----------

select 
  dsstart.StationName StartStation
  , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
  , ROW_NUMBER() OVER (ORDER BY SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) DESC) Ranking
from fact.trip ft
inner join dim.station dsstart
  on ft.StartStationKey = dsstart.StationKey
inner join dim.station dsend
  on ft.EndStationKey = dsend.StationKey
group by dsstart.StationName
order by 3 asc
limit 20;


-- COMMAND ----------

select 
  dsstart.StationName StartStation
  , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
  , ROW_NUMBER() OVER (ORDER BY SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) DESC) Ranking
from fact.trip ft
inner join dim.station dsstart
  on ft.StartStationKey = dsstart.StationKey
inner join dim.station dsend
  on ft.EndStationKey = dsend.StationKey
group by dsstart.StationName
order by 3 asc
limit 25;

-- COMMAND ----------

select ft.riderAgeYears
  , SUM(ft.ElapsedTimeSeconds) TotalElapsedTimeSeconds
  , COUNT(ft.TripKey) TripCount
  , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
from fact.trip ft
group by ft.riderAgeYears
order by ft.riderAgeYears;

-- COMMAND ----------

select dr.IsMember
  , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
from fact.trip ft
inner join dim.rider dr
  on ft.RiderKey = dr.RiderKey
group by dr.IsMember
order by dr.IsMember;

-- COMMAND ----------

select 
  dd.CalendarMonth
  , dd.MonthOfYear
  , SUM(fp.Amount) Payments
from fact.payment fp
inner join dim.dates dd
  on fp.PaymentDateKey = dd.DateKey
group by dd.CalendarMonth
  , dd.MonthOfYear
order by dd.MonthOfYear

-- COMMAND ----------

select 
  dd.QuarterOfYear
  , SUM(fp.Amount) Payments
from fact.payment fp
inner join dim.dates dd
  on fp.PaymentDateKey = dd.DateKey
group by dd.QuarterOfYear
order by dd.QuarterOfYear;

-- COMMAND ----------

select 
  dd.CalendarYear
  , SUM(fp.Amount) Payments
from fact.payment fp
inner join dim.dates dd
  on fp.PaymentDateKey = dd.DateKey
group by dd.CalendarYear
order by dd.CalendarYear;

-- COMMAND ----------

select 
  dr.AccountStartAgeYears
  , SUM(fp.Amount) / COUNT(DISTINCT dr.RiderKey) AmountPerMember
from fact.payment fp
inner join dim.rider dr
  on fp.RiderKey = dr.RiderKey
group by dr.AccountStartAgeYears
order by dr.AccountStartAgeYears;


-- COMMAND ----------


