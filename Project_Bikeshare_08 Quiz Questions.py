# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Business Outcomes Designing For
# MAGIC 
# MAGIC ## Analyze how much time is spent per ride
# MAGIC * Based on date and time factors such as day of week and time of day
# MAGIC * Based on which station is the starting and / or ending station (Need hour column or time on fact)
# MAGIC * Based on age of the rider at time of the ride (Done)
# MAGIC * Based on whether the rider is a member or a casual rider (Done)
# MAGIC 
# MAGIC ## Analyze how much money is spent
# MAGIC * Per month, quarter, year (Done)
# MAGIC * Per member, based on the age of the rider at account start
# MAGIC 
# MAGIC ##EXTRA CREDIT - Analyze how much money is spent per member
# MAGIC * Based on how many rides the rider averages per month
# MAGIC * Based on how many minutes the rider spends on a bike per month

# COMMAND ----------

# MAGIC %sql
# MAGIC select dd.CalendarDay
# MAGIC   , dd.DayOfWeek
# MAGIC   , SUM(ft.ElapsedTimeSeconds) TotalElapsedTimeSeconds
# MAGIC   , COUNT(ft.TripKey) TripCount
# MAGIC   , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
# MAGIC from fact.trip ft
# MAGIC inner join dim.dates dd
# MAGIC   on ft.TripStartDateKey = dd.DateKey
# MAGIC group by dd.CalendarDay, dd.DayOfWeek
# MAGIC order by dd.DayOfWeek;

# COMMAND ----------

# MAGIC %sql
# MAGIC select ft.TripStartHourOfDay
# MAGIC   , SUM(ft.ElapsedTimeSeconds) TotalElapsedTimeSeconds
# MAGIC   , COUNT(ft.TripKey) TripCount
# MAGIC   , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
# MAGIC from fact.trip ft
# MAGIC group by ft.TripStartHourOfDay
# MAGIC order by ft.TripStartHourOfDay;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   dsstart.StationName StartStation
# MAGIC   , dsend.StationName EndStation
# MAGIC   , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
# MAGIC   , ROW_NUMBER() OVER (ORDER BY SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) DESC) Ranking
# MAGIC from fact.trip ft
# MAGIC inner join dim.station dsstart
# MAGIC   on ft.StartStationKey = dsstart.StationKey
# MAGIC inner join dim.station dsend
# MAGIC   on ft.EndStationKey = dsend.StationKey
# MAGIC group by dsstart.StationName
# MAGIC   , dsend.StationName
# MAGIC order by 3 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   dsstart.StationName StartStation
# MAGIC   , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
# MAGIC   , ROW_NUMBER() OVER (ORDER BY SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) DESC) Ranking
# MAGIC from fact.trip ft
# MAGIC inner join dim.station dsstart
# MAGIC   on ft.StartStationKey = dsstart.StationKey
# MAGIC inner join dim.station dsend
# MAGIC   on ft.EndStationKey = dsend.StationKey
# MAGIC group by dsstart.StationName
# MAGIC order by 3 asc
# MAGIC limit 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   dsstart.StationName StartStation
# MAGIC   , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
# MAGIC   , ROW_NUMBER() OVER (ORDER BY SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) DESC) Ranking
# MAGIC from fact.trip ft
# MAGIC inner join dim.station dsstart
# MAGIC   on ft.StartStationKey = dsstart.StationKey
# MAGIC inner join dim.station dsend
# MAGIC   on ft.EndStationKey = dsend.StationKey
# MAGIC group by dsstart.StationName
# MAGIC order by 3 asc
# MAGIC limit 25;

# COMMAND ----------

# MAGIC %sql
# MAGIC select ft.riderAgeYears
# MAGIC   , SUM(ft.ElapsedTimeSeconds) TotalElapsedTimeSeconds
# MAGIC   , COUNT(ft.TripKey) TripCount
# MAGIC   , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
# MAGIC from fact.trip ft
# MAGIC group by ft.riderAgeYears
# MAGIC order by ft.riderAgeYears;

# COMMAND ----------

# MAGIC %sql
# MAGIC select dr.IsMember
# MAGIC   , SUM(ft.ElapsedTimeSeconds) / COUNT(ft.TripKey) / 60.0 AvgTimePerTripMinutes
# MAGIC from fact.trip ft
# MAGIC inner join dim.rider dr
# MAGIC   on ft.RiderKey = dr.RiderKey
# MAGIC group by dr.IsMember
# MAGIC order by dr.IsMember;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   dd.CalendarMonth
# MAGIC   , dd.MonthOfYear
# MAGIC   , SUM(fp.Amount) Payments
# MAGIC from fact.payment fp
# MAGIC inner join dim.dates dd
# MAGIC   on fp.PaymentDateKey = dd.DateKey
# MAGIC group by dd.CalendarMonth
# MAGIC   , dd.MonthOfYear
# MAGIC order by dd.MonthOfYear

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   dd.QuarterOfYear
# MAGIC   , SUM(fp.Amount) Payments
# MAGIC from fact.payment fp
# MAGIC inner join dim.dates dd
# MAGIC   on fp.PaymentDateKey = dd.DateKey
# MAGIC group by dd.QuarterOfYear
# MAGIC order by dd.QuarterOfYear;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   dd.CalendarYear
# MAGIC   , SUM(fp.Amount) Payments
# MAGIC from fact.payment fp
# MAGIC inner join dim.dates dd
# MAGIC   on fp.PaymentDateKey = dd.DateKey
# MAGIC group by dd.CalendarYear
# MAGIC order by dd.CalendarYear;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   dr.AccountStartAgeYears
# MAGIC   , SUM(fp.Amount) / COUNT(DISTINCT dr.RiderKey) AmountPerMember
# MAGIC from fact.payment fp
# MAGIC inner join dim.rider dr
# MAGIC   on fp.RiderKey = dr.RiderKey
# MAGIC group by dr.AccountStartAgeYears
# MAGIC order by dr.AccountStartAgeYears;

# COMMAND ----------


