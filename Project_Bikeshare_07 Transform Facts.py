# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Transform Data - Load Facts
# MAGIC 
# MAGIC Manufacture Keys using ROW_NUMBER function
# MAGIC 
# MAGIC Areas for improvement: 
# MAGIC 1. Update queries to Merge statements (Upserts). Can't do that without better way to create Keys for Dims. 

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE Fact.Payment;
# MAGIC 
# MAGIC INSERT INTO Fact.Payment(
# MAGIC   PaymentKey
# MAGIC   , RiderKey
# MAGIC   , PaymentDateKey
# MAGIC   , Amount
# MAGIC )
# MAGIC SELECT
# MAGIC   p.payment_id PaymentKey
# MAGIC   , dr.RiderKey
# MAGIC   , date_format(p.date, 'yyyyMMdd') PaymentDateKey
# MAGIC   , p.amount
# MAGIC FROM silver_payments p
# MAGIC LEFT JOIN Dim.Rider dr
# MAGIC   ON p.rider_id = dr.RemoteSystemRiderID

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE Fact.Trip;
# MAGIC 
# MAGIC INSERT INTO Fact.Trip(
# MAGIC   TripKey
# MAGIC   ,RiderKey
# MAGIC   ,TripStartDateKey
# MAGIC   ,TripStartHourOfDay
# MAGIC   ,StartStationKey
# MAGIC   ,EndStationKey
# MAGIC   ,ElapsedTimeSeconds
# MAGIC   ,RiderAgeYears
# MAGIC )
# MAGIC SELECT
# MAGIC   trip.trip_id                                                TripKey
# MAGIC   , rider.RiderKey                                            RiderKey
# MAGIC   , date_format(trip.start_at, 'yyyyMMdd')  TripStartDateKey
# MAGIC   , date_format(trip.start_at, 'HH') TripStartHourOfDay
# MAGIC   , startstation.StationKey                                   StartStationKey
# MAGIC   , endstation.StationKey                                     EndStationKey
# MAGIC   , datediff(SECOND, trip.start_at, trip.ended_at)            ElapsedTimeSeconds
# MAGIC   , floor(datediff(trip.start_at, riderBirthDay.CalendarDate)/365.0, 0)   RiderAgeYears
# MAGIC FROM silver_trips trip
# MAGIC LEFT JOIN Dim.Rider rider
# MAGIC   ON trip.rider_id = rider.RemoteSystemRiderID
# MAGIC LEFT JOIN Dim.Dates riderBirthDay
# MAGIC   ON rider.BirthDateKey = riderBirthDay.DateKey
# MAGIC LEFT JOIN Dim.Station startstation
# MAGIC   ON trip.start_station_id = startstation.RemoteSystemStationID
# MAGIC LEFT JOIN Dim.Station endstation
# MAGIC   ON trip.end_station_id = endstation.RemoteSystemStationID;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- There are some ridiculous trip elapsed times, but they are legit.  
# MAGIC -- I looked up F043F0F6A1AA4F85 within csv and it was loaded properly
# MAGIC SELECT
# MAGIC   trip.trip_id                                                TripKey
# MAGIC   , rider.RiderKey                                            RiderKey
# MAGIC   , startstation.StationKey                                   StartStationKey
# MAGIC   , trip.start_at
# MAGIC   , trip.ended_at
# MAGIC   , datediff(SECOND, trip.start_at, trip.ended_at)            ElapsedTimeSeconds
# MAGIC   , datediff(SECOND, trip.start_at, trip.ended_at) / 60.0 /060          ElapsedTimeHours
# MAGIC FROM silver_trips trip
# MAGIC LEFT JOIN Dim.Rider rider
# MAGIC   ON trip.rider_id = rider.RemoteSystemRiderID
# MAGIC LEFT JOIN Dim.Station startstation
# MAGIC   ON trip.start_station_id = startstation.RemoteSystemStationID
# MAGIC LEFT JOIN Dim.Station endstation
# MAGIC   ON trip.end_station_id = endstation.RemoteSystemStationID
# MAGIC order by 6 desc
# MAGIC limit 100;

# COMMAND ----------


