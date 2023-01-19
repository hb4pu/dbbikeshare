-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC #Transform Data - Load Facts
-- MAGIC 
-- MAGIC Manufacture Keys using ROW_NUMBER function
-- MAGIC 
-- MAGIC Areas for improvement: 
-- MAGIC 1. Update queries to Merge statements (Upserts). Can't do that without better way to create Keys for Dims. 

-- COMMAND ----------

TRUNCATE TABLE Fact.Payment;

INSERT INTO Fact.Payment(
  PaymentKey
  , RiderKey
  , PaymentDateKey
  , Amount
)
SELECT
  p.payment_id PaymentKey
  , dr.RiderKey
  , date_format(p.date, 'yyyyMMdd') PaymentDateKey
  , p.amount
FROM silver_payments p
LEFT JOIN Dim.Rider dr
  ON p.rider_id = dr.RemoteSystemRiderID


-- COMMAND ----------

TRUNCATE TABLE Fact.Trip;

INSERT INTO Fact.Trip(
  TripKey
  ,RiderKey
  ,TripStartDateKey
  ,TripStartHourOfDay
  ,StartStationKey
  ,EndStationKey
  ,ElapsedTimeSeconds
  ,RiderAgeYears
)
SELECT
  trip.trip_id                                                TripKey
  , rider.RiderKey                                            RiderKey
  , date_format(trip.start_at, 'yyyyMMdd')  TripStartDateKey
  , date_format(trip.start_at, 'HH') TripStartHourOfDay
  , startstation.StationKey                                   StartStationKey
  , endstation.StationKey                                     EndStationKey
  , datediff(SECOND, trip.start_at, trip.ended_at)            ElapsedTimeSeconds
  , rider.LivingAgeYears                                      RiderAgeYears
FROM silver_trips trip
LEFT JOIN Dim.Rider rider
  ON trip.rider_id = rider.RemoteSystemRiderID
LEFT JOIN Dim.Station startstation
  ON trip.start_station_id = startstation.RemoteSystemStationID
LEFT JOIN Dim.Station endstation
  ON trip.end_station_id = endstation.RemoteSystemStationID;

-- COMMAND ----------

-- There are some ridiculous trip elapsed times, but they are legit.  
-- I looked up F043F0F6A1AA4F85 within csv and it was loaded properly
SELECT
  trip.trip_id                                                TripKey
  , rider.RiderKey                                            RiderKey
  , startstation.StationKey                                   StartStationKey
  , trip.start_at
  , trip.ended_at
  , datediff(SECOND, trip.start_at, trip.ended_at)            ElapsedTimeSeconds
  , datediff(SECOND, trip.start_at, trip.ended_at) / 60.0 /060          ElapsedTimeHours
FROM silver_trips trip
LEFT JOIN Dim.Rider rider
  ON trip.rider_id = rider.RemoteSystemRiderID
LEFT JOIN Dim.Station startstation
  ON trip.start_station_id = startstation.RemoteSystemStationID
LEFT JOIN Dim.Station endstation
  ON trip.end_station_id = endstation.RemoteSystemStationID
order by 6 desc
limit 100;

-- COMMAND ----------


