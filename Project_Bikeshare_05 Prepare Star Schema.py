# Databricks notebook source
# MAGIC %md
# MAGIC ## 05 - Prepare Star Schema
# MAGIC 
# MAGIC This was done within spark sql destination

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA Dim;
# MAGIC CREATE SCHEMA Fact;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Fact.Trip;
# MAGIC 
# MAGIC CREATE TABLE Fact.Trip(
# MAGIC 	TripKey VARCHAR(36),
# MAGIC 	RiderKey INT,
# MAGIC 	TripStartDateKey INT NOT NULL,
# MAGIC     TripStartHourOfDay INT NOT NULL,
# MAGIC 	StartStationKey INT NOT NULL,
# MAGIC 	EndStationKey INT NOT NULL,
# MAGIC 	ElapsedTimeSeconds INT NOT NULL,
# MAGIC 	RiderAgeYears INT NOT NULL
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Fact.Payment;
# MAGIC CREATE TABLE Fact.Payment(
# MAGIC 	PaymentKey INT NOT NULL,
# MAGIC 	RiderKey INT NOT NULL,
# MAGIC 	PaymentDateKey INT NOT NULL,
# MAGIC 	Amount NUMERIC(18,4) NOT NULL
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Dim.Station;
# MAGIC CREATE TABLE Dim.Station(
# MAGIC 	StationKey int,
# MAGIC 	RemoteSystemStationID varchar(50),
# MAGIC 	StationName varchar(250),
# MAGIC 	Latitude float,
# MAGIC 	Longitude float
# MAGIC );
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Dim.Rider;
# MAGIC CREATE TABLE Dim.Rider(
# MAGIC 	RiderKey int,
# MAGIC 	RemoteSystemRiderID int,
# MAGIC 	FirstName varchar(100),
# MAGIC 	LastName varchar(100),
# MAGIC 	BirthDateKey int,
# MAGIC 	AccountStartDateKey int,
# MAGIC 	AccountEndDateKey int,
# MAGIC 	LivingAgeYears int,
# MAGIC 	AccountStartAgeYears int,
# MAGIC 	IsMember char(1) not NULL,
# MAGIC 	IsActiveAccount char(1) not NULL
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Dim.Dates;
# MAGIC 
# MAGIC CREATE TABLE Dim.Dates(
# MAGIC 	DateKey int,
# MAGIC 	CalendarDate date,
# MAGIC 	CalendarYear int,
# MAGIC 	CalendarMonth varchar(25),
# MAGIC 	MonthOfYear int,
# MAGIC     CalendarDay varchar(25),
# MAGIC 	DayOfWeek int,
# MAGIC 	DayOfMonth int,
# MAGIC     DayOfYear int,
# MAGIC 	QuarterOfYear int	
# MAGIC ) USING DELTA;