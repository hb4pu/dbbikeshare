-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 05 - Prepare Star Schema
-- MAGIC 
-- MAGIC This was done within spark sql destination

-- COMMAND ----------

CREATE SCHEMA Dim;
CREATE SCHEMA Fact;

-- COMMAND ----------

DROP TABLE IF EXISTS Fact.Trip;

CREATE TABLE Fact.Trip(
	TripKey VARCHAR(36),
	RiderKey INT,
	TripStartDateKey INT NOT NULL,
    TripStartHourOfDay INT NOT NULL,
	StartStationKey INT NOT NULL,
	EndStationKey INT NOT NULL,
	ElapsedTimeSeconds INT NOT NULL,
	RiderAgeYears INT NOT NULL
) USING DELTA;

DROP TABLE IF EXISTS Fact.Payment;
CREATE TABLE Fact.Payment(
	PaymentKey INT NOT NULL,
	RiderKey INT NOT NULL,
	PaymentDateKey INT NOT NULL,
	Amount NUMERIC(18,4) NOT NULL
) USING DELTA;


DROP TABLE IF EXISTS Dim.Station;
CREATE TABLE Dim.Station(
	StationKey int,
	RemoteSystemStationID varchar(50),
	StationName varchar(250),
	Latitude float,
	Longitude float
);

DROP TABLE IF EXISTS Dim.Rider;
CREATE TABLE Dim.Rider(
	RiderKey int,
	RemoteSystemRiderID int,
	FirstName varchar(100),
	LastName varchar(100),
	BirthDateKey int,
	AccountStartDateKey int,
	AccountEndDateKey int,
	LivingAgeYears int,
	AccountStartAgeYears int,
	IsMember char(1) not NULL,
	IsActiveAccount char(1) not NULL
) USING DELTA;

DROP TABLE IF EXISTS Dim.Dates;

CREATE TABLE Dim.Dates(
	DateKey int,
	CalendarDate date,
	CalendarYear int,
	CalendarMonth varchar(25),
	MonthOfYear int,
    CalendarDay varchar(25),
	DayOfWeek int,
	DayOfMonth int,
    DayOfYear int,
	QuarterOfYear int	
) USING DELTA;

