# Databricks notebook source
# MAGIC %md
# MAGIC ## 04 - Establish Date Dim in Delta

# COMMAND ----------

#Attribution - used this article to build date dim in spark: https://www.bluegranite.com/blog/generate-a-calendar-dimension-in-spark

from pyspark.sql.functions import explode, sequence, to_date
beginDate = '1937-01-01'
endDate = '2050-12-31'
(
  spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
    .createOrReplaceTempView('dates')
)


# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold_dates
# MAGIC using delta
# MAGIC location '/delta/gold/gold_dates'
# MAGIC as select
# MAGIC   year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as DateKey,
# MAGIC   CalendarDate,
# MAGIC   year(calendarDate) AS CalendarYear,
# MAGIC   date_format(calendarDate, 'MMMM') as CalendarMonth,
# MAGIC   month(calendarDate) as MonthOfYear,
# MAGIC   date_format(calendarDate, 'EEEE') as CalendarDay,
# MAGIC   dayofweek(calendarDate) AS DayOfWeek,
# MAGIC   weekday(calendarDate) + 1 as DayOfWeekStartMonday,
# MAGIC   case
# MAGIC     when weekday(calendarDate) < 5 then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsWeekDay,
# MAGIC   dayofmonth(calendarDate) as DayOfMonth,
# MAGIC   case
# MAGIC     when calendarDate = last_day(calendarDate) then 'Y'
# MAGIC     else 'N'
# MAGIC   end as IsLastDayOfMonth,
# MAGIC   dayofyear(calendarDate) as DayOfYear,
# MAGIC   weekofyear(calendarDate) as WeekOfYearIso,
# MAGIC   quarter(calendarDate) as QuarterOfYear
# MAGIC from
# MAGIC   dates
# MAGIC order by
# MAGIC   calendarDate
