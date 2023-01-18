-- Databricks notebook source
use f1_presentation;

select * from f1_presentation.calculated_race_results

-- COMMAND ----------

use f1_presentation;
select driver_name,count(*) as total_races ,sum(calculated_points
) as total_points,avg(calculated_points
) as avg_points from calculated_race_results
group by driver_name
having count(*)>=50
order by avg_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC dominant drivers in last decade

-- COMMAND ----------

select driver_name,count(*) as total_races ,sum(calculated_points
) as total_points,avg(calculated_points
) as avg_points from calculated_race_results
where race_year between 211 and 2020
group by driver_name
having count(*)>=50
order by avg_points desc
