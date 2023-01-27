-- Databricks notebook source
use f1_presentation;
select team_name,count(*) as total_races ,sum(calculated_points
) as total_points,avg(calculated_points
) as avg_points from calculated_race_results
group by team_name
having count(*)>=100
order by avg_points desc


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC last decade

-- COMMAND ----------

use f1_presentation;
select team_name,count(*) as total_races ,sum(calculated_points
) as total_points,avg(calculated_points
) as avg_points from calculated_race_results
where race_year between 2011 and 2020
group by team_name
having count(*)>=100
order by avg_points desc
