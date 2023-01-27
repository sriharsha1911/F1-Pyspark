-- Databricks notebook source
use f1_processed

-- COMMAND ----------



-- COMMAND ----------

create table  if not exists  f1_presentation.calculated_race_results
using parquet
as
select ra.race_year,c.name as team_name,d.name as driver_name,r.position,r.points,11-r.position as calculated_points  from results r
join drivers d on r.driver_id=d.driver_id
join constructors c on r.constructor_id=c.constructor_id
join races ra on r.race_id=ra.race_id
where r.position<11

-- COMMAND ----------

select * from f1_presentation.calculated_race_results
