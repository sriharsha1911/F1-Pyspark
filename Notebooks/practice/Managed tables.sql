-- Databricks notebook source
-- MAGIC %md
-- MAGIC * Create manage tables in python

-- COMMAND ----------

-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC     f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
-- MAGIC     	f"{storage_account_key}")
-- MAGIC 
-- MAGIC race_standing_df=spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC #display(race_standing_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_standing_df.write.format("parquet").mode("overwrite").saveAsTable("race_results_py")
-- MAGIC # the underlying data will be stored as parquet file in ADLS

-- COMMAND ----------

select current_database()

-- COMMAND ----------


show tables;
desc table race_results_py

-- COMMAND ----------

select * from race_results_py where race_year=2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * create managed table using sql

-- COMMAND ----------

create table race_results_sql as 
select * from race_results_py where race_year=2020


-- COMMAND ----------

show tables;
drop table race_results_sql
