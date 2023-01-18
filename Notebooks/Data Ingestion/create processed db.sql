-- Databricks notebook source
-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC  	f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
-- MAGIC     	f"{storage_account_key}")

-- COMMAND ----------

show databases

-- COMMAND ----------

drop table if exists f1_processed;
create database if  not exists f1_processed 


-- COMMAND ----------

desc database extended f1_processed   
