-- Databricks notebook source
-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC      f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
-- MAGIC         f"{storage_account_key}")

-- COMMAND ----------

create database f1_processed 


-- COMMAND ----------

use f1_processed;
select count(*) from results --25000
drop table results
