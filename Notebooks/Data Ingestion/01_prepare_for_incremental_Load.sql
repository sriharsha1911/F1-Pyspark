-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------


CREATE DATABASE IF NOT EXISTS f1_presentation 


-- COMMAND ----------

use f1_processed;
select * from circuits
