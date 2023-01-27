# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from race_results_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from global_temp.race_results_global_view
