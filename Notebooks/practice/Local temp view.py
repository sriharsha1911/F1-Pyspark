# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    	f"{storage_account_key}")

race_standing_df=spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_standing_df)

# COMMAND ----------

race_standing_df.createOrReplaceTempView('race_results_temp_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from race_results_temp_view where race_year=2020

# COMMAND ----------

race_year=2020
race_results_2020=spark.sql(f'select * from race_results_temp_view where race_year= {race_year}')
display(race_results_2020)

# COMMAND ----------

race_standing_df.createOrReplaceGlobalTempView('race_results_global_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.race_results_global_view

# COMMAND ----------

# MAGIC %sql
# MAGIC show Tables in global_temp
