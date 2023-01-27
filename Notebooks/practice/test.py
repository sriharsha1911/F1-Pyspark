# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",f"{storage_account_key}"
)

# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_folder_path}/constructors")
