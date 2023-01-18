# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    	f"{storage_account_key}")

race_standing_df=spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_standing_df)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import sum,when,desc,rank,col,asc,count
from pyspark.sql.window import Window

driver_standing_df = race_standing_df.groupBy('race_year','driver_name','driver_nationality','team')\
                                      .agg(sum('points').alias('total_points'),count(when(col('position')==1 ,True)).alias('wins'))

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))
print(final_df.count())







# COMMAND ----------

final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_presentation.driver_results")

#df=spark.read.parquet(f"{presentation_folder_path}/driver_standings")
#display(df.filter(df.race_year==2020))
#df.write.parquet(f"{presentation_folder_path}/driver_standings",mode='overwrite')
