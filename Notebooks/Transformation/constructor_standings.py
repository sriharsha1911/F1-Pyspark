# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    	f"{storage_account_key}")

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_results_df)

# COMMAND ----------

#from pyspark.sql.functions import sum,when,desc,rank,col,asc,count,window
#driver_standing_df = race_results_df.groupBy('race_year','team')\
#                                      .agg(sum(col('points')).alias("total_points"), count(when(col('position')==1 ,True)).alias('wins'))
#display(driver_standing_df)
from pyspark.sql.functions import sum, when, count, col

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

#display(constructor_standings_df.filter("race_year = 2020"))
constructor_standings_df.write.mode('overwrite').format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

#final_df.write.parquet(f"{presentation_folder_path}/constructor_standings",mode='overwrite')
