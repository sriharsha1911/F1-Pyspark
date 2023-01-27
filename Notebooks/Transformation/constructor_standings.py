# Databricks notebook source
# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")
p_file_date =dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    	f"{storage_account_key}")

race_years_list=spark.read.parquet(f"{presentation_folder_path}/race_results").filter(f"p_file_date='{p_file_date}'").select("race_year").distinct().collect()


# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col
new_race_year_list=[]
for race_year in race_years_list:
     new_race_year_list.append(race_year.race_year)
race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(new_race_year_list))
display(race_results_df)

# COMMAND ----------

#from pyspark.sql.functions import sum,when,desc,rank,col,asc,count,window
#driver_standing_df = race_results_df.groupBy('race_year','team')\
#                                      .agg(sum(col('points')).alias("total_points"), count(when(col('position')==1 ,True)).alias('wins'))
#display(driver_standing_df)


constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

display(final_df.filter("race_year = 2020"))

# COMMAND ----------



# COMMAND ----------

#final_df.write.parquet(f"{presentation_folder_path}/constructor_standings",mode='overwrite')
incremental_load("f1_presentation.constructor_standings",final_df,'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings where race_year='2021'

# COMMAND ----------


