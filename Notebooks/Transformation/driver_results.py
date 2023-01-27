# Databricks notebook source
dbutils.widgets.text('p_file_date',"2021-03-21")
p_file_date =dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    	f"{storage_account_key}")

race_years_list=spark.read.parquet(f"{presentation_folder_path}/race_results").filter(f"p_file_date='{p_file_date}'").select("race_year").distinct().collect()
print(race_years_list)


# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col
new_race_year_list=[]
for race_year in race_years_list:
     new_race_year_list.append(race_year.race_year)
race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results").filter(col("race_year").isin(new_race_year_list))
display(race_results_df)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import sum,when,desc,rank,col,asc,count
from pyspark.sql.window import Window

driver_standing_df = race_results_df.groupBy('race_year','driver_name','driver_nationality','team')\
                                      .agg(sum('points').alias('total_points'),count(when(col('position')==1 ,True)).alias('wins'))

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))








# COMMAND ----------

#final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_presentation.driver_results")
incremental_load("f1_presentation.driver_results",final_df,'race_year')
#df=spark.read.parquet(f"{presentation_folder_path}/driver_standings")
#display(df.filter(df.race_year==2020))
#df.write.parquet(f"{presentation_folder_path}/driver_standings",mode='overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(race_year) from f1_presentation.driver_results 
