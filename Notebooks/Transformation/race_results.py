# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    	f"{storage_account_key}")

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races").select('race_id','circuit_id','race_year','name','race_timestamp').withColumnRenamed('race_timestamp','race_date') \
.withColumnRenamed('name','race_name').withColumnRenamed('race_id','ra_race_id').withColumnRenamed('circuit_id','ra_circuit_id') 

display(races_df)

drivers_df=spark.read.parquet(f"{processed_folder_path}/drivers").select('driver_id','name','number','nationality') \
.withColumnRenamed('name','driver_name').withColumnRenamed('number','driver_number').withColumnRenamed('nationality','driver_nationality').withColumnRenamed('driver_id','dr_driver_id')

constructors_df=spark.read.parquet(f"{processed_folder_path}/constructors").select('constructor_id','name').withColumnRenamed('name','team').withColumnRenamed('constructor_id','con_constructor_id')

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits").select('circuit_id','location').withColumnRenamed('location','circuit_location')

results_df=spark.read.parquet(f"{processed_folder_path}/results").select('race_id','position','driver_id','constructor_id','grid','fastest_lap','time','points').withColumnRenamed('time','race_time')

# COMMAND ----------

race_results_df=results_df.join(races_df,results_df.race_id==races_df.ra_race_id,'inner') \
.join(drivers_df,results_df.driver_id==drivers_df.dr_driver_id,'inner') \
.join(constructors_df,results_df.constructor_id==constructors_df.con_constructor_id,'inner') \

from pyspark.sql.functions import current_timestamp

race_results_df=race_results_df.join(circuits_df,race_results_df.ra_circuit_id==circuits_df.circuit_id).withColumn('created_date',current_timestamp()).drop('race_id','ra_race_id','ra_circuit_id','circuit_id','dr_driver_id','driver_id','con_constructor_id','constructor_id') 
#.filter((race_results_df.race_year == 2020) & (race_results_df.race_name == 'Abu Dhabi Grand Prix')).orderBy(race_results_df.points.desc())

display(race_results_df)
race_results_df.count()


# COMMAND ----------

race_results_df.write.mode('overwrite').format("parquet").saveAsTable("f1_presentation.race_results")
#race_results_df.write.parquet(f"{presentation_folder_path}/race_results",mode='overwrite')

# COMMAND ----------

df=spark.read.parquet("dbfs:/user/hive/warehouse/f1_presentation.db/race_results")
display(df)
df.count()
