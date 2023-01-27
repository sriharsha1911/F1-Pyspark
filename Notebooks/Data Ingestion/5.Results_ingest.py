# Databricks notebook source
dbutils.widgets.text('p_data_source',"")
data_source=dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")
p_file_date =dbutils.widgets.get("p_file_date")

# COMMAND ----------

print(data_source,p_file_date)

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------



spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",f"{storage_account_key}"
)

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType


Results_schema=StructType(fields=[StructField('resultId',IntegerType()),
                                  StructField('raceId',IntegerType()),
                                  StructField('driverId',IntegerType()),
                                  StructField('constructorId',IntegerType()),
                                  StructField('number',IntegerType()),
                                  StructField('grid',IntegerType()),
                                  StructField('position',IntegerType()),
                                  StructField('positionText',StringType()),
                                  StructField('positionOrder',IntegerType()),
                                  StructField('points',FloatType()),
                                  StructField('laps',IntegerType()),
                                  StructField('time',StringType()),
                                  StructField('milliseconds',IntegerType()),
                                  StructField('fastestLap',IntegerType()),
                                  StructField('rank',IntegerType()),
                                  StructField('fastestLapTime',StringType()),
                                  StructField('fastestLapSpeed',StringType()),
                                  StructField('statusId',IntegerType())
                                  ])


Results_df=spark.read.json(f"{raw_folder_path}/{p_file_date}/results.json",schema=Results_schema)
#display(Results_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming and  dropping columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
Results_df_renamed=Results_df.withColumnRenamed('resultId','result_id') \
                             .withColumnRenamed('raceId','race_id') \
                            .withColumnRenamed('driverId','driver_id') \
.withColumnRenamed('constructorId','constructor_id').withColumnRenamed('positionText','position_text').withColumnRenamed('positionOrder','position_order').withColumnRenamed('fastestLap','fastest_lap') \
.withColumnRenamed('fastestlapTime','fastest_lap_time').withColumnRenamed('fastestlapSpeed','fastest_lap_speed').drop(Results_df.statusId).withColumn('date_ingested',current_timestamp()).withColumn('data_source',lit(data_source)).withColumn('p_file_date',lit(p_file_date))
#display(Results_df_renamed)


# COMMAND ----------

# MAGIC %md
# MAGIC ## writing df as parquet file to ADLS

# COMMAND ----------

#Method 1
# for race_id_list in Results_df_renamed.select("race_id").distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     #spark.sql("Alter table f1_processed.results drop  if exists partition(race_id={race_id_list.race_id})")
#         #spark.sql("Alter table f1_processed.results drop  if exists partition(race_id=={race_id_list})")
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")


# COMMAND ----------

#Results_df_renamed.write.partitionBy('race_id').parquet(f"{processed_folder_path}/results",mode='overwrite')


# COMMAND ----------

# method 2


# COMMAND ----------

#incremental_load("f1_processed.results",Results_df_renamed,'race_id')
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning",True)
from delta.tables import DeltaTable
if spark.catalog.tableExists("f1_processed.results"):
    delta_table = DeltaTable.forPath(spark,"dbfs:/user/hive/warehouse/f1_processed.db/results")
    delta_table.alias("tgt").merge(Results_df_renamed.alias("src"),  "tgt.result_id=src.result_id  and tgt.race_id=src.race_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    #Results_df_renamed.write.mode("overwrite").insertInto("f1_processed.results")
else:
    Results_df_renamed.write.mode("overwrite").partitionBy('race_id').format("delta").saveAsTable("f1_processed.results")



# COMMAND ----------

#df=spark.read.parquet("dbfs:/user/hive/warehouse/f1_processed.db/results")
#display(df)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'dbfs:/user/hive/warehouse/f1_processed.db/results/_delta_log'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC select race_id,count(*) from f1_processed.results group by race_id order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")
