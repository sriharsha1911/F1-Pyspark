# Databricks notebook source
dbutils.widgets.text('p_data_source',"")
data_source=dbutils.get('p_data_source')

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


Results_df=spark.read.json(f"{raw_folder_path}/results.json",schema=Results_schema)
display(Results_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming and  dropping columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
Results_df_renamed=Results_df.withColumnRenamed('resultId','result_id') \
                             .withColumnRenamed('raceId','race_id') \
                            .withColumnRenamed('driverId','driver_id') \
.withColumnRenamed('constructorId','constructor_id').withColumnRenamed('positionText','position_text').withColumnRenamed('positionOrder','position_order').withColumnRenamed('fastestLap','fastest_lap') \
.withColumnRenamed('fastestlapTime','fastest_lap_time').withColumnRenamed('fastestlapSpeed','fastest_lap_speed').drop(Results_df.statusId).withColumn('date_ingested',current_timestamp())



# COMMAND ----------

# MAGIC %md
# MAGIC ## writing df as parquet file to ADLS

# COMMAND ----------


Results_df_renamed.write.partitionBy('race_id').parquet(f"{processed_folder_path}/results",mode='overwrite')

# COMMAND ----------

df=spark.read.parquet(f"{processed_folder_path}/results")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
