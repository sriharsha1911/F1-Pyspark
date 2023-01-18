# Databricks notebook source
dbutils.widgets.text('p_data_source',"")
data_source=dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------



spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",f"{storage_account_key}"
)

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType

laptimes_schema=StructType(fields=[StructField('raceId',IntegerType()),
                                    StructField('driverId',IntegerType()),
                                    StructField('position',IntegerType()),
                                    StructField('lap',IntegerType()),
                                    StructField('time',StringType()),
                                    StructField('milliseconds',IntegerType())

                                  ])
container_name='raw'
laptimes_df=spark.read.csv(f"{raw_folder_path}/lap_times",schema=laptimes_schema)
display(laptimes_df)
laptimes_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
laptimes_df_renamed=laptimes_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumn('date_ingested',current_timestamp()) \
.withColumn('data_source',lit(data_source))


# COMMAND ----------

container_name='processed'
#laptimes_df_renamed.write.parquet(f"{processed_folder_path}/lap_times",mode='overwrite')
laptimes_df_renamed.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

df=spark.read.parquet(f"dbfs:/user/hive/warehouse/f1_processed.db/lap_times")
display(df)
df.count()

# COMMAND ----------

dbutils.notebook.exit("Success")
