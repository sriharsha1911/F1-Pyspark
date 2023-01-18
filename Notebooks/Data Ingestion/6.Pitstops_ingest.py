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

pitstsops_schema=StructType(fields=[StructField('raceId',IntegerType()),
                                    StructField('driverId',IntegerType()),
                                    StructField('stop',IntegerType()),
                                    StructField('lap',IntegerType()),
                                    StructField('time',StringType()),
                                    StructField('duration',StringType()),
                                    StructField('milliseconds',IntegerType())

                                  ])

pitstops_df=spark.read.json(f"{raw_folder_path}/pit_stops.json",schema=pitstsops_schema,multiLine=True)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
pitstops_df_renamed=pitstops_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumn('date_ingested',current_timestamp()) \
.withColumn('data_source',lit(data_source))


# COMMAND ----------

#pitstops_df_renamed.write.parquet(f"{processed_folder_path}/pit_stops",mode='overwrite')
pitstops_df_renamed.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

df=spark.read.parquet(f"dbfs:/user/hive/warehouse/f1_processed.db/pit_stops")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
