# Databricks notebook source


# COMMAND ----------

dbutils.widgets.text('p_data_source',"")
data_source=dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")
p_file_date =dbutils.widgets.get("p_file_date")

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

pitstops_df=spark.read.json(f"{raw_folder_path}/{p_file_date}/pit_stops.json",schema=pitstsops_schema,multiLine=True)


# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
pitstops_df_renamed=pitstops_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumn('date_ingested',current_timestamp()) \
.withColumn('data_source',lit(data_source)).withColumn('p_file_date',lit(p_file_date))


# COMMAND ----------

# MAGIC %md 
# MAGIC load to ADLS

# COMMAND ----------

# #incremental_load("f1_processed.pitstops",pitstops_df_renamed,'race_id')
# overwrite_partition(pitstops_df_renamed, 'f1_processed', 'pit_stops', 'race_id')
merge_condition='tgt.driver_id=src.driver_id  and tgt.race_id=src.race_id and tgt.lap=src.lap'
incremental_load(pitstops_df_renamed,'f1_processed','pitstops','race_id',merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_processed.pitstops 

# COMMAND ----------

dbutils.notebook.exit("Success")
