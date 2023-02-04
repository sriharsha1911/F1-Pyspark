# Databricks notebook source
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

qualify_schema=StructType(fields=[StructField('qualifyId',IntegerType()),
                                  StructField('raceId',IntegerType()),
                                    StructField('driverId',IntegerType()),
                                    StructField('constructorId',IntegerType()),
                                    StructField('number',IntegerType()),
                                    StructField('position',IntegerType()),
                                    StructField('q1',StringType()),
                                    StructField('q2',StringType()),
                                  StructField('q3',StringType())
                                  ])
container_name='raw'
qualify_df=spark.read.json(f"{raw_folder_path}/{p_file_date}/qualifying",schema=qualify_schema,multiLine=True)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
qualify_df_renamed=qualify_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumnRenamed('qualifyId','qualify_id').withColumnRenamed('constructorId','constructor_id') \
.withColumn('date_ingested',current_timestamp()).withColumn('data_source',lit(data_source))


# COMMAND ----------

# incremental_load("f1_processed.qualifying",qualify_df_renamed,'race_id')
# #overwrite_partition(qualify_df_renamed, 'f1_processed', 'qualifying', 'race_id')
merge_condition='tgt.qualify_id=src.qualify_id  and tgt.race_id=src.race_id'
incremental_load(qualify_df_renamed,'f1_processed','qualifying','race_id',merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_processed.qualifying

# COMMAND ----------

dbutils.notebook.exit("Success")
