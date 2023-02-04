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

laptimes_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

laptimes_df=spark.read.csv(f"{raw_folder_path}/{p_file_date}/lap_times",schema=laptimes_schema)
display(laptimes_df)
laptimes_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
laptimes_df_renamed=laptimes_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumn('date_ingested',current_timestamp()) \
.withColumn('data_source',lit(data_source)).withColumn('p_file_date',lit(p_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC load to ADLS

# COMMAND ----------

laptimes_df_renamed.dtypes

# COMMAND ----------

# #incremental_load("f1_processed.lap_times",laptimes_df_renamed,'race_id')
# overwrite_partition(laptimes_df_renamed, 'f1_processed', 'lap_times', 'race_id')
merge_condition='tgt.driver_id=src.driver_id  and tgt.race_id=src.race_id and tgt.lap=src.lap'
incremental_load(laptimes_df_renamed,'f1_processed','laptimes','race_id',merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
