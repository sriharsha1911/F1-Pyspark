# Databricks notebook source
dbutils.widgets.text('p_data_source',"")


# COMMAND ----------

data_source=dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------



spark.conf.set(
 	f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    	f"{storage_account_key}")
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType

constructor_schema=StructType(fields=[StructField('constructorId',IntegerType(),False),
                                   StructField('constructorRef',StringType()),
                                   StructField('name',StringType()),
                                   StructField('nationality',StringType()),
                                   
                                   StructField('url',StringType())
                                  ])

container_name = "raw" 

constructor_df = spark.read.json(f"{raw_folder_path}/constructors.json",schema=constructor_schema)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
constructor_df_ren=constructor_df.withColumnRenamed('constructorId','constructor_id').withColumnRenamed('constructorRef','constructor_ref').drop(constructor_df.url) \
.withColumn('date_ingested',current_timestamp()).withColumn('data_source',lit(data_source))


# COMMAND ----------


constructor_df_ren.write.parquet(f"{processed_folder_path}/constructors",mode='overwrite')
constructor_df_ren.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

df=spark.read.parquet("dbfs:/user/hive/warehouse/f1_processed.db/constructors")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
