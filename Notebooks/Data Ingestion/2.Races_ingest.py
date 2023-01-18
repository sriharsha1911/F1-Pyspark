# Databricks notebook source
dbutils.widgets.text('p_data_source',"")


# COMMAND ----------

data_sour=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------



from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType

spark.conf.set(
 	f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    	f"{storage_account_key}")
races_schema=StructType(fields=[StructField('raceId',IntegerType(),False),
                                   StructField('year',IntegerType()),
                                   StructField('round',IntegerType()),
                                   StructField('circuitId',IntegerType()),
                                   StructField('name',StringType()),
                                   StructField('date',DoubleType()),
                                   StructField('time',DoubleType()),
                                   StructField('url',StringType()),
                                  
                                  ])

Races_df = spark.read.csv(f"{raw_folder_path}/races.csv",header=True,schema=races_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dropping URl column

# COMMAND ----------

Races_df_dropcol=Races_df.drop(Races_df.url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming columns and adding race_timestamp column 

# COMMAND ----------

from pyspark.sql.functions import concat,lit,to_timestamp
Races_df_col_rename=Races_df_dropcol.withColumnRenamed('raceId','race_id').withColumnRenamed('year','race_year') \
.withColumnRenamed('circuitId','circuit_id') \
.withColumn('race_timestamp', to_timestamp(concat(Races_df_dropcol.date,lit(' '),Races_df_dropcol.time),'yyyy-MM-dd HH:mm:ss' )) \
.withColumn('data_source',lit(data_sour))


# COMMAND ----------

# MAGIC %md
# MAGIC ## adding date_ingested column and dropping date and time columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
Races_df=Races_df_col_rename.withColumn('date_ingested',current_timestamp()).drop('date','time')



# COMMAND ----------

# MAGIC %md
# MAGIC ## Loadind as parquet file to adls

# COMMAND ----------

Races_df.write.parquet(f"{processed_folder_path}/races",mode='overwrite')
Races_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

df=spark.read.parquet("dbfs:/user/hive/warehouse/f1_processed.db/races")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
