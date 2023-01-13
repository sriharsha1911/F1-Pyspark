# Databricks notebook source
storage_account_name='f1sa'
storage_account_key='7lbW/Cb2rarnNH5il8lqOWODP2p0FSetLx7sfQkhGWs68ronFF71+YgwkZmc6BTMw+qeexPsjM1o+AStTY0cAA=='

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
container_name = "raw" 

Races_df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/races.csv",header=True,schema=races_schema)


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
.withColumn('race_timestamp', to_timestamp(concat(Races_df_dropcol.date,lit(' '),Races_df_dropcol.time),'yyyy-MM-dd HH:mm:ss' ))
display(Races_df_col_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## adding date_ingested column and dropping date and time columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
Races_df=Races_df_col_rename.withColumn('date_ingested',current_timestamp()).drop('date','time')
display(Races_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Loadind as parquet file to adls

# COMMAND ----------

container_name='processed'
Races_df.write.partitionBy('race_year').parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/races",mode='overwrite')
