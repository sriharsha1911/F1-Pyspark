# Databricks notebook source
storage_account_name='f1sa'
storage_account_key='7lbW/Cb2rarnNH5il8lqOWODP2p0FSetLx7sfQkhGWs68ronFF71+YgwkZmc6BTMw+qeexPsjM1o+AStTY0cAA=='

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

constructor_df = spark.read.json(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/constructors.json",schema=constructor_schema)
display(constructor_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
constructor_df_ren=constructor_df.withColumnRenamed('constructorId','constructor_id').withColumnRenamed('constructorRef','constructor_ref').drop(constructor_df.url) \
.withColumn('date_ingested',current_timestamp())
display(constructor_df_ren)

# COMMAND ----------

container_name='processed'
constructor_df_ren.write.parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/constructors",mode='overwrite')

# COMMAND ----------

df=spark.read.parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/constructors")
display(df)
