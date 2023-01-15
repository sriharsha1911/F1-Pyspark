# Databricks notebook source
storage_account_name='f1sa'
storage_account_key='7lbW/Cb2rarnNH5il8lqOWODP2p0FSetLx7sfQkhGWs68ronFF71+YgwkZmc6BTMw+qeexPsjM1o+AStTY0cAA=='

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
laptimes_df=spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/lap_times",schema=laptimes_schema)
display(laptimes_df)
laptimes_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
laptimes_df_renamed=laptimes_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumn('date_ingested',current_timestamp())
display(laptimes_df_renamed)

# COMMAND ----------

container_name='processed'
laptimes_df_renamed.write.parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/lap_times",mode='overwrite')

# COMMAND ----------

df=spark.read.parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/lap_times")
display(df)
df.count()
