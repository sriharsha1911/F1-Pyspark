# Databricks notebook source
storage_account_name='f1sa'
storage_account_key='7lbW/Cb2rarnNH5il8lqOWODP2p0FSetLx7sfQkhGWs68ronFF71+YgwkZmc6BTMw+qeexPsjM1o+AStTY0cAA=='

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
container_name='raw'
pitstops_df=spark.read.json(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/pit_stops.json",schema=pitstsops_schema,multiLine=True)
display(pitstops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
pitstops_df_renamed=pitstops_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumn('date_ingested',current_timestamp())
display(pitstops_df_renamed)

# COMMAND ----------

container_name='processed'
pitstops_df_renamed.write.parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/pit_stops",mode='overwrite')

# COMMAND ----------

df=spark.read.parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/pit_stops")
display(df)
