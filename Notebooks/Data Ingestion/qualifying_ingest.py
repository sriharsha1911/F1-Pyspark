# Databricks notebook source
storage_account_name='f1sa'
storage_account_key='7lbW/Cb2rarnNH5il8lqOWODP2p0FSetLx7sfQkhGWs68ronFF71+YgwkZmc6BTMw+qeexPsjM1o+AStTY0cAA=='

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
qualify_df=spark.read.json(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/qualifying",schema=qualify_schema,multiLine=True)
display(qualify_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
qualify_df_renamed=qualify_df.withColumnRenamed('raceId','race_id').withColumnRenamed('driverId','driver_id').withColumnRenamed('qualifyId','qualify_id').withColumnRenamed('constructorId','constructor_id') \
.withColumn('date_ingested',current_timestamp())
display(qualify_df_renamed)

# COMMAND ----------

container_name='processed'
qualify_df_renamed.write.parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/qualifying",mode='overwrite')

# COMMAND ----------

df=spark.read.parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/qualifying")
display(df)
