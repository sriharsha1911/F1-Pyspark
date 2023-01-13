# Databricks notebook source
storage_account_name='f1sa'
storage_account_key='7lbW/Cb2rarnNH5il8lqOWODP2p0FSetLx7sfQkhGWs68ronFF71+YgwkZmc6BTMw+qeexPsjM1o+AStTY0cAA=='

spark.conf.set(
 	f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    	f"{storage_account_key}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ingest data

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType
name_schema=StructType(fields=[StructField('forename',StringType()),
                                StructField('surname',StringType()),
                               
                               ])
driver_schema=StructType(fields=[StructField('driverId',IntegerType()),
                                 StructField('driverRef',StringType()),
                                  StructField('number',IntegerType()),
                                  StructField('code',StringType()),
                                  StructField('name',name_schema),
                                  StructField('dob',DateType()),
                                  StructField('url',StringType()),
                                  StructField('nationality',StringType())
                               ])
container_name="raw"
drivers_df=spark.read.json(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/drivers.json",schema=driver_schema)
drivers_df.printSchema()
display(drivers_df)


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Renaming cols and adding column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import concat,lit
drivers_df_addcol=drivers_df.withColumnRenamed('driverID','driver_id').withColumnRenamed('driverRef','driver_ref') \
.withColumn('date_ingested',current_timestamp()).withColumn('name',concat(drivers_df.name.forename,lit(' '),drivers_df.name.surname)).drop('url')
display(drivers_df_addcol)

# COMMAND ----------

container_name='processed'
drivers_df_addcol.write.parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/drivers",mode='overwrite')

# COMMAND ----------

df=spark.read.parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/drivers")
display(df)
