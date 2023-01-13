# Databricks notebook source
storage_account_name='f1sa'
storage_account_key='7lbW/Cb2rarnNH5il8lqOWODP2p0FSetLx7sfQkhGWs68ronFF71+YgwkZmc6BTMw+qeexPsjM1o+AStTY0cAA=='

spark.conf.set(
 	f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    	f"{storage_account_key}")
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType

circuits_schema=StructType(fields=[StructField('circuitId',IntegerType(),False),
                                   StructField('circuitRef',StringType()),
                                   StructField('name',StringType()),
                                   StructField('location',StringType()),
                                   StructField('country',StringType()),
                                   StructField('lat',DoubleType()),
                                   StructField('lng',DoubleType()),
                                   StructField('alt',IntegerType()),
                                   StructField('url',StringType()),
                                  
                                  ])
container_name = "raw" 
##Instead of .option("inferschema":True) we can use schema()
circuits_df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/circuits.csv",header=True,schema=circuits_schema)


display(circuits_df)

#circuits_df.describe().show()
circuits_df.printSchema()

# COMMAND ----------

circuits_df_sel=circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)
display(circuits_df_sel)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Renaming columns

# COMMAND ----------

circuit_df_cols_rename=circuits_df_sel.withColumnRenamed('circuitId','circuit_id').withColumnRenamed('circuitRef','circuit_ref').withColumnRenamed('lat','latitude').withColumnRenamed('lng','longitude')
display(circuit_df_cols_rename)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding Timestamp column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
circuit_df=circuit_df_cols_rename.withColumn('date_ingested',current_timestamp())
display(circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## writing dataframe as parquet 

# COMMAND ----------

container_name='processed'
circuit_df.write.parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/circuits",mode='overwrite')

# COMMAND ----------

df=spark.read.parquet(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/circuits")
display(df)
