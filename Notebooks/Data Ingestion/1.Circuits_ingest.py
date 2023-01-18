# Databricks notebook source
dbutils.widgets.text('p_data_source',"Ergast API")

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")

# COMMAND ----------

data_sour=dbutils.widgets.get("p_data_source")

# COMMAND ----------

file_date =dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------



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

##Instead of .option("inferschema":True) we can use schema()
circuits_df = spark.read.csv(f"{raw_folder_path}/{file_date}/circuits.csv",header=True,schema=circuits_schema)
#circuits_df = spark.read.csv(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/circuits.csv",header=True,schema=circuits_schema)

#display(circuits_df)

#circuits_df.describe().show()
#circuits_df.printSchema()

# COMMAND ----------

circuits_df_sel=circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Renaming columns

# COMMAND ----------

from pyspark.sql.functions import lit
circuit_df_cols_rename=circuits_df_sel.withColumnRenamed('circuitId','circuit_id').withColumnRenamed('circuitRef','circuit_ref').withColumnRenamed('lat','latitude').withColumnRenamed('lng','longitude').withColumn('data_source',lit(data_sour)).withColumn('file_date',lit(file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding Timestamp column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
#circuit_df=circuit_df_cols_rename.withColumn('date_ingested',current_timestamp())
circuit_df=add_ingestion_date(circuit_df_cols_rename)


# COMMAND ----------

# MAGIC %md
# MAGIC ## writing dataframe as parquet 

# COMMAND ----------

container_name='processed'
#circuit_df.write.parquet(f"{processed_folder_path}/circuits",mode='overwrite')
circuit_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------



# COMMAND ----------

df=spark.read.parquet("dbfs:/user/hive/warehouse/f1_processed.db/circuits")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")
