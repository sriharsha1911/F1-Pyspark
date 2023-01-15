# Databricks notebook source
dbutils.widgets.text('p_data_source',"")
data_source=dbutils.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------



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
drivers_df=spark.read.json(f"{raw_folder_path}/drivers.json",schema=driver_schema)




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
drivers_df_addcol.write.parquet(f"{processed_folder_path}/drivers",mode='overwrite')

# COMMAND ----------

df=spark.read.parquet(f"{processed_folder_path}/drivers")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
