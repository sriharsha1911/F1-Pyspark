# Databricks notebook source
dbutils.widgets.text('p_data_source',"")
data_source=dbutils.widgets.get('p_data_source')

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")

# COMMAND ----------

file_date =dbutils.widgets.get("p_file_date")

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
drivers_df=spark.read.json(f"{raw_folder_path}/{file_date}/drivers.json",schema=driver_schema)




# COMMAND ----------

# MAGIC %md 
# MAGIC ## Renaming cols and adding column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import concat,lit
drivers_df_addcol=drivers_df.withColumnRenamed('driverID','driver_id').withColumnRenamed('driverRef','driver_ref') \
.withColumn('date_ingested',current_timestamp()).withColumn('name',concat(drivers_df.name.forename,lit(' '),drivers_df.name.surname)) \
.withColumn('data_source',lit(data_source)).withColumn('file_date',lit(file_date)).drop('url')
#display(drivers_df_addcol)

# COMMAND ----------

container_name='processed'
#drivers_df_addcol.write.parquet(f"{processed_folder_path}/drivers",mode='overwrite')
#drivers_df_addcol.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.drivers")
drivers_df_addcol.write.mode('overwrite').format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

df=spark.read.format("delta").load("dbfs:/user/hive/warehouse/f1_processed.db/drivers")
display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed.drivers 
