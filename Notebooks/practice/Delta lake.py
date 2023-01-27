# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_demo

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",f"{storage_account_key}"
)

# COMMAND ----------


results_df=spark.read.json(f"{raw_folder_path}/2021-03-28/results.json")

# COMMAND ----------


results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("abfss://demo@f1sa.dfs.core.windows.net/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create table f1_demo.results_exeternal
# MAGIC using delta
# MAGIC location "abfss://demo@f1sa.dfs.core.windows.net/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  f1_demo.results_exeternal

# COMMAND ----------

# MAGIC %md
# MAGIC  reading from delta file

# COMMAND ----------

results_external_df=spark.read.format("delta").load("abfss://demo@f1sa.dfs.core.windows.net/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC partitionBy

# COMMAND ----------

results_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert sql

# COMMAND ----------


drivers_day1_df = spark.read \
.json("abfss://raw@f1sa.dfs.core.windows.net/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

display(drivers_day1_df)
drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read \
.json("abfss://raw@f1sa.dfs.core.windows.net/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
drivers_day2_df.createOrReplaceTempView("drivers_day2")
display(drivers_day2_df)


# COMMAND ----------

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("abfss://raw@f1sa.dfs.core.windows.net/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
display(drivers_day3_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drivers_day2;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC  surname STRING,
# MAGIC  createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob = upd.dob,
# MAGIC tgt.forename = upd.forename,
# MAGIC tgt.surname = upd.surname,
# MAGIC  tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob = upd.dob,
# MAGIC tgt.forename = upd.forename,
# MAGIC tgt.surname = upd.surname,
# MAGIC tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC desc  view extended f1_demo.drivers_merge

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/f1_demo.db/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC * History & Versioning
# MAGIC * Time Travel
# MAGIC * Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history  f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  f1_demo.drivers_merge version as of 2
# MAGIC --selet * from  f1_demo.drivers_merge timestamp as of ''

# COMMAND ----------

df=spark.read.format("delta").option("versionAsOf",2).load('dbfs:/user/hive/warehouse/f1_demo.db/drivers_merge')
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC spark.databricks.delta.retentionDurationCheck.enabled = false
# MAGIC vacuum f1_demo.drivers_merge retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from  f1_demo.drivers_merge where driverId=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge
# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge version as of 4

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using f1_demo.drivers_merge version as of 3 as src
# MAGIC on (tgt.driverId=src.driverId)
# MAGIC when not matched then
# MAGIC   Insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_tran (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC  surname STRING,
# MAGIC  createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

fs
ls 'dbfs:/user/hive/warehouse/f1_demo.db/drivers_convert_to_delta_new'

# COMMAND ----------

#delta tables has delta log folder which contains transaction files as json for each version and these in turn contain which parquet to read

# COMMAND ----------

file_path = "dbfs:/user/hive/warehouse/f1_demo.db/drivers_tran/_delta_log/00000000000000000004.json"
content = spark.read.json(file_path).collect()
for row in content:
    print(row[0])


# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into f1_demo.drivers_tran
# MAGIC select * from f1_demo.drivers_merge where driverId=2
# MAGIC delete from f1_demo.drivers_merge where driverId not in (1,2)

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_merge where driverId=2

# COMMAND ----------

# MAGIC %md
# MAGIC Convert parquet to delta

# COMMAND ----------

# MAGIC  %sql
# MAGIC  CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC  driverId INT,
# MAGIC  dob DATE,
# MAGIC  forename STRING, 
# MAGIC  surname STRING,
# MAGIC  createdDate DATE, 
# MAGIC  updatedDate DATE
# MAGIC  )
# MAGIC  USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_convert_to_delta
# MAGIC select * from f1_demo.drivers_merge 

# COMMAND ----------

# MAGIC %sql
# MAGIC  CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")
display(df)
#df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("dbfs:/user/hive/warehouse/f1_demo.db/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC  %sql
# MAGIC  CONVERT TO DELTA parquet.`dbfs:/user/hive/warehouse/f1_demo.db/drivers_convert_to_delta_new`
