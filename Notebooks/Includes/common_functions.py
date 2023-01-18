# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df=input_df.withColumn("date_ingested",current_timestamp())
    return output_df

# COMMAND ----------

# MAGIC %md
# MAGIC move race_id column to last

# COMMAND ----------


def move_race_id_column(df,col_name):
    new_lis=[]
    list_of_cols=df.schema.names
    list_of_cols.remove(col_name)
    list_of_cols.append(col_name)
    return list_of_cols
    

# COMMAND ----------


    

# COMMAND ----------

def incremental_load(dbname_tablename,df,partition_col):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if(spark._jsparkSession.catalog().tableExists("{}".format(dbname_tablename))):
        df.write.mode('overwrite').insertInto("{}".format(dbname_tablename))
    else:
        df.write.mode('append').partitionBy("{}".format(partition_col)).format("parquet").saveAsTable("{}".format(dbname_tablename))

