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
    for column_name in df.schema.names:
        if column_name != col_name:
            new_lis.append(column_name)
    new_lis.append(col_name)
    return df.select(new_lis)
    

# COMMAND ----------

def incremental_load(dbname_tablename,df,partition_col):
    
    #list_of_cols=move_race_id_column(df,partition_col)    
    output_df=move_race_id_column(df,partition_col)
    
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{dbname_tablename}")):
        output_df.write.mode("overwrite").insertInto(f"{dbname_tablename}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_col).format("parquet").saveAsTable(f"{partition_col}")

        
