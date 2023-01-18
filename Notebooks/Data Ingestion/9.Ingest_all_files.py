# Databricks notebook source
result=dbutils.notebook.run("1.Circuits_ingest",1000,{"p_data_source":'Ergast API'})

# COMMAND ----------

result=dbutils.notebook.run("2.Races_ingest",0,{"p_data_source":'Ergast API'})

# COMMAND ----------

result=dbutils.notebook.run("3.Constructors_ingest",0,{"p_data_source":'Ergast API'})

# COMMAND ----------

result=dbutils.notebook.run("4.Drivers_ingest",0,{"p_data_source":'Ergast API'})

# COMMAND ----------

result=dbutils.notebook.run("5.Results_ingest",0,{"p_data_source":'Ergast API'})

# COMMAND ----------

result=dbutils.notebook.run("6.Pitstops_ingest",0,{"p_data_source":'Ergast API'})

# COMMAND ----------

result=dbutils.notebook.run("7.Laptimes_ingest",0,{"p_data_source":'Ergast API'})

# COMMAND ----------

result=dbutils.notebook.run("8.qualifying_ingest",0,{"p_data_source":'Ergast API'})
