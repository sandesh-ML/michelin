# Databricks notebook source
# %sql
# create database michelin

# COMMAND ----------

df_circuit = spark.read.csv("/Volumes/ws_micheline/michelin/db_volume/circuits.csv",inferSchema =True, header = True)

# COMMAND ----------

from pyspark.sql.functions import *

df_derv =df_circuit.withColumnsRenamed({"CircuitID":"Circuit_ID","Circuitref":"Circuit_ref","lat":"latitude","lng":"Longitude","alt":"Altitude"})\
.withColumn("ingestion_date",current_date())\
.drop("url")

# COMMAND ----------

df_derv.write.mode("overwrite").saveAsTable("michelin.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ws_micheline.michelin.circuits
