# Databricks notebook source
# MAGIC %sql
# MAGIC create database michelin

# COMMAND ----------

df_circuit = spark.read.csv("/Volumes/ws_micheline/michelin/db_volume/circuits.csv",inferSchema =True, header = True)

# COMMAND ----------

df_circuit.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.functions import col
df_circuit.select(col('circuitId').alias('circuit Id'),'circuitRef',).display()

# COMMAND ----------

df_circuit.withColumnRenamed("circuitid","circuit_id")

# COMMAND ----------

df_circuit.withColumnsRenamed({
"circuitId"
:
"circuit_id"
,
"circuitRef"
:
"circuit_ref"
})

# COMMAND ----------

# DBTITLE 1,New column
df_circuit.withColumn("ingestion_date",current_date).display

# COMMAND ----------

# DBTITLE 1,Replace existing column
df_circuit.withColumn(
"country"
,upper(
"country"
)).display()
