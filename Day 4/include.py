# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path= "dbfs:/mnt/ssmtorage/raw/project/"

# COMMAND ----------

def add_ingestion_col(df):
  df_final=df.withColumn("ingestion_date",current_timestamp())
  df_final=df_final.withColumn("source_path",input_file_name())
  return df_final

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog michelin;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists michelin.bronze;
# MAGIC create schema if not exists michelin.silver;
# MAGIC create schema if not exists michelin.gold;