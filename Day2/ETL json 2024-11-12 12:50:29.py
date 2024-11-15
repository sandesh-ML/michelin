# Databricks notebook source
# MAGIC %run /Workspace/include

# COMMAND ----------

df = spark.read.json("/Volumes/ws_micheline/michelin/db_volume/constructors.json")

# COMMAND ----------

df1 = add_ingestion_column(df)

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("constructor")
