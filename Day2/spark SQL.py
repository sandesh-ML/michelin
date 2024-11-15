# Databricks notebook source
# MAGIC %md
# MAGIC In spark SQL CTAS is works when there is single line json or parquet but for csv it wont work 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table michelin.constructor_sql as
# MAGIC select * from json.`/Volumes/ws_micheline/michelin/db_volume/constructors.json`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table michelin.contructor_csv
# MAGIC select * from csv.`/Volumes/ws_micheline/michelin/db_volume/circuits.csv`
