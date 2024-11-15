# Databricks notebook source
# MAGIC %run  "/Workspace/Michelin/Day 4/include"

# COMMAND ----------

# DBTITLE 1,Sales Bronze
df_sales=spark.read.csv(f"{input_path}sales.csv",header=True,inferSchema=True)
df_sales_final=add_ingestion_col(df_sales)
df_sales_final.write.mode("overwrite").saveAsTable("bronze.sales")

# COMMAND ----------

df=spark.read.csv(f"{input_path}{dbutils.widgets.get('source')}.csv",header=True,inferSchema=True)
df_final=add_ingestion_col(df)
df_final.write.mode("overwrite").saveAsTable(f"bronze.{dbutils.widgets.get('source')}")

# COMMAND ----------


