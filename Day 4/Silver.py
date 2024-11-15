# Databricks notebook source
# MAGIC %run "/Workspace/Michelin/Day 4/include"

# COMMAND ----------

# DBTITLE 1,PySpark
df=spark.table("michelin.bronze.sales")
df1=df.dropDuplicates().dropna()
df1.write.mode("overwrite").saveAsTable("michelin.silver.sales")

# COMMAND ----------

# DBTITLE 1,Spark SQL
# MAGIC %sql
# MAGIC -- create or replace table michelin.silver.sales as (select distinct order_id, customer_id,transaction_id,product_id,quantity,discount_amount,total_amount,order_date from michelin.bronze.sales where order_id is not null)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view products_bronze as (select * from michelin.bronze.products)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if NOT EXISTS michelin.silver.products (
# MAGIC   product_id INT,
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   product_price DOUBLE
# MAGIC   )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deduplicated_bp AS (
# MAGIC   SELECT
# MAGIC     bp.*,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY seqNum
# MAGIC DESC) AS row_num
# MAGIC   FROM
# MAGIC     products_bronze bp
# MAGIC )
# MAGIC MERGE INTO michelin.silver.products sp
# MAGIC USING (
# MAGIC   SELECT * FROM deduplicated_bp WHERE row_num = 1
# MAGIC ) bp
# MAGIC ON sp.product_id = bp.product_id 
# MAGIC WHEN MATCHED AND bp.operation = 'UPDATE'
# MAGIC THEN
# MAGIC   UPDATE SET
# MAGIC     product_name = bp.product_name,
# MAGIC     product_category = bp.product_category,
# MAGIC     product_price = bp.product_price
# MAGIC WHEN MATCHED AND bp.operation = 'DELETE'
# MAGIC THEN
# MAGIC   DELETE  
# MAGIC WHEN NOT MATCHED
# MAGIC THEN
# MAGIC   INSERT (
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     product_category,
# MAGIC     product_price
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     bp.product_id,
# MAGIC     bp.product_name,
# MAGIC     bp.product_category,
# MAGIC     bp.product_price
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE michelin.silver.customers (
# MAGIC   customer_id INT,
# MAGIC   customer_name STRING,
# MAGIC   customer_email STRING,
# MAGIC   customer_city STRING,
# MAGIC   customer_state STRING,
# MAGIC   operation STRING,
# MAGIC   sequenceNum INT,
# MAGIC   ingestion_date TIMESTAMP,
# MAGIC   source_path STRING,
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP,
# MAGIC   is_current BOOLEAN
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO michelin.silver.customers AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     customer_email,
# MAGIC     customer_city,
# MAGIC     customer_state,
# MAGIC     operation,
# MAGIC     sequenceNum,
# MAGIC     ingestion_date,
# MAGIC     source_path,
# MAGIC     current_timestamp() AS start_date,
# MAGIC     NULL AS end_date,
# MAGIC     TRUE AS is_current
# MAGIC   FROM  (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY sequenceNum ORDER BY sequenceNum asc) AS row_num
# MAGIC     FROM michelin.bronze.customers
# MAGIC   ) AS subquery
# MAGIC   WHERE row_num = 1  -- Keeps only the most recent record per customer_id
# MAGIC ) AS source
# MAGIC ON target.customer_id = source.customer_id AND target.is_current = TRUE
# MAGIC WHEN MATCHED AND (source.operation = 'UPDATE' or source.operation = 'DELETE') THEN
# MAGIC   UPDATE SET
# MAGIC     target.end_date = current_timestamp(),
# MAGIC     target.is_current = FALSE
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     customer_email,
# MAGIC     customer_city,
# MAGIC     customer_state,
# MAGIC     operation,
# MAGIC     sequenceNum,
# MAGIC     ingestion_date,
# MAGIC     source_path,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     is_current
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.customer_id,
# MAGIC     source.customer_name,
# MAGIC     source.customer_email,
# MAGIC     source.customer_city,
# MAGIC     source.customer_state,
# MAGIC     source.operation,
# MAGIC     source.sequenceNum,
# MAGIC     source.ingestion_date,
# MAGIC     source.source_path,
# MAGIC     source.start_date,
# MAGIC     source.end_date,
# MAGIC     source.is_current
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table michelin.silver.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.customers

# COMMAND ----------

# DBTITLE 1,First run
# MAGIC %sql
# MAGIC select * from silver.customers

# COMMAND ----------

# DBTITLE 1,second run
# MAGIC %sql
# MAGIC select * from silver.customers

# COMMAND ----------

# DBTITLE 1,third run
# MAGIC %sql
# MAGIC select * from silver.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY sequenceNum DESC) AS row_num
# MAGIC     FROM michelin.bronze.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY sequenceNum ORDER BY sequenceNum DESC) AS row_num
# MAGIC     FROM michelin.bronze.customers
