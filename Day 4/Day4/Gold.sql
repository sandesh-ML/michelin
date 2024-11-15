-- Databricks notebook source
create or replace view michelin.gold.customer_total_sales as (select customer_id,customer_name, round(sum(total_amount)) as total_amount from michelin.silver.sales_customer group by all )

-- COMMAND ----------

create or replace view michelin.gold.total_sale as (select round(sum(total_amount)) as total_sales from michelin.silver.sales)

-- COMMAND ----------

select * from michelin.gold.customer_total_sales
