-- Databricks notebook source
create table michelin.emp(id int,name string)

-- COMMAND ----------

describe extended michelin.emp

-- COMMAND ----------

insert into michelin.emp values(1,'sandesh')

-- COMMAND ----------

describe detail michelin.emp

-- COMMAND ----------

describe history michelin.emp

-- COMMAND ----------

insert into michelin.emp values(2,'a');
insert into michelin.emp values(121,'ab');
insert into michelin.emp values(122,'avfv');
insert into michelin.emp values(123,'aaf');
insert into michelin.emp values(124,'awgt');
insert into michelin.emp values(129,'aww');

-- COMMAND ----------

select * from michelin.emp

-- COMMAND ----------

describe michelin.emp

-- COMMAND ----------

describe extended michelin.emp

-- COMMAND ----------

describe history michelin.emp

-- COMMAND ----------

describe detail michelin.emp

-- COMMAND ----------

optimize michelin.emp

-- COMMAND ----------

describe detail michelin.emp

-- COMMAND ----------

vacuum michelin.emp

-- COMMAND ----------

create view michelin.circuits_country as
(select country,count(country) as count from michelin.circuits group by country order by count desc)

-- COMMAND ----------

create or replace temp view circuits_country_tmp as
(select country,count(country) as count from michelin.circuits group by country order by count desc)

-- COMMAND ----------

show views in global_temp

-- COMMAND ----------

create or replace global temp view global_circuits_country_tmp as
(select country,count(country) as count from michelin.circuits group by country order by count desc)

-- COMMAND ----------

select * from global_circuits_country_tmp

-- COMMAND ----------

select *,michelin.voter_eligible(id) eligible from michelin.emp

-- COMMAND ----------

create function michelin.voter_eligible(age int)
returns string
return case when age >= 18 then 'eligible' else 'not eligible' end
