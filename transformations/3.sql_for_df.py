# Databricks notebook source
# MAGIC %md
# MAGIC - first we need to convert our df into view using createTempView/createOrReplaceTempView
# MAGIC - global view can be accessed by another notebooks 
# MAGIC   - createOrReplaceGlobalTempView 

# COMMAND ----------

# MAGIC %md
# MAGIC - managed tables
# MAGIC   - resides inside hive metastore 
# MAGIC   - commands :
# MAGIC       - create country.country_mt ( full syntax: create schema/database if not exists database_name)
# MAGIC       - insert data from another table 
# MAGIC         - create country.country_copy as select * from country_mt
# MAGIC       - df.write.saveAsTable("table_name") : save in current database of hive metastore 
# MAGIC       - to save at specified location in hive metastore database 
# MAGIC         - use database_name
# MAGIC         - df.write.saveAsTable("table_name")
# MAGIC       - manually create table using filesource  in database not suppported in community version
# MAGIC - external tables 
# MAGIC   - resides in adls  
# MAGIC   - in databricks we can 
# MAGIC   - to save as Table in external location 
# MAGIC     - path="dbfs:/FileStore/external/countries"
# MAGIC     - df.write.option("path",path).saveAsTable("countries.table_name") or we can give container path 
# MAGIC - diffe b/w managed and external: 
# MAGIC   - it will create table in both hive metastore and external location in databse countries the difference is just if 
# MAGIC     we drop table it will only delete from hive metastore but in external it will persists 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - permanent view in catalog 
# MAGIC   - catalog_name = "main"
# MAGIC   - database_name = "analytics"
# MAGIC   - view_name = "total_sales"
# MAGIC
# MAGIC  
# MAGIC   - create_view_query = f"""CREATE VIEW {catalog_name}.{database_name}.{view_name} AS SELECT product, SUM(sales_amount) AS total_sales FROM temp_sales GROUP BY product """
# MAGIC   - spark.sql(create_view_query)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - delta table 
# MAGIC   -  Save the DataFrame to ADLS in Delta format
# MAGIC   -  df.write.format("delta").mode("overwrite").save(adls_path)
# MAGIC
# MAGIC

# COMMAND ----------

now if i wnat to 
