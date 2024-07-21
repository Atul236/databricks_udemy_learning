# Databricks notebook source
# MAGIC %md
# MAGIC - joins 
# MAGIC - grouping
# MAGIC - aggregare
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType

# COMMAND ----------

path_country="dbfs:/mnt/IDA_DEE_DEV/confidential/ida_dee_dev/ATUL/raw/Countries"
path_region="dbfs:/mnt/IDA_DEE_DEV/confidential/ida_dee_dev/ATUL/raw/Region"

# COMMAND ----------

schema_region=StructType([StructField("COUNTRY_ID",IntegerType(),False),
                   StructField("NAME",StringType(),False)])

# COMMAND ----------

schema_country=StructType([StructField("COUNTRY_ID",IntegerType(),False),
                   StructField("NAME",StringType(),False),
                   StructField("NATIONALITY",StringType(),False),
                   StructField("COUNTRY_CODE",StringType(),False),
                   StructField("ISO_ALPHA2",StringType(),False),
                   StructField("CAPITAL",StringType(),False),
                   StructField("POPULATION",IntegerType(),False),
                   StructField("AREA_KM2",IntegerType(),False),
                   StructField("REGION_ID",IntegerType(),False),
                   StructField("SUB_REGION_ID",IntegerType(),False),
                   StructField("INTERMEDIATE_REGION_ID",IntegerType(),False),
                   StructField("ORGANIZATION_REGION_ID",IntegerType(),False)
                   ])

# COMMAND ----------

country=spark.read.option('header',True) \
        .schema(schema_country)\
            .format("csv")\
                .load(path_country)

# COMMAND ----------

country.display()

# COMMAND ----------

region=spark.read.option('header',True) \
        .schema(schema_region)\
            .format("csv")\
                .load(path_region)

# COMMAND ----------

region.display()

# COMMAND ----------

# MAGIC %md
# MAGIC note here could could have only provided on="country_id " it would also have worked , it matchingh columns are different the we will have to use col 
# MAGIC

# COMMAND ----------

country.alias('C').join(region.alias("R"),on=col("C.COUNTRY_ID")==col('R.COUNTRY_ID'),how='inner')\
    .select("C.COUNTRY_ID","c.POPULATION","C.AREA_KM2","R.NAME")\
        .groupBy("R.NAME").agg(sum("C.POPULATION").alias("total_pop"),max("C.AREA_KM2").alias("max_area"))\
        .orderBy(col("total_pop").desc()).display()

# COMMAND ----------



# COMMAND ----------


