# Databricks notebook source
path="dbfs:/mnt/IDA_DEE_DEV/confidential/ida_dee_dev/ATUL/raw/Countries"

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType

# COMMAND ----------

from pyspark.sql.functions import col,current_date,lit

# COMMAND ----------

schema=StructType([StructField("COUNTRY_ID",IntegerType(),False),
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
        .schema(schema)\
            .format("csv")\
                .load(path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### select,rename and casting

# COMMAND ----------

df=country.select("COUNTRY_ID",'NAME','COUNTRY_CODE','POPULATION','REGION_ID')


# COMMAND ----------

df_renamed=df.withColumnRenamed("NAME","COUNTRY_NAME")\
    .withColumnRenamed("POPULATION","POPULATION_IN_MILLION")\
        .withColumn("POPULATION_IN_MILLION",col("POPULATION_IN_MILLION").cast(DoubleType()))\
        .withColumn("POPULATION_IN_MILLION",(col("POPULATION_IN_MILLION")/1000000))\
        .withColumn("ingestion_date",current_date())

# COMMAND ----------

df_renamed.display()

# COMMAND ----------

# MAGIC %md
# MAGIC same thing using selectExpr

# COMMAND ----------

COLS=['COUNTRY_ID',
 'NAME as COUNTRY_NAME',
 'COUNTRY_CODE',
 'POPULATION/1000000 AS POPULATION_IN_MILLION',
 'REGION_ID']

# COMMAND ----------

df_expr=country.selectExpr(*COLS)\
    .withColumn("INGESTION_DATE",current_date())

# COMMAND ----------

df_expr.display()
