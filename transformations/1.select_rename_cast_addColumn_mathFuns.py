# Databricks notebook source
# MAGIC %md
# MAGIC **_what all will i learn _**
# MAGIC - create schema using StrucType and StructField
# MAGIC - renaming - withColumnRenamed
# MAGIC - add column - withColumn
# MAGIC - _change data type_ 
# MAGIC   - using df["col_name"].cast("int")
# MAGIC   - df.withColumn(COL("col_name").cast(IntegerType()))
# MAGIC - do select and casting together  
# MAGIC   - df.selectExpr(*["pupulation/1000000 as population_mi","name as country_name","region"])
# MAGIC - math function 
# MAGIC   - round: 
# MAGIC     - using withColumn :  df.withColumn("population_mi",round(df["population"]/1000000,2))
# MAGIC     - using selectExpr :  df.selectExpr(*["round(population/1000000,2),"name as country_name","to_date(DOB,"yy-mm-dd") as date_of_birth]")
# MAGIC - sorting :
# MAGIC   - asc
# MAGIC   - desc
# MAGIC   - asc_null_first
# MAGIC   - asc_null_last
# MAGIC   - desc_null_first
# MAGIC   - desc_null_last
# MAGIC
# MAGIC - string functions 
# MAGIC   - upper
# MAGIC   - initcap
# MAGIC   - length
# MAGIC   - concat_ws
# MAGIC
# MAGIC - date functions 
# MAGIC   - current_date()
# MAGIC   - current_date()
# MAGIC   - current_timestamp()
# MAGIC   - dayofmonth()
# MAGIC   - dayofweek()
# MAGIC   - date_diff()
# MAGIC   - to_date()
# MAGIC   - date_add()
# MAGIC   - date_trunc()
# MAGIC - filter 
# MAGIC - conditionals 
# MAGIC - window functions()
# MAGIC   - partitionBy() with window functions
# MAGIC   - orderBy() with partition and window funs 
# MAGIC
# MAGIC

# COMMAND ----------

path="dbfs:/mnt/IDA_DEE_DEV/confidential/ida_dee_dev/ATUL/raw/Countries"

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType

# COMMAND ----------

from pyspark.sql.functions import col,current_date,round

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

# df_renamed.display()

# COMMAND ----------

# MAGIC %md
# MAGIC same thing using selectExpr with ROUNDING 

# COMMAND ----------

COLS=['COUNTRY_ID',
 'NAME as COUNTRY_NAME',
 'COUNTRY_CODE',
 'round(POPULATION/1000000,2) AS POPULATION_IN_MILLION',
 'REGION_ID']

# COMMAND ----------

df_expr=country.selectExpr(*COLS)\
    .withColumn("INGESTION_DATE",current_date())

# COMMAND ----------

df_expr.display()

# COMMAND ----------

# MAGIC %md
# MAGIC filter 

# COMMAND ----------

df_expr.filter("POPULATION_IN_MILLION >= 25 and REGION_ID = 10").display()

# COMMAND ----------

# MAGIC %md
# MAGIC sorting 

# COMMAND ----------

df_asc=df_expr.sort(col('POPULATION_IN_MILLION').asc())
df_desc=df_expr.sort(col('POPULATION_IN_MILLION').desc())
df_asc_null_first=df_expr.sort(col('POPULATION_IN_MILLION').asc_nulls_first())


# COMMAND ----------

# df_desc.display()

# COMMAND ----------

# MAGIC %md
# MAGIC conditionals :
# MAGIC - using selectEXpr :
# MAGIC   df3 = df.withColumn("new_gender", expr("CASE WHEN gender = 'M' THEN 'Male' " + 
# MAGIC                "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
# MAGIC                "ELSE gender END"))
# MAGIC   - note here + is used to concat string only becoz they are written in multilines
# MAGIC - using when : 
# MAGIC   df.select(col("*"),when(df.gender == "M","Male")
# MAGIC                   .when(df.gender == "F","Female")
# MAGIC                   .when(df.gender.isNull() ,"")
# MAGIC                   .otherwise(df.gender).alias("new_gender"))

# COMMAND ----------

# MAGIC %md
# MAGIC string functions 

# COMMAND ----------

from pyspark.sql.functions import upper,concat_ws,length,initcap

# COMMAND ----------

df_str_fun=df_expr.selectExpr(*["upper(COUNTRY_NAME) as COUNTRY_NAME_UPPER","concat_ws(' ',COUNTRY_NAME,COUNTRY_CODE) as country_name_code","length(COUNTRY_NAME) as name_length","initcap(concat_ws(' ',COUNTRY_NAME,COUNTRY_CODE)) as country_name_code"])


# COMMAND ----------

# MAGIC %md
# MAGIC Date_time functions 
# MAGIC - current_date()
# MAGIC - current_timestamp()
# MAGIC - dayofmonth()
# MAGIC - dayofweek()
# MAGIC - date_diff()
# MAGIC - to_date()
# MAGIC - date_add()
# MAGIC - date_trunc()

# COMMAND ----------

df_date_fun=df_expr.selectExpr("*",*["current_date() as date","year(current_date()) as year",
                                     "current_timestamp() as INGESTION_STAMP",
                                     "date_trunc('month',INGESTION_STAMP) as trunc_date"],
                               )

# COMMAND ----------

# df_date_fun.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_date_fun.selectExpr("date_trunc('month',INGESTION_STAMP) as trunc_date","date_add(trunc_date,-1) as previous_date","add_months(INGESTION_STAMP,2) as added_month","dateDiff(added_month,previous_date) as date_difference").display()

# COMMAND ----------

# MAGIC %md
# MAGIC WindowFunctions
# MAGIC - row_number() : alwaYs consecutive 
# MAGIC - rank() : when two are saME gives same rank , and skips next 
# MAGIC - dense_rank() : doesnt skip next one 

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------


df_rnk=df_expr.selectExpr("COUNTRY_NAME","REGION_ID","POPULATION_IN_MILLION",
                   "row_number() over (PARTITION BY REGION_ID ORDER BY POPULATION_IN_MILLION desc) as rnk",
                   "dense_rank() over (PARTITION BY REGION_ID ORDER BY POPULATION_IN_MILLION desc) as d_rnk"
                   )

# COMMAND ----------

df_rnk.filter("rnk between 1 and 2").display()

# COMMAND ----------

# MAGIC %md
# MAGIC hashing 
# MAGIC - md5()

# COMMAND ----------

col_names=df_expr.columns

# COMMAND ----------

expr=f"array_join(array({','.join(col_names)}),'|')"

# COMMAND ----------

df_expr.selectExpr(f"md5({expr}) as hash_value").display()

# COMMAND ----------


