# Databricks notebook source
path="dbfs:/mnt/IDA_DEE_DEV/confidential/ida_dee_dev/ATUL/raw/Employee"

# COMMAND ----------

df= spark.read.option("header",True)\
        .option("inferSchema",True)\
            .format("csv")\
                .load(path)

# COMMAND ----------

df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("dbfs:/mnt/IDA_DEE_DEV/confidential/ida_dee_dev/ATUL/euh/Emp_table")

# COMMAND ----------

# MAGIC %md
# MAGIC - standard approad to work with data lake tables but we can symply perform operation using dataframe apis by reading as delta format  

# COMMAND ----------

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# COMMAND ----------

spark = SparkSession.builder.appName("UpdateDeleteDeltaTableExample").getOrCreate()

# COMMAND ----------

adls_path="dbfs:/mnt/IDA_DEE_DEV/confidential/ida_dee_dev/ATUL/euh/Emp_table"

# COMMAND ----------

# delta_table = DeltaTable.forPath(spark, adls_path)

# COMMAND ----------

# delta_table.delete(condition="EMPLOYEE_ID >150")

# COMMAND ----------

# MAGIC %md
# MAGIC we cant directly perform query over data lake tables but we can perform over delta lake tables only if it is registered as table in unity  catalog or hive metastore
# MAGIC - we can perform delete update merge in data lake tables 
# MAGIC

# COMMAND ----------

# updated_delta=spark.read.format("delta").load(adls_path)

# COMMAND ----------

# updated_delta.display()

# COMMAND ----------

# MAGIC %md
# MAGIC - upsert 

# COMMAND ----------

# MAGIC %md
# MAGIC - we have already done the full load ()
# MAGIC - now lets do the change in few records and take as source with update and insert 

# COMMAND ----------

df_filtered=df.filter("EMPLOYEE_ID <111")

# COMMAND ----------

source_df=df_filtered.withColumn("SALARY",expr("CASE WHEN EMPLOYEE_ID >105 THEN SALARY+1234 ELSE SALARY END "))\
    .withColumn("EMPLOYEE_ID",expr("CASE WHEN EMPLOYEE_ID<106 THEN EMPLOYEE_ID+500 ELSE EMPLOYEE_ID END "))

# COMMAND ----------

source_df.display()

# COMMAND ----------

source_df.count()

# COMMAND ----------

from delta.tables import DeltaTable
target_path="dbfs:/mnt/IDA_DEE_DEV/confidential/ida_dee_dev/ATUL/euh/Emp_table"

# Create a DeltaTable object for the target table
delta_table = DeltaTable.forPath(spark, target_path)

# COMMAND ----------

columns = source_df.columns
set_clause = {col: f"source.{col}" for col in columns}

# Dynamically generate the 'values' clause for whenNotMatchedInsert
values_clause = {col: f"source.{col}" for col in columns}

# COMMAND ----------

delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.EMPLOYEE_ID = source.EMPLOYEE_ID"  
).whenMatchedUpdate(set=set_clause
).whenNotMatchedInsert(values=values_clause
).execute()


# COMMAND ----------

merged_delta=spark.read.format("delta").load(target_path)

# COMMAND ----------

merged_delta.display()

# COMMAND ----------

# target 107 
# inserts 6 
# total 113

# COMMAND ----------

# task :  with condition string 
# from pyspark.sql import SparkSession
# from delta.tables import DeltaTable

# columns = ["product", "category", "sales_amount", "quantity"]



# df_source = spark.createDataFrame(data_source, columns)

# 1. Create a DeltaTable object for the target table
# delta_table = DeltaTable.forPath(spark, adls_path)

# columns = df_source.columns

# 2. Dynamically generate the 'set' clause for whenMatchedUpdate as a dictionary
# set_clause = {col: f"source.{col}" for col in columns}

# 3. Dynamically generate the 'values' clause for whenNotMatchedInsert as a dictionary
# values_clause = {col: f"source.{col}" for col in columns}

#  4. Dynamically generate the condition for whenMatchedUpdate
# conditions = [f"target.{col} != source.{col}" for col in columns if col != 'product']  # Exclude the primary key
# condition_str = " OR ".join(conditions)

# 5. Perform the merge operation with the dynamically generated condition
# delta_table.alias("target").merge(
#     df_source.alias("source"),
#     "target.product = source.product"      note : Assuming 'product' is the primary key
# ).whenMatchedUpdate(
#     condition=condition_str,
#     set=set_clause
# ).whenNotMatchedInsert(values=values_clause
# ).execute()


