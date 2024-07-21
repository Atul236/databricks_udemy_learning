# Databricks notebook source
path="dbfs:/mnt/IDA_DEE_DEV/confidential/ida_dee_dev/ATUL/raw/Orders"

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DoubleType

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

store=spark.read.option('inferSchema',True).option('header',True).csv(f"{path}/stores.csv")

# COMMAND ----------

orders=spark.read.option('inferSchema',True).option('header',True).csv(f"{path}/orders.csv")

# COMMAND ----------

orders.display()

# COMMAND ----------

date_format="dd-MMM-yy HH.mm.ss.SS"

# COMMAND ----------

df_joined=orders.alias("o")\
        .join(store.alias("s"),on='STORE_ID',how="inner")\
        .select("o.ORDER_ID","o.CUSTOMER_ID","s.STORE_NAME","o.ORDER_DATETIME")\
            .filter("o.ORDER_STATUS='COMPLETE'")\
                .withColumn('ORDER_DATETIME',to_timestamp('ORDER_DATETIME',date_format))

# COMMAND ----------



# COMMAND ----------

item_schema=StructType([StructField("ORDER_ID",IntegerType(),False),
                   StructField("LINE_ITEM_ID",IntegerType(),False),
                   StructField("PRODUCT_ID",IntegerType(),False),
                   StructField("UNIT_PRICE",DoubleType(),False),
                   StructField("QUANTITY",IntegerType(),False)
])

# COMMAND ----------

item=spark.read.schema(item_schema).option('header',True).csv(f"{path}/order_items.csv")

# COMMAND ----------

product_schema=StructType([StructField("PRODUCT_ID",IntegerType(),False),
                   StructField("PRODUCT_NAME",StringType(),False),
                   StructField("UNIT_PRICE",DoubleType(),False)
                   ])

# COMMAND ----------

product=spark.read.schema(product_schema).option('header',True).csv(f"{path}/products.csv")

# COMMAND ----------

product.display()

# COMMAND ----------

customer_schema=StructType([StructField("CUSTOMER_ID",IntegerType(),False),
                   StructField("CUSTOMER_NAME",StringType(),False),
                   StructField("EMAIL_ADDRESS",StringType(),False)
                   ])

# COMMAND ----------

customer=spark.read.schema(customer_schema).option('header',True).csv(f"{path}/customers.csv")

# COMMAND ----------

customer.display()

# COMMAND ----------

# MAGIC %md
# MAGIC S2G

# COMMAND ----------

# MAGIC %md
# MAGIC assignment : 1 order_details

# COMMAND ----------

order_alias=orders.alias("o")
item_alias=item.alias("i")
store_alias=store.alias("s")

# COMMAND ----------

ois=order_alias.join(item_alias,on="ORDER_ID",how="inner")\
    .join(store_alias,on="STORE_ID",how="left")\
        .select(col("o.ORDER_ID"),col("o.ORDER_DATETIME"),col("i.PRODUCT_ID"),col("o.CUSTOMER_ID"),col("i.UNIT_PRICE"),col("i.QUANTITY"),col("s.STORE_NAME"))\
            .withColumn("AMOUNT",round(col("i.QUANTITY")*col("i.UNIT_PRICE"),2))\
                .withColumn("ORDER_DATETIME",to_date(col("o.ORDER_DATETIME"),"dd-MMM-yy HH.mm.ss.SS"))\
                .groupBy(col("o.ORDER_ID"),col("ORDER_DATETIME"),col("o.CUSTOMER_ID"),col("s.STORE_NAME"))\
                    .agg(round(sum(col("AMOUNT")),2).alias("total_amount"))

# COMMAND ----------

ois.select("STORE_NAME","ORDER_DATETIME","total_amount")\
    .withColumn("MONTH",month("ORDER_DATETIME"))\
    .groupBy("STORE_NAME","MONTH")\
        .agg(sum(col("total_amount")).alias("total"))\
            .orderBy(col("total"))\
    .display()

# COMMAND ----------


