# Databricks notebook source
#read csv file
df = spark.read.csv("dbfs:/FileStore/shared_uploads/sonilaxman580@gmail.com/ratings.csv",header = True, inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("userId"), col("timestamp")).show(5)

# COMMAND ----------

df.select(df["userId"], df["timestamp"]).show()
