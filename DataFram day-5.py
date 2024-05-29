# Databricks notebook source
#data types or structure


# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType,ArrayType,BooleanType,DateType,FloatType, StringType

# COMMAND ----------

schemaJson=StructType([
  StructField("id", IntegerType(), True),
  StructField("first_name", StringType(), True),
  StructField("last_name", StringType(), True),
  StructField("fav_movies", ArrayType(StringType()), True),
  StructField("salary", FloatType(), True),
  StructField("image_url", StringType(), True),
  StructField("date_of_birth", DateType(), True),
  StructField("active", BooleanType(), True),

])



# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/shared_uploads/sonilaxman580@gmail.com/persons.json", schema=schemaJson ,multiLine= True)

df.show(2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

df.select("id").show(4)

# COMMAND ----------

df.select(df["id"]).show(1)

# COMMAND ----------

df.select(f.expr('id')).show(3)

# COMMAND ----------

df.select(f.col("id")).show(2)

# COMMAND ----------

df.select(f.col("first_name"),f.concat_ws(' ',f.col("first_name"),f.col("last_name"))).show()

# COMMAND ----------

df.select(f.col("first_name"),f.concat_ws(' ',f.col("first_name"),f.col("last_name")).alias("fullname"))\
    .show()

# COMMAND ----------

df.select(f.col("first_name"),f.concat_ws(' ',f.col("first_name"),f.col("last_name")).alias("fullname"),
          f.col("salary"),
          (f.col("salary")*1.25).alias("newSalary"))\
    .show()

# COMMAND ----------


