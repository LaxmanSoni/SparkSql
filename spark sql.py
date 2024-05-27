# Databricks notebook source
# spark and sql 

# COMMAND ----------

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

  

# COMMAND ----------

# Spark Sql 
# data scienst , analyts => interactive sql queries 

# spark core := 


# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]


# COMMAND ----------

df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType,IntegerType

# COMMAND ----------

schema1 = StructType([StructField("firstName", StringType(), True),
            StructField("middaleName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("Gender", StringType(), True),
            StructField("idperson", StringType(), True),
            StructField("Salary", IntegerType(), True)])

# COMMAND ----------

type(schema1)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = spark.createDataFrame(data, schema=schema1)
df.show()

# COMMAND ----------

df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("tusar").getOrCreate()

# COMMAND ----------

data = [("tusar", "goyal", 100),
        ("aman","kumar", 200)]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------



# COMMAND ----------


