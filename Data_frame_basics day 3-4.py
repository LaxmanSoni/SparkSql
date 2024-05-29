# Databricks notebook source
df = spark.read.csv("dbfs:/FileStore/shared_uploads/sonilaxman580@gmail.com/ratings.csv",header = True, schema = "userId INT, movieID int, rating double, timestamp int")

df

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

# change col
df.withColumn("newrating", col("rating")+10).show(5)

# COMMAND ----------

df.columns

# COMMAND ----------

#change col names 
df = df.withColumnRenamed("userId", "userId2")

df.show(2)

# COMMAND ----------

df.show(2)

# COMMAND ----------

df.filter(df["rating"]>4.0).show(3)

# COMMAND ----------

#another methods

df.filter(col("rating")>4.0).show(3)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

 from pyspark.sql import functions as f



# COMMAND ----------

df.withColumn("newtimestamp", f.from_unixtime("timestamp") ).show(2)

# COMMAND ----------

df2 = df.withColumnRenamed("timestamp","unixtimestamp")\
    .withColumn("timestamp", f.from_unixtime("unixtimestamp") )

df2.show(2)


# COMMAND ----------

df2.printSchema()


# COMMAND ----------

df2 = df.withColumnRenamed("timestamp","unixtimestamp")\
    .withColumn("strtimestamp", f.from_unixtime("unixtimestamp") )\
        .withColumn("timestamp", f.to_timestamp("strtimestamp"))\
            .drop( col("unixtimestamp"), col("strtimestamp"))

df2.show(2)


# COMMAND ----------

df.show()

# COMMAND ----------

# Q.1 filter out all the data who has movie id grater than 100 and also show the respective year 

# Q.2 try to filter out the data a rating 3 and wh has movie id grater than 70 



# COMMAND ----------

#Ans 1 
df.filter(col("movieID")>100).show(3)

# COMMAND ----------

#Ans 2

filtered_df = df.filter((df.rating == 3) & (df.movieID > 70))
filtered_df.show()


# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

df2 = df.withColumnRenamed("timestamp","unixtimestamp")\
    .withColumn("strtimestamp", f.from_unixtime("unixtimestamp") )\
        .withColumn("timestamp", f.to_timestamp("strtimestamp"))\
            .drop( col("unixtimestamp"), col("strtimestamp"))

df2.show(2)

# COMMAND ----------

#movie df 

movie_df = spark.read.csv("dbfs:/FileStore/shared_uploads/sonilaxman580@gmail.com/movies.csv", header = True, inferSchema = True)

movie_df.show()

# COMMAND ----------

movie_df.withColumn("GenreArray", f.split(f.col("genres"), "\|"))\
    .select("movieId", "title", "GenreArray")\
    .show(2, False)

# COMMAND ----------

movie_df.withColumn("GenreArray", f.split(f.col("genres"), "\|"))\
    .withColumn("genres", f.explode("GenreArray"))\
    .select("movieId", "title", "genres")\
    .show(6, False)

# COMMAND ----------

movie_df = movie_df.withColumn("GenreArray", f.split(f.col("genres"), "\|"))\
                   .withColumn("genres", f.explode("GenreArray"))\
                   .drop("GenreArray")


movie_df.show(6, False)

# COMMAND ----------

movie_df.show()

# COMMAND ----------


comedy_count = movie_df.filter(f.col("genres") == "Comedy").count()


print(f"Total number of comedy movies: {comedy_count}")

# COMMAND ----------


