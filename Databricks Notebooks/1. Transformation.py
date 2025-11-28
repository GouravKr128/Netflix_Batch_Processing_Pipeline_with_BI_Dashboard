# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a Catalog and Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS netflix_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS netflix_catalog.netflix_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("/Volumes/netflix_catalog/netflix_schema/data/netflix_titles.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

display(spark.createDataFrame([(df.count(), len(df.columns))], ["total_rows", "total_columns"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Duplicates

# COMMAND ----------

# Duplicate records
df.groupBy(df.columns).agg(count("*").alias("count")).filter(col("count") > 1).display()

# COMMAND ----------

df = df.dropDuplicates()

# COMMAND ----------

# Duplicate records
df.groupBy(df.columns).agg(count("*").alias("count")).filter(col("count") > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling null values

# COMMAND ----------

def find_null(df):
    l=[]
    from builtins import round
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        null_percent = round((null_count/df.count())*100,2)
        l.append([column, null_count, null_percent])

    display(spark.createDataFrame(l, ["column", "null_count", "null_%"]))

# COMMAND ----------

find_null(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **How null values are handled?**<br>
# MAGIC show_id -> drop record<br>
# MAGIC title -> drop record<br>
# MAGIC date added -> drop record <br>
# MAGIC release year -> drop record <br>
# MAGIC director -> replace null by "NA" <br>
# MAGIC cast -> replace null by "NA" <br>
# MAGIC listed_in -> replace null by "NA" <br>
# MAGIC description -> replace null by "NA" <br>
# MAGIC duration -> replace null by "NA" <br>
# MAGIC type -> replace null by mode <br>
# MAGIC country -> replace null by mode <br>
# MAGIC rating -> replace null by mode

# COMMAND ----------

# Dropping the entire row if show_id, title, date_added, release_year is null
df = df.dropna(subset=["show_id", "title", "date_added", "release_year"])

# COMMAND ----------

# Replacing null values in director, cast, listed_in, description, duration by "NA"
df = df.fillna({"director": "NA", "cast": "NA", "listed_in": "NA", "description": "NA", "duration": "NA"})

# COMMAND ----------

# Replacing null values in type, country, rating by most frequent value
mode_dict = {}
for col_name in ["type", "country", "rating"]:
    mode_value = df.groupBy(col_name).count().orderBy(desc("count")).first()[0]
    mode_dict[col_name] = mode_value

df = df.fillna(mode_dict)

# COMMAND ----------

find_null(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Analysis

# COMMAND ----------

# Function to find value counts
def value_count(df, column):
    df.groupBy(column).count().orderBy(desc("count")).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Column -> Show_ID

# COMMAND ----------

# Find rows where show_id does not start with "s" or does not end with an integer
pattern = r"^s.*\d$"
df.filter(~col("show_id").rlike(pattern)).display()

# Regular expression explanation:
# ^s      : start of string, followed by 's'
# .*      : any characters (zero or more)
# \d$     : ends with a digit

# COMMAND ----------

# MAGIC %md
# MAGIC Column -> Type

# COMMAND ----------

value_count(df, "type")

# COMMAND ----------

# MAGIC %md
# MAGIC Column -> title

# COMMAND ----------

df = df.withColumn("title", trim(col("title")))

# COMMAND ----------

# MAGIC %md
# MAGIC Column -> Country

# COMMAND ----------

value_count(df, "country")

# COMMAND ----------

df = df.withColumn("country", split("country", ","))
df.display()

# COMMAND ----------

df = df.withColumn("country", transform("country", lambda x: trim(x)))
df.display()

# COMMAND ----------

df = df.withColumn("country", concat_ws(",", "country"))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Column -> Director

# COMMAND ----------

df = df.withColumn("director", split("director", ","))
df = df.withColumn("director", transform("director", lambda x: trim(x)))
df = df.withColumn("director", concat_ws(",", "director"))
df.display()

# COMMAND ----------

df.filter(col("country").startswith(" ") | col("country").endswith(" ")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Column -> Date Added

# COMMAND ----------

value_count(df,"date_added")

# COMMAND ----------

df.filter(col("date_added").startswith(" ") | col("date_added").endswith(" ")).display()

# COMMAND ----------

df = df.withColumn("date_added", trim(col("date_added")))
df.display()

# COMMAND ----------

# Finding rows in 'date_added' column that do not match the expected date format "MMMM d, yyyy"
# The regex checks for a full month name, a space, 1-2 digit day, comma, space, and 4 digit year

pattern = r"^(January|February|March|April|May|June|July|August|September|October|November|December) [1-9]{1}[0-9]?, \d{4}$"
df.filter(~col("date_added").rlike(pattern)).display()

# COMMAND ----------

df = df.filter(col("date_added").rlike(pattern))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Column -> release_year

# COMMAND ----------

df = df.withColumn("release_year", trim(col("release_year")))

# COMMAND ----------

value_count(df,"release_year")

# COMMAND ----------

# MAGIC %md
# MAGIC Column -> rating

# COMMAND ----------

df = df.withColumn("rating", trim(col("rating")))

# COMMAND ----------

value_count(df,"rating")

# COMMAND ----------

df.filter(col("rating").contains("min")).display()

# COMMAND ----------

swap_show_ids = ["s5814", "s5542", "s5795"]  

df = df.withColumn("duration",when(col("show_id").isin(swap_show_ids), col("rating")).otherwise(col("duration"))) \
      .withColumn("rating", when(col("show_id").isin(swap_show_ids), "NA").otherwise(col("rating")))

# COMMAND ----------

df.filter(col("rating").contains("NA")).display()

# COMMAND ----------

# Calculate mode of 'rating' excluding 'NA'
mode_rating = df.filter(col("rating") != "NA") \
    .groupBy("rating") \
    .count() \
    .orderBy(desc("count")) \
    .first()["rating"]

# Replace 'NA' in 'rating' with the mode
df = df.withColumn("rating", when(col("rating") == "NA", mode_rating).otherwise(col("rating")))

# COMMAND ----------

df.filter(col("show_id").isin(["s5814", "s5542", "s5795"])).display()

# COMMAND ----------

value_count(df,"rating")

# COMMAND ----------

# MAGIC %md
# MAGIC Column -> duration

# COMMAND ----------

df = df.withColumn("duration", trim(col("duration")))

# COMMAND ----------

df = df.withColumn("duration_type", split(col("duration"), " ")[1]) \
      .withColumn("duration", split(col("duration"), " ")[0]) 

# COMMAND ----------

# MAGIC %md
# MAGIC Column -> duration_type

# COMMAND ----------

value_count(df,"duration_type")

# COMMAND ----------

# MAGIC %md
# MAGIC Column -> listed_in

# COMMAND ----------

df = df.withColumn("listed_in", trim(col("listed_in")))

# COMMAND ----------

df = df.withColumn("listed_in", split("listed_in", ","))
df = df.withColumn("listed_in", transform("listed_in", lambda x: trim(x)))
df = df.withColumn("listed_in", concat_ws(",", "listed_in"))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Column -> Description

# COMMAND ----------

df = df.withColumn("description", trim(col("description")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Type Conversion

# COMMAND ----------

df = df.withColumn("date_added", to_date(col("date_added"), "MMMM d, yyyy")) \
       .withColumn("release_year", col("release_year").cast(IntegerType())) \
       .withColumn("duration", col("duration").cast(IntegerType()))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/Volumes/netflix_catalog/netflix_schema/cleaned")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("netflix_catalog.netflix_schema.cleaned_data")