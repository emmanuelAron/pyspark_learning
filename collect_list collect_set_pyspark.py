# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, collect_set

# Créer la SparkSession
spark = SparkSession.builder \
    .appName("CollectList_vs_CollectSet") \
    .getOrCreate()


# COMMAND ----------

data = [
    ("Alice", "Math"),
    ("Alice", "Physics"),
    ("Alice", "Math"),    # doublon volontaire
    ("Bob", "Math"),
    ("Bob", "Chemistry"),
    ("Bob", "Chemistry"), # doublon volontaire
]

df = spark.createDataFrame(data, ["name", "subject"])
df.show()


# COMMAND ----------

df.groupBy('name').agg(collect_list('subject')).show(truncate=False)

# COMMAND ----------

df.groupBy('name').agg(collect_set('subject')).show(truncate=False)

# COMMAND ----------

