# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Création d'un DataFrame exemple
df = spark.createDataFrame([
    (1, "Alice", 3000),
    (2, "Bob", 4500),
    (3, "Charlie", 5000)
], ["id", "name", "salary"])

df.show()


# COMMAND ----------

out_path = "/FileStore/tables/employees"
df.write.format('delta').save(out_path)

# COMMAND ----------

(df.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")   # <-- important
   .saveAsTable("default.employees"))



# COMMAND ----------



df2 = spark.createDataFrame([
    (1, ["Python", "Java", "C++"]),
    (2, ["Scala", "SQL"]),
    (3, [])
], ["id", "skills"])
df2.show()

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df2.select('id' , F.explode("skills").alias("skills")).show()

# COMMAND ----------

df2.select('id' , F.explode(F.col("skills")).alias("skills")).show()

# COMMAND ----------

df2.select('id' , F.explode(df2["skills"]).alias("skills")).show()

# COMMAND ----------

