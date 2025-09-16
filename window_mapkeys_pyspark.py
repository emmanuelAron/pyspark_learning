# Databricks notebook source

from pyspark.sql import Row
from pyspark.sql.functions import first, last
 
df = spark.createDataFrame([
Row(employee = "santosh", salary = 100000, department = "engineering"),
Row(employee = "amit", salary = 120000, department = "engineering"),
Row(employee = "rahul", salary = 150000, department = "engineering"),
Row(employee = "raj", salary = 80000, department = "sales"),
Row(employee = "vijay", salary = 180000, department = "sales"),
Row(employee = "ankit", salary = 250000, department = "hr"),
Row(employee = "rakesh", salary = 220000, department = "finance"),
Row(employee = "sachin", salary = 110000, department = "finance"),
Row(employee = "shubham", salary = 140000, department = "finance"),
Row(employee = "amit", salary = 120000, department = "finance")
]
)
df.show()
df.groupBy("department").agg(first("salary"), last("salary")).show()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_min = Window.partitionBy("department").orderBy(F.col("salary").asc())
w_max = Window.partitionBy("department").orderBy(F.col("salary").desc())

min_rows = (df
    .withColumn("rn", F.row_number().over(w_min))
    .where(F.col("rn")==1)
    .select("department", F.col("employee").alias("employee_min"), F.col("salary").alias("min_salary")))

max_rows = (df
    .withColumn("rn", F.row_number().over(w_max))
    .where(F.col("rn")==1)
    .select("department", F.col("employee").alias("employee_max"), F.col("salary").alias("max_salary")))

result = min_rows.join(max_rows, "department")
result.show(truncate=False)


# COMMAND ----------

df = spark.createDataFrame([(10,), (20,), (30,)], ["value"])
df.show()

df.selectExpr( "value AS originalValue","value * 2 AS doubleNewColumn ").show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType
from pyspark.sql.functions import col, map_keys
 
# Define MapType
 
schema = StructType([
StructField("id", IntegerType(), True),
StructField("attributes", MapType(StringType(), StringType()), True)
])
 
# Sample Data
 
data = [(1, {"Key1": "Value1", "Key2": "Value2"}), (2, {"Key3": "Value3", "Key4": "Value4"})]
 
df = spark.createDataFrame(data, schema)
df.show(truncate=False)

# COMMAND ----------

df.select(map_keys(col("attributes")).alias('keys')).show()

# COMMAND ----------

data = [
(1, {"city": "New York", "state": "NY"}),
(2, {"city": "San Francisco", "state": "CA"})
]
 
schema = ["id", "address"]
 
df = spark.createDataFrame(data, schema=schema)
 
print("approach 1")
df.select(col("address.city")).show() #correct syntax
 
print("approach 2")
df.select("address.city").show() #correct syntax
 
print("approach 3")
df.select(col("address")["city"]).show() #correct syntax
 
print("approach 4")
df.select("address").select(col("address.city")).show() #correct syntax