# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# COMMAND ----------

print("Hiiiiiiiiiiiii")

# COMMAND ----------

data = [
    (1, "Alice", 23, "Engineering"),
    (2, "Bob", 29, "Marketing"),
    (3, "Charlie", 35, "Engineering"),
    (4, "David", 40, "HR"),
    (5, "Eve", 30, "Marketing")
]

columns = ["id", "name", "age", "department"]

df = spark.createDataFrame(data, columns)

display(df)

# COMMAND ----------

df_filtered = df.filter(col("age") > 30)
display(df_filtered)

# COMMAND ----------

df_grouped = df.groupBy("department").agg(avg("age").alias("avg_age"))
display(df_grouped)

# COMMAND ----------

display(df_grouped)

# COMMAND ----------

display(df.count())

# COMMAND ----------


