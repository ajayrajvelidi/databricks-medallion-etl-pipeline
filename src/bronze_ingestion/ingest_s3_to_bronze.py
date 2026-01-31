# Databricks notebook source
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("S3-IEEE-Ingestion")
    .config("spark.hadoop.fs.s3a.access.key", "Access_key")
    .config("spark.hadoop.fs.s3a.secret.key", "Scret_key")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)
print(spark)


# COMMAND ----------

s3_path = ""
/Volumes/workspace/default/data_files/

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s3_path)
)

display(df)


# COMMAND ----------

s3_path = ""
/Volumes/workspace/default/data_files/

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s3_path)
)

display(df)


# COMMAND ----------

(
    df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("ing.ing_train_transaction")
)


# COMMAND ----------

(
    df2.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("ing.ing_train_identity")
)


# COMMAND ----------

