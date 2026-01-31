# Databricks notebook source
from pyspark.sql.functions import sum, count


# COMMAND ----------

silver_df = spark.table("sot.train_transaction")


# COMMAND ----------

gold_product_metrics = (
    silver_df
    .groupBy("ProductCD", "card4")
    .agg(
        count("*").alias("total_transactions"),
        sum("TransactionAmt").alias("total_spend")
    )
)


# COMMAND ----------

gold_device_metrics = (
    silver_df
    .groupBy("DeviceType")
    .agg(
        count("*").alias("transaction_count"),
        sum("TransactionAmt").alias("total_amount")
    )
)


# COMMAND ----------

gold_product_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("view.product_spend_metrics")


# COMMAND ----------

gold_device_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("view.device_transaction_metrics")
