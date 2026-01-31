# Databricks notebook source
from pyspark.sql.functions import col


# COMMAND ----------

tx_df = spark.table("ing.ing_train_transaction")
id_df = spark.table("ing.ing_train_identity")


# COMMAND ----------

silver_joined_df = (
    tx_df.alias("tx")
    .join(
        id_df.alias("id"),
        on="TransactionID",
        how="left"
    )
)


# COMMAND ----------

silver_df = (
    silver_joined_df
    .dropDuplicates(["TransactionID"])          # PK
    .filter(col("TransactionAmt").isNotNull())  # basic DQ
)


# COMMAND ----------

silver_df = silver_df.select(
    col("TransactionID").cast("long"),
    col("TransactionDT").cast("long"),
    col("TransactionAmt").cast("double"),
    col("ProductCD"),
    col("card1"),
    col("card2"),
    col("card3"),
    col("card4"),
    col("card5"),
    col("card6"),
    col("addr1"),
    col("addr2"),
    col("DeviceType"),
    col("DeviceInfo")
)


# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("sot.train_transaction")


# COMMAND ----------

