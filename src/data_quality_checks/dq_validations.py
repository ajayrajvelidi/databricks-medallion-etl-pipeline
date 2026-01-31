# Databricks notebook source
from pyspark.sql.functions import col, sum, when


# COMMAND ----------

silver_df = spark.table("sot.train_transaction")
bronze_df = spark.table("ing.ing_train_transaction")


# COMMAND ----------

bronze_count = bronze_df.count()
silver_count = silver_df.count()

print(f"Bronze count: {bronze_count}")
print(f"Silver count: {silver_count}")


# COMMAND ----------

if silver_count < bronze_count * 0.95:
    raise Exception("❌ DQ FAILED: Silver lost more than 5% rows compared to Bronze")


# COMMAND ----------

null_check_df = silver_df.select(
    sum(when(col("TransactionID").isNull(), 1).otherwise(0)).alias("null_txn_id"),
    sum(when(col("TransactionAmt").isNull(), 1).otherwise(0)).alias("null_txn_amt")
)

null_check_df.show()


# COMMAND ----------

nulls = null_check_df.collect()[0]

if nulls["null_txn_id"] > 0 or nulls["null_txn_amt"] > 0:
    raise Exception("❌ DQ FAILED: Nulls found in critical columns")


# COMMAND ----------

duplicate_count = (
    silver_df.count()
    - silver_df.select("TransactionID").distinct().count()
)

print(f"Duplicate records: {duplicate_count}")


# COMMAND ----------

if duplicate_count > 0:
    raise Exception("❌ DQ FAILED: Duplicate TransactionID found")


# COMMAND ----------

expected_columns = {
    "TransactionID",
    "TransactionDT",
    "TransactionAmt",
    "ProductCD",
    "card4",
    "DeviceType"
}

actual_columns = set(silver_df.columns)

missing_cols = expected_columns - actual_columns

if missing_cols:
    raise Exception(f"❌ DQ FAILED: Missing columns {missing_cols}")


# COMMAND ----------

print("✅ ALL DATA QUALITY CHECKS PASSED")


# COMMAND ----------

