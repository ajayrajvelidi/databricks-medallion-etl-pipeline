# Databricks notebook source
# 1. Read Bronze
bronze_tx = spark.table("ing.ing_train_transaction")
bronze_id = spark.table("ing.ing_train_identity")

# 2. Transform & join (same as Silver logic)
silver_source_df = (
    bronze_tx.join(bronze_id, "TransactionID", "left")
    .dropDuplicates(["TransactionID"])
)

silver_source_df.createOrReplaceTempView("silver_source")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Incremental MERGE
# MAGIC MERGE INTO sot.train_transaction AS target
# MAGIC USING silver_source AS source
# MAGIC ON target.TransactionID = source.TransactionID
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC

# COMMAND ----------

