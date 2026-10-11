# Databricks notebook source
df = spark.read.text("/Volumes/workspace/landing/wikimedia/pageviews/2026/10/08/pageviews-20261008-150000.gz")


# COMMAND ----------

from pyspark.sql import functions as F
df_bronze = ( df.withColumn("_source_file",F.col("_metadata.file_path"))
                .withColumn("_ingested_at",F.current_timestamp()))
df_bronze.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.pageviews_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT (*) FROM workspace.bronze.pageviews_raw