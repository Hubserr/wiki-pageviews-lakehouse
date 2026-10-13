# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from datetime import datetime
from pyspark.sql import functions as F
PROCESS_DATE = "2026-10-08"
LANDING_PATH = "/Volumes/workspace/landing/wikimedia/pageviews"

# COMMAND ----------

dt = datetime.strptime(PROCESS_DATE, "%Y-%m-%d")
day_pattern = f"{LANDING_PATH}/{dt:%Y/%m/%d}/pageviews-*.gz"

df_bronze = ( spark.read.text(day_pattern)
                .withColumn("_source_file",F.col("_metadata.file_path"))
                .withColumn("_ingested_at",F.current_timestamp()))

# COMMAND ----------

df_bronze.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.pageviews_raw")