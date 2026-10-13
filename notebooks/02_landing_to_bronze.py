# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from datetime import datetime
from pyspark.sql import functions as F
PROCESS_DATE = "2026-10-09"
LANDING_PATH = "/Volumes/workspace/landing/wikimedia/pageviews"

# COMMAND ----------

dt = datetime.strptime(PROCESS_DATE, "%Y-%m-%d")
day_pattern = f"{LANDING_PATH}/{dt:%Y/%m/%d}/pageviews-*.gz"

df_bronze = ( spark.read.text(day_pattern)
                .withColumn("_source_file",F.col("_metadata.file_path"))
                .withColumn("_ingested_at",F.current_timestamp()))

# COMMAND ----------

condition = f"_source_file LIKE '%/{dt:%Y/%m/%d}/%'"
df_bronze.write.format("delta").mode("overwrite").option("replaceWhere",condition).saveAsTable("workspace.bronze.pageviews_raw")