# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from datetime import datetime, timedelta
from pyspark.sql import functions as F

dbutils.widgets.text("run_date","","Process date (YYYY-MM-DD)")
dbutils.widgets.text("process_date","2026-10-09","Process date (YYYY-MM-DD)")

process_date = dbutils.widgets.get("process_date")
run_date = dbutils.widgets.get("run_date")

if process_date != "":
    PROCESS_DATE = process_date
elif run_date != "":
    run_dt = datetime.strptime(run_date, "%Y-%m-%d")
    process_dt = run_dt - timedelta(days=1) 
    PROCESS_DATE = process_dt.strftime("%Y-%m-%d")
else:
    raise ValueError("No process date or run date provided")

dt = datetime.strptime(PROCESS_DATE, "%Y-%m-%d")
print(f"Processing date: {PROCESS_DATE}")

LANDING_PATH = "/Volumes/workspace/landing/wikimedia/pageviews"

# COMMAND ----------

day_pattern = f"{LANDING_PATH}/{dt:%Y/%m/%d}/pageviews-*.gz"

df_bronze = ( spark.read.text(day_pattern)
                .withColumn("_source_file",F.col("_metadata.file_path"))
                .withColumn("_ingested_at",F.current_timestamp()))

# COMMAND ----------

condition = f"_source_file LIKE '%/{dt:%Y/%m/%d}/%'"
df_bronze.write.format("delta").mode("overwrite").option("replaceWhere",condition).saveAsTable("workspace.bronze.pageviews_raw")