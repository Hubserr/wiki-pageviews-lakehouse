# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F
from datetime import datetime, timedelta

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

bronze_condition = f"_source_file LIKE '%/{dt:%Y/%m/%d}/%'"
silver_condition = f"view_date = '{PROCESS_DATE}'"  


# COMMAND ----------

df_silver_day = (spark.read.table("workspace.bronze.pageviews_raw")
            .filter(bronze_condition)
            .withColumn("parts",F.split(F.col("value"), " "))
            .withColumn("domain_code",F.col("parts").getItem(0))
            .withColumn("page_title",F.col("parts").getItem(1))
            .withColumn("view_count",F.col("parts").getItem(2).cast("bigint"))
            .withColumn("view_hour_utc",F.to_timestamp(F.regexp_extract("_source_file",r"pageviews-(\d{8}-\d{6})\.gz",1),"yyyyMMdd-HHmmss"))
            .withColumn("view_date",F.to_date(F.col("view_hour_utc")))
            # column with response size is always = 0 
            .select("domain_code","page_title","view_count","view_hour_utc","view_date","_source_file","_ingested_at")
)

# COMMAND ----------

df_silver_day.write.format("delta").mode("overwrite").option("replaceWhere",silver_condition).saveAsTable("workspace.silver.pageviews")