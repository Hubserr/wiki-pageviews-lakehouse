# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F
from datetime import datetime

dbutils.widgets.text("process_date","2026-10-09","Process date (YYYY-MM-DD)")
PROCESS_DATE = dbutils.widgets.get("process_date")
dt = datetime.strptime(PROCESS_DATE, "%Y-%m-%d")
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