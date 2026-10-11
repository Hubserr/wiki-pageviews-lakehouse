# Databricks notebook source
from pyspark.sql import functions as F

df_silver = (spark.read.table("workspace.bronze.pageviews_raw")
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

df_silver.write.format("delta").mode("overwrite").saveAsTable("workspace.silver.pageviews")

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.silver.pageviews IS 'Hourly Wikimedia pageviews: one row per project (domain_code), page title and hour. All timestamps are in UTC. Source: Wikimedia pageviews dumps via bronze.pageviews_raw.'