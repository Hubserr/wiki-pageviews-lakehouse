# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import datetime, timedelta
NON_ARTICLE_NAMESPACES = [
    "Specjalna", "Media", "Wikipedia", "Wikipedysta", "Wikipedystka",
    "Użytkownik", "Użytkowniczka", "Plik", "Grafika", "MediaWiki",
    "Szablon", "Pomoc", "Kategoria", "Portal", "Wikiprojekt", "Moduł",
    "Gadżet", "Definicja_gadżetu", "Temat",
    "Special", "Talk", "User", "User_talk", "Wikipedia_talk", "WP",
    "File", "File_talk", "Image", "Template", "Template_talk",
    "Help", "Help_talk", "Category", "Category_talk",
    "Portal_talk", "Module", "Module_talk", "MediaWiki_talk",
    "pl",
]

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

condition = f"view_date='{PROCESS_DATE}'"



# COMMAND ----------



hourly_rank_window = (
    Window
    .partitionBy("view_hour_utc")
    .orderBy(F.desc("total_views"),F.asc("page_title"))
)

df_top_articles = (spark.read.table("workspace.silver.pageviews")
                .filter(F.col("view_date") == PROCESS_DATE)
                .filter(F.col("domain_code").isin("pl","pl.m"))
                .withColumn("prefix",F.regexp_extract(F.col("page_title"),r"^([^:]+):",1))
                .filter(
                    (~F.col("prefix").isin(*NON_ARTICLE_NAMESPACES))
                    &(~F.col("prefix").startswith("Dyskusja"))
                    )
                .groupBy("view_date","view_hour_utc","page_title")
                .agg(F.sum("view_count").alias("total_views"))
                .withColumn("rank",F.row_number().over(hourly_rank_window))
                .filter(F.col("rank")<=100)
                .select("view_date","view_hour_utc","rank","page_title","total_views")
)




# COMMAND ----------

df_top_articles.write.format("delta").mode("overwrite").option("replaceWhere",condition).saveAsTable("workspace.gold.pl_top_articles_hourly")
