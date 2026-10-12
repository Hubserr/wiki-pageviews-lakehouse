# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F
from pyspark.sql import Window
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



# COMMAND ----------



hourly_rank_window = (
    Window
    .partitionBy("view_hour_utc")
    .orderBy(F.desc("total_views"),F.asc("page_title"))
)

df_top_articles = (spark.read.table("workspace.silver.pageviews")
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

df_top_articles.write.format("delta").mode("overwrite").saveAsTable("workspace.gold.pl_top_articles_hourly")


    

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.gold.pl_top_articles_hourly IS 'Top 100 most viewed articles of Polish Wikipedia (pl + pl.m, desktop and mobile summed) per hour. One row per hour and rank. Non-article namespaces excluded. Timestamps in UTC.'