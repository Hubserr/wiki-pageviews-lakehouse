# Wikipedia Pageviews Lakehouse

A data pipeline that processes hourly Wikipedia pageview statistics
(Wikimedia pageviews) using a lakehouse architecture.

## Scope

- Ingestion of hourly pageview dumps and Wikimedia REST API data (Python)
- Processing in Databricks (PySpark) using the medallion architecture: bronze → silver → gold
- Delta Lake tables with incremental loading (MERGE)
- Dimensional model in the gold layer built with dbt
- Orchestration: Databricks Jobs and Apache Airflow
- Cloud data lake for raw files

## Tech stack

Python · PySpark · Databricks · Delta Lake · dbt · Apache Airflow · AWS / Azure

## Status

Work in progress.