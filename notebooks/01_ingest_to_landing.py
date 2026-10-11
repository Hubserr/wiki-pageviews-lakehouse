# Databricks notebook source
import os 
import requests

url ="https://dumps.wikimedia.org/other/pageviews/2026/2026-10/pageviews-20261008-150000.gz"
target_dir ="/Volumes/workspace/landing/wikimedia/pageviews/2026/10/08"
file_name = url.split("/")[-1]
target_path = os.path.join(target_dir,file_name)

print(target_path)

# COMMAND ----------

os.makedirs(target_dir,exist_ok=True)

# COMMAND ----------

headers = {"User-Agent": "wiki-pageviews-lakehouse, https://github.com/Hubserr/wiki-pageviews-lakehouse"}

with requests.get(url,headers=headers,stream=True, timeout=60) as response:
    response.raise_for_status()
    with open(target_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=1024*1024):
            file.write(chunk)
