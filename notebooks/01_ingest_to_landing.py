# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import os 
import requests
from datetime import datetime, timedelta
PROCESS_DATE = "2026-10-08"
BASE_URL = "https://dumps.wikimedia.org/other/pageviews"
LANDING_PATH = "/Volumes/workspace/landing/wikimedia/pageviews"
USER_AGENT = "wiki-pageviews-lakehouse/0.1 (https://github.com/Hubserr/wiki-pageviews-lakehouse)"




# COMMAND ----------

def build_paths(dt):
    url = f"{BASE_URL}/{dt:%Y}/{dt:%Y}-{dt:%m}/pageviews-{dt:%Y%m%d}-{dt:%H}0000.gz"
    target_dir = f"{LANDING_PATH}/{dt:%Y}/{dt:%m}/{dt:%d}"
    file_name = url.split("/")[-1]
    target_path = os.path.join(target_dir,file_name)
    return url, target_path

# COMMAND ----------

def download_file(url,target_path):
    if os.path.exists(target_path):
        print(f"File {target_path} already exists. Skipping download.")
        return "skipped"
    
    os.makedirs(os.path.dirname(target_path),exist_ok=True)

    headers = {"User-Agent": USER_AGENT}

    with requests.get(url,headers=headers,stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(target_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024*1024):
                file.write(chunk)
    return "downloaded"

# COMMAND ----------

start = datetime.strptime(PROCESS_DATE, "%Y-%m-%d")
downloaded =[]
skipped = []
failed = []


for hour in range(24):
    dt = start + timedelta(hours=hour)
    url, target_path = build_paths(dt)
    try: 
        status = download_file(url,target_path)
        if status == "downloaded":
            downloaded.append(target_path)
        if status == "skipped":
            skipped.append(target_path)
            
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file {target_path}: {e}")
        failed.append(target_path)

print(f"Downloaded: {len(downloaded)}")
print(f"Skipped: {len(skipped)}")
print(f"Failed: {len(failed)}")

if len(failed) > 0:
    raise Exception(f"Failed to download {len(failed)} files")