import argparse
import os
import sys
from datetime import datetime as dt, timedelta

import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv


BASE_URL = "https://dumps.wikimedia.org/other/pageviews"
USER_AGENT = "wiki-pageviews-lakehouse/0.1 (https://github.com/Hubserr/wiki-pageviews-lakehouse)"

def build_url_and_key(dt):
    file_name = f"pageviews-{dt:%Y%m%d}-{dt:%H}0000.gz"
    url = f"{BASE_URL}/{dt:%Y}/{dt:%Y}-{dt:%m}/{file_name}"
    key = f"pageviews/dt={dt:%Y}-{dt:%m}-{dt:%d}/{file_name}"
    return url, key


def object_exists(s3, bucket, key):

    try:
        s3.head_object(Bucket=bucket, Key = key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise    

def upload_from_url(s3, bucket, url, key):
    headers = {"User-Agent": USER_AGENT}
    with requests.get(url,headers=headers,stream=True,timeout=60) as response:
        response.raise_for_status()
        s3.upload_fileobj(response.raw,bucket,key)


def main():
    load_dotenv()
    bucket = os.environ["S3_BUCKET"]
    profile = os.environ["AWS_PROFILE"]

    parser = argparse.ArgumentParser(description="Download Wikimedia pageview dumps to S3")
    parser.add_argument("--date", required=True, help="Processing date, YYYY-MM-DD")
    parser.add_argument("--hours", type=int, default=24, help="How many hours to process (for testing)")
    args = parser.parse_args()

    start = dt.strptime(args.date, "%Y-%m-%d")
    s3 = boto3.Session(profile_name=profile).client("s3")

    uploaded, skipped, failed = [], [], []

    for hour in range(args.hours):
        date = start + timedelta(hours=hour)
        url, key = build_url_and_key(date)
        try:
            if object_exists(s3, bucket, key):
                print(f"Skipping {key} (already in S3)")
                skipped.append(key)
                continue
            upload_from_url(s3, bucket, url, key)
            print(f"Uploaded {key}")
            uploaded.append(key)
        except (requests.exceptions.RequestException, ClientError) as e:
            print(f"Failed {key}: {e}")
            failed.append(key)

    print(f"Uploaded: {len(uploaded)}, skipped: {len(skipped)}, failed: {len(failed)}")
    if failed:
        sys.exit(f"Failed to upload {len(failed)} files: {failed}")


if __name__ == "__main__":
    main()