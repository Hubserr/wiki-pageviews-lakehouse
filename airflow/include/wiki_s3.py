from botocore.exceptions import ClientError
import requests 

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

