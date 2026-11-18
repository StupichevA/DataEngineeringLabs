import os
import boto3
from botocore.client import Config

ENDPOINT_URL = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
BUCKET_NAME = os.environ.get("S3_BUCKET", "hh-lake")
OBJECT_NAME = os.environ.get("S3_OBJECT", "it_vacancies_python_segment.csv")

_client = None
_size = None


def get_s3_client():
    global _client
    if _client is None:
        _client = boto3.client(
            "s3",
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )
    return _client


def get_file_size():
    global _size
    if _size is None:
        _size = get_s3_client().head_object(Bucket=BUCKET_NAME, Key=OBJECT_NAME)["ContentLength"]
    return _size


def read_s3_split(start, end):
    s3 = get_s3_client()
    try:
        resp = s3.get_object(Bucket=BUCKET_NAME, Key=OBJECT_NAME, Range=f"bytes={start}-{end - 1}")
        data = resp["Body"].read()
        if start > 0:
            nl = data.find(b"\n")
            data = data[nl + 1:] if nl != -1 else b""
        if end < get_file_size():
            nxt = s3.get_object(Bucket=BUCKET_NAME, Key=OBJECT_NAME, Range=f"bytes={end}-{end + 1024}")["Body"].read()
            nl = nxt.find(b"\n")
            if nl != -1:
                data += nxt[:nl + 1]
        return data
    except Exception as e:
        print(f"Ошибка чтения S3: {e}", flush=True)
        raise


def put_text(key, text):
    get_s3_client().put_object(Bucket=BUCKET_NAME, Key=key, Body=text.encode("utf-8"))


def get_text(key):
    return get_s3_client().get_object(Bucket=BUCKET_NAME, Key=key)["Body"].read().decode("utf-8")


def list_keys(prefix):
    keys = []
    pag = get_s3_client().get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return sorted(keys)


def delete_prefix(prefix):
    for k in list_keys(prefix):
        get_s3_client().delete_object(Bucket=BUCKET_NAME, Key=k)