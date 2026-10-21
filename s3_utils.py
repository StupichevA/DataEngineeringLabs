import boto3
from botocore.client import Config

ENDPOINT_URL = 'http://localhost:9000'
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'hh-lake'
OBJECT_NAME = 'it_vacancies_python_segment.csv'


def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def get_file_size():
    s3 = get_s3_client()
    return s3.head_object(Bucket=BUCKET_NAME, Key=OBJECT_NAME)['ContentLength']


def read_s3_split(start, end):
    s3 = get_s3_client()
    range_header = f"bytes={start}-{end - 1}"
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=OBJECT_NAME, Range=range_header)
        data = response['Body'].read()
        if start > 0:
            nl = data.find(b'\n')
            data = data[nl + 1:] if nl != -1 else b""
        if end < get_file_size():
            next_response = s3.get_object(Bucket=BUCKET_NAME, Key=OBJECT_NAME, Range=f"bytes={end}-{end + 1024}")
            next_data = next_response['Body'].read()
            nl = next_data.find(b'\n')
            if nl != -1:
                data += next_data[:nl + 1]
        return data
    except Exception as e:
        print(f"Ошибка чтения S3: {e}")
        return b""