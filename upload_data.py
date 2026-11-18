"""Загрузка CSV в S3. Адрес берётся из S3_ENDPOINT (по умолчанию localhost:9000)."""
import os
from botocore.exceptions import ClientError
from s3_utils import get_s3_client, BUCKET_NAME, OBJECT_NAME, ENDPOINT_URL

FILE_PATH = os.environ.get("CSV_PATH", "it_vacancies_python_segment.csv")


def main():
    if not os.path.exists(FILE_PATH):
        print(f"ОШИБКА: файл '{FILE_PATH}' не найден")
        return
    s3 = get_s3_client()
    print(f"Endpoint: {ENDPOINT_URL}")
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Бакет '{BUCKET_NAME}' создан")
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"Ошибка создания бакета: {e}")
            return
        print(f"Бакет '{BUCKET_NAME}' уже есть")
    print("Загрузка...")
    s3.upload_file(FILE_PATH, BUCKET_NAME, OBJECT_NAME)
    for o in s3.list_objects_v2(Bucket=BUCKET_NAME).get("Contents", []):
        print(f" - {o['Key']} ({o['Size']} байт)")


if __name__ == "__main__":
    main()