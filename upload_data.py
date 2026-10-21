import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import os

ENDPOINT_URL = 'http://localhost:9000'
ACCESS_KEY = 'minioadmin'
SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'hh-lake'
FILE_PATH = 'it_vacancies_python_segment.csv'
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

def clear_bucket(s3_client, bucket_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            print(f"Найдены объекты в бакете '{bucket_name}'. Удаляем...")
            for obj in response['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                print(f"  Удален: {obj['Key']}")
        else:
            print(f"Бакет '{bucket_name}' пуст.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"Бакет '{bucket_name}' не существует. Создадим его.")
        else:
            print(f"Ошибка при очистке: {e}")

def main():
    if not os.path.exists(FILE_PATH):
        print(f"ОШИБКА: Файл '{FILE_PATH}' не найден. Проверьте путь.")
        return

    s3_client = get_s3_client()

    try:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"Бакет '{BUCKET_NAME}' успешно создан.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['BucketAlreadyOwnedByYou', 'BucketAlreadyExists']:
            print(f"Бакет '{BUCKET_NAME}' уже существует. Очищаем его от старых данных...")
            clear_bucket(s3_client, BUCKET_NAME)
        else:
            print(f"Ошибка при создании бакета: {e}")
            return

    print(f"Загрузка файла '{FILE_PATH}' в бакет '{BUCKET_NAME}'...")
    try:
        s3_client.upload_file(FILE_PATH, BUCKET_NAME, OBJECT_NAME)
        print("Файл успешно загружен!")
    except ClientError as e:
        print(f"Ошибка при загрузке: {e}")
        return

    print("\nСодержимое бакета:")
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
        for obj in response.get('Contents', []):
            print(f" - {obj['Key']} (размер: {obj['Size']} байт)")
    except ClientError as e:
        print(f"Ошибка при проверке: {e}")

if __name__ == "__main__":
    main()