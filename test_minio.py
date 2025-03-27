import boto3
from botocore.client import Config

# Configurações do MinIO
minio_endpoint = "http://localhost:9000"
access_key = "h8VchqtGQvaO9HUxC6ko"
secret_key = "pvzDRFNqAOqqgu73gFAuHuklQV8UV0dEGA5dqKQE"
bucket_name = "test-bucket"
object_name = "hello.txt"
file_content = "Hello from Python to MinIO!"

# Conecta ao MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=minio_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# Cria bucket (ignora se já existe)
try:
    s3.create_bucket(Bucket=bucket_name)
except s3.exceptions.BucketAlreadyOwnedByYou:
    pass

# Upload do arquivo (string -> MinIO)
s3.put_object(Bucket=bucket_name, Key=object_name, Body=file_content.encode())

# Download do arquivo
response = s3.get_object(Bucket=bucket_name, Key=object_name)
downloaded = response['Body'].read().decode()

print("📝 Conteúdo baixado:", downloaded)
