"""Q9 — S3 → Spark → S3 통합 파이프라인 (세 task 를 순서대로 연결).

  download_csv  : boto3 로 s3://{버킷}/bronze/netflix_titles.csv 를 컨테이너 안으로 다운로드
  transform     : spark-submit 으로 jobs/transform.py 실행 (기준 연도는 인자로 주입)
  upload_silver : boto3 로 결과 parquet 을 s3://{버킷}/silver/{오늘날짜}/ 에 올리고 목록·개수 출력

자격증명은 Airflow Connection(aws_default) 에서만 읽고, 버킷 이름은 환경변수 S3_BUCKET 으로 받습니다.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

BUCKET = os.environ["S3_BUCKET"]                      # 환경변수 (docker-compose .env → 컨테이너)
BRONZE_KEY = "bronze/netflix_titles.csv"
LOCAL_INPUT = "/opt/airflow/data/netflix_titles.csv"
LOCAL_OUTPUT = "/opt/airflow/output/silver"
TRANSFORM_JOB = "/opt/airflow/dags/jobs/transform.py"
BASE_YEAR = 2015


def download_csv():
    s3 = S3Hook(aws_conn_id="aws_default").get_conn()   # boto3 client
    os.makedirs(os.path.dirname(LOCAL_INPUT), exist_ok=True)
    s3.download_file(BUCKET, BRONZE_KEY, LOCAL_INPUT)
    print(f"downloaded s3://{BUCKET}/{BRONZE_KEY} -> {LOCAL_INPUT} ({os.path.getsize(LOCAL_INPUT):,} bytes)")


def upload_silver(ds, **context):
    s3 = S3Hook(aws_conn_id="aws_default").get_conn()
    today = datetime.now().strftime("%Y-%m-%d")          # 오늘 날짜 폴더
    prefix = f"silver/{today}/"
    uploaded = 0
    for name in sorted(os.listdir(LOCAL_OUTPUT)):
        local = os.path.join(LOCAL_OUTPUT, name)
        if os.path.isfile(local) and not name.startswith("."):   # .crc 파일 제외
            s3.upload_file(local, BUCKET, prefix + name)
            uploaded += 1
            print(f"uploaded {local} -> s3://{BUCKET}/{prefix}{name}")

    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    objects = resp.get("Contents", [])
    print(f"--- s3://{BUCKET}/{prefix} objects ({len(objects)}) ---")
    for obj in objects:
        print(f"  {obj['Key']}  {obj['Size']:,} bytes")
    print(f"uploaded {uploaded} files, listed {len(objects)} objects")


with DAG(
    dag_id="weekly_pipeline_강민석",
    start_date=datetime(2026, 9, 1),
    schedule="@weekly",
    catchup=False,
    tags=["q9"],
) as dag:
    t_download = PythonOperator(task_id="download_csv", python_callable=download_csv)

    t_transform = BashOperator(
        task_id="transform",
        bash_command=(
            "spark-submit --master 'local[*]' "
            f"{TRANSFORM_JOB} --input {LOCAL_INPUT} --output {LOCAL_OUTPUT} --year {BASE_YEAR}"
        ),
    )

    t_upload = PythonOperator(task_id="upload_silver", python_callable=upload_silver)

    t_download >> t_transform >> t_upload
