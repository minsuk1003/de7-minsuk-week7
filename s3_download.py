"""Q8 — boto3 로 S3 에서 읽어오기 (목록·크기 → 다운로드 → 행 수 출력).

사용법:
    python s3_download.py --bucket de-7-minsuk
    S3_BUCKET=de-7-minsuk python s3_download.py

자격증명은 코드에 두지 않고 boto3 기본 체인(~/.aws/credentials 등)에서 읽습니다.
버킷 이름은 실행 인자(--bucket) 또는 환경변수(S3_BUCKET)로 받습니다.
"""
import argparse
import csv
import os
import sys

import boto3

PREFIX = "bronze/"
FILE_NAME = "netflix_titles.csv"
LOCAL_DIR = "data"


def parse_args():
    parser = argparse.ArgumentParser(description="S3 bronze/ 목록 → 다운로드 → CSV 행 수")
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET"), help="S3 버킷 이름 (기본: 환경변수 S3_BUCKET)")
    parser.add_argument("--region", default=os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
    args = parser.parse_args()
    if not args.bucket:
        parser.error("--bucket 인자 또는 S3_BUCKET 환경변수가 필요합니다")
    return args


def list_objects(s3, bucket):
    """[1] bronze/ 아래 객체 목록과 각 객체의 크기 출력."""
    print(f"[1] list     s3://{bucket}/{PREFIX}")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            print(f"    {obj['Key']:<40} {obj['Size']:>12,} bytes")


def download(s3, bucket):
    """[2] netflix_titles.csv 를 project/data/ 아래로 다운로드."""
    key = PREFIX + FILE_NAME
    os.makedirs(LOCAL_DIR, exist_ok=True)
    local_path = os.path.join(LOCAL_DIR, FILE_NAME)
    print(f"[2] download s3://{bucket}/{key} -> {local_path}")
    s3.download_file(bucket, key, local_path)
    print(f"    downloaded ({os.path.getsize(local_path):,} bytes)")
    return local_path


def count_rows(path):
    """[3] CSV 레코드 수 (헤더 제외). 따옴표 안의 줄바꿈은 한 행으로 세지 않도록 csv 모듈 사용."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        n = sum(1 for _ in reader)
    print(f"[3] rows     {n:,} (header excluded, {len(header)} columns)")
    return n


def main():
    args = parse_args()
    s3 = boto3.client("s3", region_name=args.region)
    list_objects(s3, args.bucket)
    path = download(s3, args.bucket)
    count_rows(path)


if __name__ == "__main__":
    sys.exit(main())
