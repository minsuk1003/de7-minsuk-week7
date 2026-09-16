# de7-minsuk-week7

메타코드 데이터 엔지니어링 부트캠프 **7기 강민석** — 7주차 과제 (Docker · Airflow · Spark · AWS S3 · Git)

## 구성
- `docker-compose.yaml` — Airflow 실습 환경 (Q3, Q5에서 커스텀 이미지로 전환)
- `docker-compose.spark.yaml` — Spark Standalone 클러스터 (Q4)
- `dags/` — Airflow DAG (Q3 sample_dag, Q6 xcom_demo, Q7 backfill_demo, Q9 weekly_pipeline)
- `jobs/` — Spark 잡 (Q4 wordcount)
- `s3_download.py` — boto3 S3 다운로드 스크립트 (Q8)
