# de7-minsuk-week7

메타코드 데이터 엔지니어링 부트캠프 **7기 강민석** — 7주차 과제 (Docker · Airflow · Spark · AWS S3 · Git)

## 구성
- `docker-compose.yaml` — Airflow 실습 환경 (Q3, Q5에서 커스텀 이미지로 전환)
- `docker-compose.spark.yaml` — Spark Standalone 클러스터 (Q4)
- `dags/` — Airflow DAG (Q3 sample_dag, Q6 xcom_demo, Q7 backfill_demo, Q9 weekly_pipeline)
- `jobs/` — Spark 잡 (Q4 wordcount)
- `s3_download.py` — boto3 S3 다운로드 스크립트 (Q8)

## 실습 환경
- 로컬 PC(Windows 11, RAM 16GB) 의 Docker Desktop 에서 Airflow 3.3.1 + Spark 3.5.6 실습, AWS 는 S3(ap-northeast-2) 만 사용

## 회고
- 문항마다 커밋을 남기고 캡처에 실행 지문을 붙이는 습관이 재현 가능한 작업 기록의 기본임을 체감했다
