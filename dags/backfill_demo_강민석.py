"""Q7 — 백필(backfill) 데모.

- 스케줄 매일 1회, start_date 는 작업일(2026-09-17) 기준 7일 전 날짜 리터럴(2026-09-10)로 고정
- catchup=True : DAG 를 켜면 start_date 부터 지금까지의 과거 구간(scheduled 실행)을 자동으로 채움
  (Q3 의 sample_dag 는 catchup=False 라서 과거 구간을 건너뛰었음)
- 각 실행은 자신의 logical date(ds)에 해당하는 파일명으로 결과를 씀 → 날짜별로 분리된 산출물
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

OUTPUT_DIR = "/opt/airflow/output/backfill"     # 컨테이너 안 경로


def write_daily_file(ds, **context):
    """실행 날짜(ds = logical date, YYYY-MM-DD)별 파일 생성."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"daily_{ds}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"logical_date={ds}\nrun_id={context['run_id']}\n")
    print(f"wrote {path}")
    return path


def verify_file(ds, **context):
    path = os.path.join(OUTPUT_DIR, f"daily_{ds}.txt")
    assert os.path.exists(path), f"{path} not found"
    print(f"verified {path} ({os.path.getsize(path)} bytes)")
    print("files so far:", sorted(os.listdir(OUTPUT_DIR)))


with DAG(
    dag_id="backfill_demo_강민석",
    start_date=datetime(2026, 9, 10),   # 고정된 날짜 리터럴 (작업일 9/17 의 7일 전)
    schedule="@daily",                  # 매일 1회
    catchup=True,                       # 과거 구간 자동 채우기 (핵심 설정)
    max_active_runs=2,
    tags=["q7"],
) as dag:
    write = PythonOperator(task_id="write_daily_file", python_callable=write_daily_file)
    verify = PythonOperator(task_id="verify_file", python_callable=verify_file)

    write >> verify
