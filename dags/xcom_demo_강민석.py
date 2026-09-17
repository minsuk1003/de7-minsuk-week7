"""Q6 — XCom 전달 + 재시도.

- count_lines : 파일의 줄 수를 계산해 return (→ XCom 'return_value' 로 저장)
                retries=2, retry_delay=10s. 첫 시도(try_number == 1)에서만 일부러 실패.
- use_value   : xcom_pull 로 앞 작업의 반환값을 받아 로그에 출력.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

TARGET_FILE = "/opt/airflow/dags/sample_dag.py"


def count_lines(**context):
    try_number = context["ti"].try_number
    print(f"try_number = {try_number}")
    if try_number == 1:
        # 첫 시도에서만 실패 → 재시도(2회차)에서 성공
        raise RuntimeError("첫 시도는 일부러 실패시킵니다 (재시도 확인용)")

    with open(TARGET_FILE, encoding="utf-8") as f:
        n = sum(1 for _ in f)
    print(f"{TARGET_FILE} line count = {n}")
    return n                       # XCom(return_value) 에 저장됨


def use_value(**context):
    ti = context["ti"]
    received = ti.xcom_pull(task_ids="count_lines")   # 앞 작업의 return 값
    print(f"received from XCom: {received}")
    print(f"doubled = {received * 2}")


with DAG(
    dag_id="xcom_demo_강민석",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["q6"],
) as dag:
    t1 = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines,
        retries=2,                          # 2회 이상
        retry_delay=timedelta(seconds=10),
    )
    t2 = PythonOperator(
        task_id="use_value",
        python_callable=use_value,
    )

    t1 >> t2
