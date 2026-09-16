"""Q3 — 시작 → 파이썬 작업 2개 → 종료 순차 DAG.

시작·종료는 아무 동작도 하지 않는 EmptyOperator,
파이썬 작업 2개는 문자열을 return(XCom 에 저장됨)합니다.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def say_hello():
    return "Hello Airflow"


def say_goodbye():
    return "Goodbye Airflow"


with DAG(
    dag_id="sample_dag",
    start_date=datetime(2026, 9, 1),
    schedule="@daily",   # 매일 1회
    catchup=False,       # 과거 구간은 자동 실행하지 않음
    tags=["q3"],
) as dag:
    start = EmptyOperator(task_id="start")
    hello_task = PythonOperator(task_id="hello_task", python_callable=say_hello)
    goodbye_task = PythonOperator(task_id="goodbye_task", python_callable=say_goodbye)
    end = EmptyOperator(task_id="end")

    start >> hello_task >> goodbye_task >> end
