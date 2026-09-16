# Q5 — 공식 Airflow 이미지를 베이스로 JDK + Spark/AWS provider + pyspark 를 얹은 커스텀 이미지
# 빌드: docker compose build   (docker-compose.yaml 의 x-airflow-common 에서 build: . 로 참조)
FROM apache/airflow:3.3.1

# 1) 시스템 패키지 — apt 는 root 로
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jdk-headless \
        netcat-openbsd \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 2) 파이썬 패키지 — pip 는 airflow 유저로 (root 로 설치하면 이미지 규약 위반)
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
