"""Q9 — Spark 집계 잡: netflix_titles.csv → (type × 장르) 작품 수 → parquet(snappy).

처리 내용
  1. release_year 가 기준 연도(--year, 기본 2015) 이상인 행만 남김
  2. listed_in(쉼표로 구분된 장르 목록)을 장르 하나당 한 행이 되도록 펼치고 앞뒤 공백 제거
  3. type × 장르 별 작품 수 집계
  4. parquet(snappy 압축)으로 컨테이너 로컬 경로에 저장, 집계 행 수 출력

사용법
  spark-submit transform.py --input /opt/airflow/data/netflix_titles.csv \
                            --output /opt/airflow/output/silver --year 2015
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, explode, split, trim


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="입력 CSV 경로")
    p.add_argument("--output", required=True, help="출력 parquet 디렉터리")
    p.add_argument("--year", type=int, default=2015, help="기준 연도 (이 값 이상만 남김)")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("NetflixTransform_강민석").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 따옴표 안의 줄바꿈·쉼표가 있는 CSV 이므로 multiLine + escape 설정
    df = spark.read.csv(args.input, header=True, multiLine=True, escape='"')
    print(f"input rows = {df.count()}")

    filtered = df.filter(col("release_year").cast("int") >= args.year)
    print(f"rows with release_year >= {args.year} = {filtered.count()}")

    exploded = (
        filtered.withColumn("genre", explode(split(col("listed_in"), ",")))
        .withColumn("genre", trim(col("genre")))
        .filter(col("genre") != "")
    )

    result = (
        exploded.groupBy("type", "genre")
        .agg(count("*").alias("title_count"))
        .orderBy(col("type"), col("title_count").desc(), col("genre"))
    )

    result.write.mode("overwrite").option("compression", "snappy").parquet(args.output)

    n = result.count()
    result.show(n, truncate=False)
    print(f"aggregated rows = {n}")
    print(f"saved parquet(snappy) to {args.output}")

    spark.stop()


if __name__ == "__main__":
    main()
