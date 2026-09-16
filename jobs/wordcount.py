"""Q4 — wordcount: 텍스트 읽기 → 공백 기준 단어 분리 → 단어별 개수 집계 → 빈도 상위 20개.

분리 규칙은 공백뿐입니다. 소문자 변환·구두점 제거는 하지 않습니다.
(Lorem 과 lorem, amet 과 amet, 는 서로 다른 단어)
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, explode, split

INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark/data/wordcount.txt"

spark = SparkSession.builder.appName("WordCount_강민석").getOrCreate()
spark.sparkContext.setLogLevel("WARN")   # 결과 표가 INFO 로그에 묻히지 않도록

lines = spark.read.text(INPUT_PATH)                          # 1. 텍스트 파일 읽기 (컬럼: value)
words = (
    lines.select(explode(split(col("value"), r"\s+")).alias("word"))  # 2. 공백 기준 단어 분리
    .filter(col("word") != "")                               #    빈 줄·연속 공백에서 생기는 빈 토큰 제거
)
counts = words.groupBy("word").count()                       # 3. 단어별 개수 집계

print(f"total_words = {words.count()}, distinct_words = {counts.count()}")
counts.orderBy(desc("count"), col("word")).show(20, truncate=False)  # 4. 빈도 내림차순 상위 20개

spark.stop()
