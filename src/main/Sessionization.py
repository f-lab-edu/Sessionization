from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, unix_timestamp, lag, when, sha2, concat_ws, sum as spark_sum
from main.schemas.common_schemas import EVENT_SCHEMA


def load_data(ss, path, schema) -> DataFrame:
    return ss.read.option("header", "true").schema(schema).csv(path)  # ss 사용


def pre_processing_data(ss, raw_data_path, prev_data_path, schema, session_timeout) -> DataFrame:
    raw_data = load_data(ss, raw_data_path, schema)
    prev_data = load_data(ss, prev_data_path, schema)
    # 데이터 병합 및 날짜 타입 변경
    df = raw_data.unionAll(prev_data)
    df = df.withColumn("event_time", col("event_time").cast("timestamp"))
    return df


def assign_session_id(df, session_timeout) -> DataFrame:
    window_spec = Window.partitionBy("user_id").orderBy("event_time")

    # 필요 컬럼 생성
    # new_session : 새로운 세션 발생 여부
    # session_number : window 함수를 통해 new_session 컬럼을 누적하며 session_id 구분 로직 구현
    # session_id : sha 함수를 통한 최종 session_id
    df = (df.withColumn("prev_event_time",lag("event_time").over(window_spec))
          .withColumn("time_diff",unix_timestamp("event_time") - unix_timestamp("prev_event_time"))
          .withColumn("new_session",when(col("time_diff") > session_timeout, 1).otherwise(0))
          .withColumn("session_number",spark_sum("new_session").over(window_spec.rowsBetween(Window.unboundedPreceding, 0)))
          .withColumn("session_id",sha2(concat_ws("_", col("user_id"), col("session_number")), 256)))
    return df


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Sessionization") \
        .getOrCreate()

    # 스키마 가져오기
    schema = EVENT_SCHEMA

    # 데이터 경로
    from_file = True
    path = "/Users/doyeonpyun/Downloads/input_data/year=2019/month=10/day=10/hour=4/*.csv" # 동적으로 변경 예정
    prev_path = "/Users/doyeonpyun/Downloads/input_data/year=2019/month=10/day=10/hour=5/*.csv" # 동적으로 변경 예정

    # 세션 초과 기준
    SESSION_TIMEOUT = 1800  # 30분

    # 데이터 처리
    result_df = pre_processing_data(spark, path, prev_path, schema, SESSION_TIMEOUT)
    final_df = assign_session_id(result_df, SESSION_TIMEOUT)

    # 결과 확인
    final_df.select('event_time','user_id','prev_event_time','time_diff','session_id').show(truncate=False,n = 1000)
