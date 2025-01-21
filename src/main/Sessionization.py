import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, unix_timestamp, lag, when, last, sha2, concat_ws, \
    sum as spark_sum, min as spark_min, year, month, dayofmonth, hour
from common_schemas import EVENT_SCHEMA


def load_data(ss, path, schema) -> DataFrame:
    print(f"Loading data from path: {path}")  # 데이터 경로 출력
    return ss.read.option("header", "true").schema(schema).csv(path)


def pre_processing_data(ss, raw_data_path, prev_data_path, schema, session_timeout) -> DataFrame:
    print(f"Processing raw data path: {raw_data_path}")  # raw 데이터 경로 출력
    print(f"Processing previous data path: {prev_data_path}")  # 이전 데이터 경로 출력

    raw_data = load_data(ss, raw_data_path, schema)
    prev_data = load_data(ss, prev_data_path, schema)

    print(f"Raw data count: {raw_data.count()}")  # raw 데이터 개수 출력
    print(f"Previous data count: {prev_data.count()}")  # 이전 데이터 개수 출력

    df = raw_data.unionAll(prev_data)
    df = df.withColumn("event_time", col("event_time").cast("timestamp"))
    print(f"Combined data count: {df.count()}")  # 병합된 데이터 개수 출력
    return df


def assign_session_id(df, SESSION_TIMEOUT) -> DataFrame:
    window_spec = Window.partitionBy("user_id").orderBy("event_time")

    df = (
        df.withColumn("prev_event_time", lag("event_time").over(window_spec))
        .withColumn("time_diff", unix_timestamp("event_time") - unix_timestamp("prev_event_time"))
        .withColumn("new_session", when(col("time_diff") > SESSION_TIMEOUT, 1).otherwise(0))
        .withColumn("session_number",spark_sum("new_session").over(window_spec.rowsBetween(Window.unboundedPreceding, 0)),)
    )

    session_window_spec = Window.partitionBy("user_id", "session_number")
    df = df.withColumn("session_start_time", spark_min("event_time").over(session_window_spec))

    session_id_expr = (
        when(
            col("session_id").isNotNull(),
            col("session_id")
        )
        .when(
            (col("time_diff").isNull()) | (col("time_diff") > SESSION_TIMEOUT),
            sha2(concat_ws("_", col("session_start_time"), col("user_id")), 256)
        )
        .otherwise(None)
    )

    df = df.withColumn(
        "session_id",
        last(session_id_expr, True).over(window_spec.rowsBetween(Window.unboundedPreceding, 0))
    )
    return df


def main():
    # 명령줄 인자로 날짜와 시간 받기
    if len(sys.argv) < 3:
        print("Usage: spark-submit <script> <event_date> <event_hour>")
        sys.exit(1)

    event_date = sys.argv[1]  # "{{ ds }}"
    event_hour = sys.argv[2]  # "{{ logical_date.strftime('%H') }}"

    print(f"Event Date: {event_date}, Event Hour: {event_hour}")  # 이벤트 날짜와 시간 출력

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Sessionization") \
        .getOrCreate()

    schema = EVENT_SCHEMA
    session_timeout = 1800

    # 동적 경로 생성
    path = f"/Users/doyeonpyun/Downloads/input_data/year={event_date[:4]}/month={event_date[5:7]}/day={event_date[8:10]}/hour={event_hour}/*.csv"
    prev_path = f"/Users/doyeonpyun/Downloads/input_data/year={event_date[:4]}/month={event_date[5:7]}/day={event_date[8:10]}/hour={int(event_hour) - 1:02d}/*.csv"
    output_path = "/Users/doyeonpyun/Desktop/commercial_log"

    print(f"Generated path: {path}")
    print(f"Generated prev_path: {prev_path}")
    print(f"Output path: {output_path}")

    result_df = pre_processing_data(spark, path, prev_path, schema, session_timeout)

    # 데이터 스키마 및 내용 출력
    print("Result DataFrame Schema:")
    result_df.printSchema()
    result_df.show(5)  # 상위 5개 행 출력

    # 데이터 세션 ID 생성 및 결과 저장
    final_df = assign_session_id(result_df, session_timeout)

    final_df = (
        final_df
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("day", dayofmonth("event_time"))
        .withColumn("hour", hour("event_time"))
    )

    final_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day", "hour") \
        .option("header", "true") \
        .csv(output_path)

    print(f"Data processed and saved to: {output_path}")  # 데이터 저장 완료 메시지 출력


if __name__ == "__main__":
    main()
