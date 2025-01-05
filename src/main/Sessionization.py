from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, unix_timestamp, lag, when, sha2, concat_ws, sum as spark_sum, min as spark_min
from datetime import datetime, timedelta
import shutil
from main.common_schemas import EVENT_SCHEMA


def load_data(ss, path, schema) -> DataFrame:
    return ss.read.option("header", "false").schema(schema).csv(path)


def pre_processing_data(ss, raw_data_path, prev_data_path, schema, session_timeout) -> DataFrame:
    raw_data = load_data(ss, raw_data_path, schema)
    prev_data = load_data(ss, prev_data_path, schema)
    df = raw_data.unionAll(prev_data)
    df = df.withColumn("event_time", col("event_time").cast("timestamp"))
    return df


def assign_session_id(df, session_timeout) -> DataFrame:
    window_spec = Window.partitionBy("user_id").orderBy("event_time")
    session_window_spec = Window.partitionBy("user_id", "session_number")
    df = (df.withColumn("prev_event_time", lag("event_time").over(window_spec))
          .withColumn("time_diff", unix_timestamp("event_time") - unix_timestamp("prev_event_time"))
          .withColumn("new_session", when(col("time_diff") > session_timeout, 1).otherwise(0))
          .withColumn("session_number", spark_sum("new_session").over(window_spec.rowsBetween(Window.unboundedPreceding, 0)))
          .withColumn("session_start_time", spark_min("event_time").over(session_window_spec))
          .withColumn("session_id", sha2(concat_ws("_",col("session_start_time"), col("user_id"), col("session_number")), 256)))
    return df


def save_partitioned_data(df: DataFrame, output_path: str):
    year = df.select("year").first()["year"]
    month = df.select("month").first()["month"]
    day = df.select("day").first()["day"]
    hour = df.select("hour").first()["hour"]

    partition_path = f"{output_path}/year={year}/month={month}/day={day}/hour={hour}"

    # 기존 파티션 삭제
    shutil.rmtree(partition_path, ignore_errors=True)

    # 데이터 저장
    df.write.partitionBy("year", "month", "day", "hour") \
        .mode("append") \
        .format("csv") \
        .save(output_path)


def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Sessionization") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Base file path
    base_input_path = "/Users/doyeonpyun/Desktop/commercial_log/bronze"
    base_output_path = "/Users/doyeonpyun/Desktop/commercial_log/silver"

    # Trigger time
    trigger_time = datetime(2019, 10, 10, 4, 0, 0)
    prev_trigger_time = trigger_time - timedelta(hours=1)

    # Input paths
    input_path = f"{base_input_path}/year={trigger_time.year}/month={trigger_time.month}/day={trigger_time.day}/hour={trigger_time.hour}/*.csv"
    prev_input_path = f"{base_input_path}/year={prev_trigger_time.year}/month={prev_trigger_time.month}/day={prev_trigger_time.day}/hour={prev_trigger_time.hour}/*.csv"

    # Schema and session timeout
    schema = EVENT_SCHEMA
    SESSION_TIMEOUT = 1800

    # Data processing
    result_df = pre_processing_data(spark, input_path, prev_input_path, schema, SESSION_TIMEOUT)
    result_df = (result_df
                 .withColumn("year", col("event_time").cast("timestamp").substr(0, 4))
                 .withColumn("month", col("event_time").cast("timestamp").substr(6, 2))
                 .withColumn("day", col("event_time").cast("timestamp").substr(9, 2))
                 .withColumn("hour", col("event_time").cast("timestamp").substr(12, 2)))
    final_df = assign_session_id(result_df, SESSION_TIMEOUT)

    save_partitioned_data(final_df, base_output_path)

    # 최종데이터 확인
    # final_df.select('event_time', 'user_id', 'prev_event_time', 'time_diff', 'session_id').show(truncate=False, n=1000)
    # final_df.filter(col("session_id").isNull()).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
