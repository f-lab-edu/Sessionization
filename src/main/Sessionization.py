from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, unix_timestamp, lag, when, sha2, concat_ws, sum as spark_sum, min as spark_min
from main.common_schemas import EVENT_SCHEMA# from main.schemas.common_schemas import EVENT_SCHEMA


def load_data(ss, path, schema) -> DataFrame:
    return ss.read.option("header", "true").schema(schema).csv(path)


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


def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Sessionization") \
        .getOrCreate()

    schema = EVENT_SCHEMA
    path = "/Users/doyeonpyun/Downloads/input_data/year=2019/month=10/day=10/hour=4/*.csv"
    prev_path = "/Users/doyeonpyun/Downloads/input_data/year=2019/month=10/day=10/hour=5/*.csv"
    SESSION_TIMEOUT = 1800

    result_df = pre_processing_data(spark, path, prev_path, schema, SESSION_TIMEOUT)
    final_df = assign_session_id(result_df, SESSION_TIMEOUT)
    final_df.select('event_time', 'user_id', 'prev_event_time', 'time_diff', 'session_id').show(truncate=False, n=1000)


if __name__ == "__main__":
    main()
