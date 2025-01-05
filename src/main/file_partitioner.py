from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, date_format, hour, to_timestamp
from datetime import datetime
import shutil
from main.common_schemas import EVENT_SCHEMA


def load_data(ss: SparkSession, path: str, schema) -> DataFrame:
    return ss.read.option("header", "true").schema(schema).csv(path)


def preprocess_hourly_data(df: DataFrame, trigger_time: datetime) -> DataFrame:
    trigger_date = trigger_time.strftime("%Y-%m-%d")
    trigger_hour = trigger_time.hour
    return (df.withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("event_date", date_format(col("event_time"), "yyyy-MM-dd"))
            .withColumn("event_hour", hour(col("event_time")))
            .filter((col("event_date") == trigger_date) &
                    (col("event_hour") == trigger_hour))
            .withColumn("year", date_format(col("event_time"), "yyyy"))
            .withColumn("month", date_format(col("event_time"), "MM"))
            .withColumn("day", date_format(col("event_time"), "dd"))
            .withColumn("hour", hour(col("event_time"))))


def save_partitioned_data(df: DataFrame, output_path: str):
    year = df.select("year").first()["year"]
    month = df.select("month").first()["month"]
    day = df.select("day").first()["day"]
    hour = df.select("hour").first()["hour"]

    partition_path = f"{output_path}/year={year}/month={month}/day={day}/hour={hour}"

    # 기존 파티션 삭제 (필요시)
    shutil.rmtree(partition_path, ignore_errors=True)

    df.write.partitionBy("year", "month", "day", "hour") \
        .mode("append") \
        .format("csv") \
        .save(output_path)


def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("file_partitioner") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    input_file = "/Users/doyeonpyun/Desktop/commercial_log/raw/input_data.csv"
    trigger_time = datetime(2019, 10, 10, 5, 2, 1)

    output_path = "/Users/doyeonpyun/Desktop/commercial_log/bronze"

    schema = EVENT_SCHEMA
    raw_df = load_data(spark, input_file, schema)
    hourly_df = preprocess_hourly_data(raw_df, trigger_time)
    save_partitioned_data(hourly_df, output_path)

    spark.stop()


if __name__ == "__main__":
    main()
