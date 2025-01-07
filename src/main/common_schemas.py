from pyspark.sql.types import StructType, StructField, StringType

# 단일 정적 스키마 정의
EVENT_SCHEMA = StructType([
        StructField("event_time", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), True)
])