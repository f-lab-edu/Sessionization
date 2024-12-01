from pyspark.sql.types import StructType, StructField, StringType

# 데이터 스키마 정의
def get_schema():
    return StructType([
        StructField("event_time", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("user_id", StringType(), False)
    ])
