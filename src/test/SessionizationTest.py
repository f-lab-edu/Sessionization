import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from main.Sessionization import assign_session_id
from main.SchemaDefinition import get_schema


# SparkSession fixture
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("SessionizationTest") \
        .getOrCreate()


# 샘플 데이터 생성
@pytest.fixture
def sample_data(spark):
    schema = get_schema()
    data = [
        # 같은 유저, 동일한 세션 (30분 이내)
        ("2019-10-10 04:00:00", "view", "product_111", "category_211", "brand1", "samsung", 257.15, "user_1"),
        ("2019-10-10 04:10:00", "view", "product_112", "category_212", None, "samsung", 41.19, "user_1"),

        # 같은 유저, 다른 세션 (30분 이상)
        ("2019-10-10 04:00:00", "view", "product_113", "category_213", "brand2", "apple", 694.74, "user_2"),
        ("2019-10-10 04:45:00", "view", "product_114", "category_214", "brand3", "apple", 100.36, "user_2"),

        # 다른 유저
        ("2019-10-10 04:00:00", "view", "product_115", "category_215", None, None, 15.96, "user_3")
    ]
    return spark.createDataFrame(data, schema=schema)


# assign_session_id 테스트
def test_assign_session_id(spark, sample_data):
    session_timeout = 1800  # 30분

    # 테스트 수행
    result_df = assign_session_id(sample_data, session_timeout)

    # 필요한 컬럼 선택
    result_df = result_df.select("user_id", "event_time", "prev_event_time", "time_diff", "session_id")

    # 결과 출력
    result_df.show(truncate=False)

    # 테스트 조건
    user1_sessions = result_df.filter(result_df.user_id == "user_1").select("session_id").distinct().count()
    user2_sessions = result_df.filter(result_df.user_id == "user_2").select("session_id").distinct().count()

    # 같은 세션 (user_1) 검증
    assert user1_sessions == 1, f"user_1 should have 1 session, but found {user1_sessions}"

    # 다른 세션 (user_2) 검증
    assert user2_sessions == 2, f"user_2 should have 2 sessions, but found {user2_sessions}"

